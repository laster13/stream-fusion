"""TMDB Orphan Matcher — background job that assigns tmdb_id to unmatched torrent_items.

Matching strategy (conservative — prefer no match over a wrong match):
  - Title: exact_normalized OR ordered_subset (no fuzzy)
  - Year (movies):
      • Year found in title → must match TMDB year ±1, even for exact_normalized
      • No year in title → only accepted if exact_normalized
  - Year (series):
      • Year found in title → must match TMDB year ±2
      • No year in title → exact_normalized or ordered_subset accepted without year check
  - Type unknown → search both movie + TV endpoints, keep best match (strictest level)
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Optional

from redis import ConnectionPool, Redis
from sqlalchemy import not_, or_, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.torrentitem_model import TorrentItemModel
from stream_fusion.services.postgresql.models.mismatch_model import TmdbMismatchModel
from stream_fusion.services.tmdb_matcher.history import MatchEntry, MatchResult
from stream_fusion.services.tmdb_matcher.tmdb_search import TmdbCandidate, TmdbSearchClient
from stream_fusion.utils.filter.title_matching.cache import RedisRulesCache
from stream_fusion.utils.filter.title_matching.matcher import TitleMatcher
from stream_fusion.utils.filter.title_matching.normalizer import TitleNormalizer

# Accepted match levels for automatic assignment (no fuzzy, no reverse-subset alone)
_AUTO_ACCEPT_LEVELS = {"exact_normalized", "ordered_subset"}

# Level priority for tie-breaking when both movie + TV return a match
_LEVEL_PRIORITY = {
    "exact_normalized": 0,
    "ordered_subset": 1,
    "reverse_ordered_subset": 2,
    "article_stripped": 3,
    "article_stripped_subset": 4,
    "fuzzy_rtn": 5,
}

# Season regex (duplicated locally to avoid importing private internals)
_SEASON_RE = re.compile(r"\bS\d{2}(?:E\d{2}(?:-E?\d{2})?)?\b", re.IGNORECASE)
# Year regex
_YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")

# Ebook/non-video detection — items matching any of these are skipped entirely
_EBOOK_KEYWORDS = (
    "%ebook%", "%epub%", "% cbr%", "%cbr %", "%.cbr%",
    "%cbz%", "%.pdf%", "% pdf %", "%mobi%", "%azw%", "%djvu%", "%manga%",
)
_EBOOK_SIZE_THRESHOLD = 300 * 1024 * 1024  # 300 MB


class OrphanMatcher:
    """Fetches orphan torrent_items and attempts to assign a TMDB ID to each.

    One instance is created per scheduler worker lifetime and reused across runs.
    The TitleNormalizer is initialised lazily on the first `run_batch()` call.
    """

    def __init__(
        self, session_factory: async_sessionmaker, redis_pool: ConnectionPool
    ) -> None:
        self._session_factory = session_factory
        self._redis_pool = redis_pool
        self._normalizer: Optional[TitleNormalizer] = None
        self._matcher: Optional[TitleMatcher] = None

    # ── Initialisation ────────────────────────────────────────────────────────

    async def _ensure_normalizer(self) -> None:
        if self._normalizer is not None:
            return
        redis_client = Redis(connection_pool=self._redis_pool)
        cache = RedisRulesCache(redis_client)
        normalizer = TitleNormalizer(cache=cache, db_session_factory=self._session_factory)
        await normalizer.initialize()
        self._normalizer = normalizer
        self._matcher = TitleMatcher(normalizer)

    # ── Public API ────────────────────────────────────────────────────────────

    async def run_batch(self, batch_size: int) -> MatchResult:
        """Process up to *batch_size* orphan items and return a MatchResult."""
        await self._ensure_normalizer()

        result = MatchResult()
        retry_cutoff = int(time.time()) - 7 * 24 * 3600  # retry after 7 days

        async with TmdbSearchClient() as tmdb:
            async with self._session_factory() as session:
                # 1. Fetch orphans
                # Exclude ebooks: known keywords in title OR size below 300 MB
                # (size IS NULL / 0 = unknown → keep)
                ebook_title_filter = not_(or_(
                    *(TorrentItemModel.raw_title.ilike(p) for p in _EBOOK_KEYWORDS)
                ))
                ebook_size_filter = or_(
                    TorrentItemModel.size.is_(None),
                    TorrentItemModel.size == 0,
                    TorrentItemModel.size >= _EBOOK_SIZE_THRESHOLD,
                )
                stmt = (
                    select(TorrentItemModel)
                    .where(TorrentItemModel.tmdb_id.is_(None))
                    .where(
                        (TorrentItemModel.tmdb_match_attempted_at.is_(None))
                        | (TorrentItemModel.tmdb_match_attempted_at < retry_cutoff)
                    )
                    .where(ebook_title_filter)
                    .where(ebook_size_filter)
                    .order_by(TorrentItemModel.created_at.desc())
                    .limit(batch_size)
                )
                rows = (await session.execute(stmt)).scalars().all()
                if not rows:
                    logger.info("[tmdb_orphan_matching] No orphan items to process")
                    return result

                result.processed = len(rows)

                # 2. Parse titles + detect type, build groups
                groups: dict[tuple[str, str | None], list[TorrentItemModel]] = {}
                item_meta: dict[str, tuple[str, Optional[int], Optional[str]]] = {}
                # item_meta[item.id] = (normalized_title, year, detected_type)

                n = self._normalizer
                for item in rows:
                    raw = n.remove_integrale(item.raw_title)
                    clean = n.extract_clean_title(raw)
                    normalized = n.normalize(clean)
                    year = _extract_year(item.raw_title)
                    detected_type = _detect_type(item)
                    item_meta[item.id] = (normalized, year, detected_type)
                    key = (normalized, detected_type)
                    groups.setdefault(key, []).append(item)

                # 3. Collect known mismatches (single query)
                all_hashes = [i.info_hash for i in rows if i.info_hash]
                mismatch_pairs: set[tuple[str, int]] = set()
                if all_hashes:
                    mis_rows = (await session.execute(
                        select(TmdbMismatchModel.info_hash, TmdbMismatchModel.tmdb_id)
                        .where(TmdbMismatchModel.info_hash.in_(all_hashes))
                    )).fetchall()
                    mismatch_pairs = {(r[0], r[1]) for r in mis_rows}

                # 4. Search TMDB + validate for each unique group
                now_ts = int(time.time())
                matched_item_ids: set[str] = set()

                for (normalized_title, group_type), group_items in groups.items():
                    if not normalized_title:
                        # Mark attempted even if no title
                        await _mark_attempted(session, [i.id for i in group_items], now_ts)
                        result.unmatched += len(group_items)
                        continue

                    # Representative item for year extraction
                    rep = group_items[0]
                    _, year, _ = item_meta[rep.id]

                    # 4a. Search TMDB (one or both endpoints)
                    best = await _find_best_candidate(
                        tmdb, n, self._matcher, normalized_title, group_type, year
                    )

                    # Mark all items in the group as attempted regardless
                    await _mark_attempted(session, [i.id for i in group_items], now_ts)

                    if best is None:
                        result.unmatched += len(group_items)
                        continue

                    candidate, match_level = best

                    # 4b. Assign tmdb_id to each item in the group
                    for item in group_items:
                        if item.info_hash and (item.info_hash, candidate.tmdb_id) in mismatch_pairs:
                            logger.debug(
                                f"[tmdb_orphan_matching] Skipping {item.raw_title!r}: "
                                f"known mismatch ({item.info_hash}, {candidate.tmdb_id})"
                            )
                            result.unmatched += 1
                            continue

                        await session.execute(
                            update(TorrentItemModel)
                            .where(TorrentItemModel.id == item.id)
                            .where(TorrentItemModel.tmdb_id.is_(None))
                            .values(
                                tmdb_id=candidate.tmdb_id,
                                type=candidate.item_type,
                                updated_at=now_ts,
                            )
                        )
                        matched_item_ids.add(item.id)
                        result.matched += 1
                        result.matches.append(MatchEntry(
                            info_hash=item.info_hash or "",
                            raw_title=item.raw_title,
                            tmdb_id=candidate.tmdb_id,
                            tmdb_title=candidate.title,
                            tmdb_year=candidate.year,
                            item_type=candidate.item_type,
                            match_level=match_level,
                        ))

                    if len(group_items) > len([i for i in group_items if i.id in matched_item_ids]):
                        result.unmatched += len(group_items) - sum(
                            1 for i in group_items if i.id in matched_item_ids
                        )

                await session.commit()

        logger.info(
            f"[tmdb_orphan_matching] processed={result.processed} "
            f"matched={result.matched} unmatched={result.unmatched}"
        )
        return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_type(item: TorrentItemModel) -> Optional[str]:
    """Detect whether an item is 'movie' or 'series', or None if unknown."""
    if item.type in ("movie", "series"):
        return item.type
    if item.parsed_data:
        seasons = item.parsed_data.get("seasons")
        if seasons:
            return "series"
    if _SEASON_RE.search(item.raw_title):
        return "series"
    return None


def _extract_year(raw_title: str) -> Optional[int]:
    """Extract the most likely release year from a raw torrent title.

    Uses the same boundary-detection heuristic as the normalizer: the last year
    that appears before a strong technical boundary (language, resolution…).
    Falls back to the first year found if no boundary is present.
    """
    from stream_fusion.utils.filter.title_matching.normalizer import (
        _DOT_SEP_RE, _SPACES_RE, _STRONG_BOUNDARY_RE,
    )
    text = _DOT_SEP_RE.sub(" ", raw_title)
    text = _SPACES_RE.sub(" ", text).strip()

    strong_m = _STRONG_BOUNDARY_RE.search(text)
    anchor_pos = strong_m.start() if strong_m and strong_m.start() > 0 else None

    if anchor_pos is not None:
        last_year = None
        for ym in _YEAR_RE.finditer(text[:anchor_pos]):
            if ym.start() > 0:
                last_year = ym
        if last_year:
            return int(last_year.group(1))

    # Fallback: first year at position > 0
    for m in _YEAR_RE.finditer(text):
        if m.start() > 0:
            return int(m.group(1))
    return None


def _year_ok(parsed_year: Optional[int], tmdb_year: Optional[int], item_type: str) -> bool:
    """Validate year match according to conservative rules.

    - If no parsed_year → returns True (no year to validate against)
    - If parsed_year present and tmdb_year unknown → returns False (can't confirm)
    - tolerance: ±1 for movies, ±2 for series
    """
    if parsed_year is None:
        return True
    if tmdb_year is None:
        return False
    tolerance = 1 if item_type == "movie" else 2
    return abs(parsed_year - tmdb_year) <= tolerance


async def _find_best_candidate(
    tmdb: TmdbSearchClient,
    normalizer: TitleNormalizer,
    matcher: TitleMatcher,
    normalized_title: str,
    item_type: Optional[str],
    year: Optional[int],
) -> Optional[tuple[TmdbCandidate, str]]:
    """Search TMDB and return the best (candidate, match_level) or None.

    If item_type is None, searches both movie and TV and picks the strictest match.
    """
    types_to_search: list[str]
    if item_type == "movie":
        types_to_search = ["movie"]
    elif item_type == "series":
        types_to_search = ["series"]
    else:
        types_to_search = ["movie", "series"]

    # Fire all searches concurrently
    search_coros = []
    for t in types_to_search:
        if t == "movie":
            search_coros.append(tmdb.search_movie(normalized_title, year))
        else:
            search_coros.append(tmdb.search_tv(normalized_title, year))

    results_per_type = await asyncio.gather(*search_coros, return_exceptions=True)

    best_candidate: Optional[TmdbCandidate] = None
    best_level: Optional[str] = None
    best_priority: int = 999

    for candidates in results_per_type:
        if isinstance(candidates, Exception) or not candidates:
            continue
        for candidate in candidates:
            tmdb_titles = [candidate.title]
            matched, level, _ = matcher.match(normalized_title, tmdb_titles)

            if not matched or level not in _AUTO_ACCEPT_LEVELS:
                continue

            # Year validation
            resolved_type = candidate.item_type
            parsed_year = year  # year extracted from raw title

            if resolved_type == "movie":
                if parsed_year is not None:
                    # Year present: MUST match even for exact_normalized
                    if not _year_ok(parsed_year, candidate.year, "movie"):
                        logger.debug(
                            f"[tmdb_orphan_matching] Year mismatch for movie {normalized_title!r}: "
                            f"parsed={parsed_year} tmdb={candidate.year} — skipped"
                        )
                        continue
                else:
                    # No year: only accept exact_normalized
                    if level != "exact_normalized":
                        logger.debug(
                            f"[tmdb_orphan_matching] No year for movie {normalized_title!r} "
                            f"and level={level} (not exact) — skipped"
                        )
                        continue
            else:  # series
                if parsed_year is not None:
                    # Year present: MUST match
                    if not _year_ok(parsed_year, candidate.year, "series"):
                        logger.debug(
                            f"[tmdb_orphan_matching] Year mismatch for series {normalized_title!r}: "
                            f"parsed={parsed_year} tmdb={candidate.year} — skipped"
                        )
                        continue
                # No year for series: accept without year check

            priority = _LEVEL_PRIORITY.get(level, 99)
            if priority < best_priority:
                best_priority = priority
                best_candidate = candidate
                best_level = level

    if best_candidate is None:
        logger.debug(f"[tmdb_orphan_matching] No valid TMDB match for {normalized_title!r}")
    return (best_candidate, best_level) if best_candidate else None


async def _mark_attempted(
    session, item_ids: list[str], now_ts: int
) -> None:
    """Stamp tmdb_match_attempted_at for a list of item IDs."""
    if not item_ids:
        return
    await session.execute(
        update(TorrentItemModel)
        .where(TorrentItemModel.id.in_(item_ids))
        .values(tmdb_match_attempted_at=now_ts)
    )
