import re
import unicodedata
from typing import Optional

from sqlalchemy.ext.asyncio import async_sessionmaker

from stream_fusion.logging_config import logger
from stream_fusion.utils.filter.title_matching.cache import RedisRulesCache

_CACHE_KEY = "title_rules"

# Fixed patterns (not configurable — always applied)
_APOSTROPHE_RE = re.compile(r"[''`ʼ\u2019\u2018]")
# Possessive/contraction with 's — remove both apostrophe and the s
# ("world's" → "world", "journey's" → "journey")
_POSSESSIVE_S_RE = re.compile(r"[''`ʼ\u2019\u2018]s\b", re.IGNORECASE)
_SPACES_RE = re.compile(r"\s+")
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_COLON_BEFORE_WORD_RE = re.compile(r":(\S)")
_COLON_SPACES_RE = re.compile(r"\s*:\s*")
_TMDB_FILTER_RE = re.compile(
    r'[<>"/\\|?*\x00-\x1F'
    r'\u2122\u00AE\u00A9\u2120\u00A1\u00BF\u2013\u2014'
    r'\u2018\u2019\u201C\u201D\u2022\u2026\s]+'
)
_INTEGRALE_RE = re.compile(r"\b(INTEGRALE|COMPLET|COMPLETE|INTEGRAL)\b", re.IGNORECASE)
_DOT_SEP_RE = re.compile(r"[._]")

# Brackets/parentheses wrapping purely technical content: a year alone, or a strong
# boundary tag (resolution, language, source…).  These are stripped before boundary
# detection so that e.g. "Film.Title.(2010).FRENCH" is handled correctly.
# Note: we build a simplified pattern that matches the key technical tokens without
# relying on the full _STRONG_BOUNDARY_RE internals.
_BRACKET_YEAR_RE = re.compile(
    r"[\(\[]\s*((?:19|20)\d{2})\s*[\)\]]",
    re.IGNORECASE,
)
_BRACKET_TECH_RE = re.compile(
    r"[\(\[]\s*(?:"
    r"MULTi|MULTI|FRENCH|TRUEFRENCH|VFF|VF2|VFQ|VFI|VOF|VQ|VOQ|VOSTFR|SUBFRENCH"
    r"|(?:480|576|720|1080|2160|4320)[ip]|4K|UHD"
    r"|BluRay|BLU-RAY|BLURAY|HDDVD"
    r"|WEB-DL|WEBDL|WEBRIP|WEB\.RIP"
    r"|HDTV|UHDTV|DVDRIP|DVDSCR|REMUX"
    r")\s*[\)\]]",
    re.IGNORECASE,
)

# Strong boundary: tags that signal the START of the technical section.
# Format: TITRE.YEAR.LANG.RES.SOURCE.CODEC-TEAM
# These always come AFTER the release year → they let us find the last year
# that precedes them, which is the true release year boundary.
_STRONG_BOUNDARY_RE = re.compile(
    r"\b(?:"
    # Language — very unlikely to appear inside a film title
    r"MULTi|MULTI|FRENCH|TRUEFRENCH|VFF|VF2|VFQ|VFI|VOF|VQ|VOQ|VOSTFR|SUBFRENCH"
    r"|(?:480|576|720|1080|2160|4320)[ip]"   # Resolution: 1080p, 720p, 2160p…
    r"|4K|UHD"                               # Alternative resolution markers
    r"|BluRay|BLU-RAY|BLURAY|HDDVD"         # Disc sources
    r"|WEB-DL|WEBDL|WEBRIP|WEB\.RIP"        # Web sources (compound only — bare WEB too risky)
    r"|HDTV|UHDTV|DVDRIP|DVDSCR|REMUX"      # Broadcast / other sources
    r")\b",
    re.IGNORECASE
)

# Season/episode — series-specific boundary (no year needed)
_SEASON_RE = re.compile(r"\bS\d{2}(?:E\d{2}(?:-E?\d{2})?)?\b", re.IGNORECASE)


class TitleNormalizer:
    """
    Normalizes titles for matching and search queries.
    Rules (substitutions, release tags, articles, ligatures) are loaded from
    Redis cache → PostgreSQL DB on initialization.
    """

    def __init__(self, cache: RedisRulesCache, db_session_factory: async_sessionmaker) -> None:
        self._cache = cache
        self._db_factory = db_session_factory

        # Compiled state — rebuilt by _apply_rules()
        self._substitution_map: dict[str, str] = {}
        self._ligature_map: str.maketrans = str.maketrans({})
        self._release_tags_re: Optional[re.Pattern] = None
        self._leading_article_re: Optional[re.Pattern] = None

        # Per-instance caches (cleared on reload)
        self._normalize_cache: dict[str, str] = {}
        self._clean_release_cache: dict[str, str] = {}
        self._clean_tmdb_cache: dict[str, str] = {}

    # ── Initialization ────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Load rules from Redis (or DB as fallback) and compile patterns."""
        rules = self._cache.get(_CACHE_KEY)
        if rules is None:
            logger.debug("TitleNormalizer: cache miss — loading from DB")
            rules = await self._load_from_db()
            if rules:
                self._cache.set(_CACHE_KEY, rules)
        else:
            logger.debug(f"TitleNormalizer: loaded {len(rules)} rules from Redis cache")
        self._apply_rules(rules or [])

    async def reload(self) -> None:
        """Invalidate Redis, reload from DB, recompile patterns."""
        self._cache.invalidate(_CACHE_KEY)
        self._invalidate_caches()
        await self.initialize()
        logger.info("TitleNormalizer: reloaded")

    async def _load_from_db(self) -> list[dict]:
        from stream_fusion.services.postgresql.dao.title_normalization_rule_dao import TitleNormalizationRuleDAO
        try:
            async with self._db_factory() as session:
                dao = TitleNormalizationRuleDAO(session)
                rules = await dao.get_all_active()
                return [r.to_dict() for r in rules]
        except Exception as e:
            logger.error(f"TitleNormalizer: failed to load rules from DB: {e}")
            return []

    def _apply_rules(self, rules: list[dict]) -> None:
        """Compile loaded rules into fast lookup structures."""
        substitutions: dict[str, str] = {}
        ligatures: dict[str, str] = {}
        release_tags: list[str] = []
        articles: list[str] = []

        for r in rules:
            rt = r.get("rule_type", "")
            pattern = r.get("pattern", "")
            replacement = r.get("replacement", "")
            if not pattern:
                continue
            if rt == "substitution":
                substitutions[pattern] = replacement
            elif rt == "ligature":
                ligatures[pattern] = replacement
            elif rt == "release_tag":
                release_tags.append(re.escape(pattern))
            elif rt == "article":
                articles.append(re.escape(pattern))

        self._substitution_map = substitutions
        self._ligature_map = str.maketrans(ligatures) if ligatures else str.maketrans({})

        if release_tags:
            tags_joined = "|".join(release_tags)
            self._release_tags_re = re.compile(
                rf"\b({tags_joined})\b", re.IGNORECASE
            )
        else:
            self._release_tags_re = None

        if articles:
            arts_joined = "|".join(articles)
            self._leading_article_re = re.compile(
                rf"^({arts_joined})\s+", re.IGNORECASE
            )
        else:
            self._leading_article_re = None

        logger.debug(
            f"TitleNormalizer: compiled {len(substitutions)} substitutions, "
            f"{len(ligatures)} ligatures, {len(release_tags)} release tags, "
            f"{len(articles)} articles"
        )

    def _invalidate_caches(self) -> None:
        self._normalize_cache.clear()
        self._clean_release_cache.clear()
        self._clean_tmdb_cache.clear()

    # ── Public normalization API ──────────────────────────────────────────────

    def normalize(self, text: str) -> str:
        """
        Full normalization for title matching:
        1. Apply substitutions (& → and)
        2. Apply ligatures (œ → oe)
        3. Replace apostrophes with space
        4. NFD + strip diacritics
        5. Lowercase + collapse whitespace
        """
        if not text:
            return ""
        if text in self._normalize_cache:
            return self._normalize_cache[text]

        result = self._do_normalize(text)
        self._normalize_cache[text] = result
        return result

    def _do_normalize(self, text: str) -> str:
        # 1. Substitutions (word-boundary aware)
        for src, dst in self._substitution_map.items():
            text = re.sub(rf"(?<!\w){re.escape(src)}(?!\w)", f" {dst} ", text)
        # 2. Ligatures
        if self._ligature_map:
            text = text.translate(self._ligature_map)
        # 3. Possessive 's → remove entirely ("world's" → "world")
        #    Other apostrophes → space ("l'homme" → "l homme")
        text = _POSSESSIVE_S_RE.sub("", text)
        text = _APOSTROPHE_RE.sub(" ", text)
        # 4. NFD + strip combining characters (accents)
        text = unicodedata.normalize("NFD", text)
        text = "".join(c for c in text if unicodedata.category(c) != "Mn")
        # 5. Lowercase + collapse whitespace
        text = text.lower()
        return _SPACES_RE.sub(" ", text).strip()

    def clean_release_title(self, title: str) -> str:
        """Remove release tags and years from a torrent title."""
        if not title:
            return ""
        if title in self._clean_release_cache:
            return self._clean_release_cache[title]

        result = title
        if self._release_tags_re:
            result = self._release_tags_re.sub(" ", result)
        result = _YEAR_RE.sub(" ", result)
        result = _SPACES_RE.sub(" ", result).strip()

        self._clean_release_cache[title] = result
        return result

    def clean_tmdb_title(self, title: str) -> str:
        """Remove special characters from TMDB title and normalize colons."""
        if not title:
            return ""
        if title in self._clean_tmdb_cache:
            return self._clean_tmdb_cache[title]

        result = _COLON_BEFORE_WORD_RE.sub(r" \1", title)
        result = _COLON_SPACES_RE.sub(" ", result)
        result = _TMDB_FILTER_RE.sub(" ", result).strip()

        self._clean_tmdb_cache[title] = result
        return result

    def strip_leading_article(self, text: str) -> str:
        """Remove leading article (the, le, la…) from a normalized text."""
        if not text or not self._leading_article_re:
            return text
        return self._leading_article_re.sub("", text).strip()

    def normalize_for_search(self, title: str) -> str:
        """
        Light normalization for indexer search queries:
        substitutions + ligatures + apostrophes → space.
        Preserves accents (some FR indexers handle them natively).
        """
        if not title:
            return ""
        # Substitutions
        for src, dst in self._substitution_map.items():
            title = re.sub(rf"(?<!\w){re.escape(src)}(?!\w)", f" {dst} ", title)
        # Ligatures
        if self._ligature_map:
            title = title.translate(self._ligature_map)
        # Apostrophes → space
        title = _APOSTROPHE_RE.sub(" ", title)
        return _SPACES_RE.sub(" ", title).strip()

    def remove_integrale(self, text: str) -> str:
        """Strip INTEGRALE/COMPLET/COMPLETE/INTEGRAL from a title."""
        return _INTEGRALE_RE.sub("", text).strip()

    def extract_clean_title(self, raw: str) -> str:
        """
        Extract a clean title from a raw torrent name.

        Strategy (TITRE.YEAR.LANG.RES.SOURCE.CODEC-TEAM tracker convention):
        1. Dots/underscores → spaces
        2. Find first strong boundary (language/resolution/source).
           Find last year BEFORE that boundary (pos > 0) → that is the release year.
           Use the release year as the cut point so title-years are preserved
           (e.g. "Blade Runner 2049 2017 FRENCH" → boundary at 2017, not 2049).
        3. Series fallback: if a season marker (S01E05) is found before any strong
           boundary, split there directly (no year needed).
        4. Last-resort fallback: first year at pos > 0 if no other marker found.
        5. Secondary cleanup: remove any remaining DB release tags.
        """
        if not raw:
            return ""

        # Step 1 — separators to spaces
        result = _DOT_SEP_RE.sub(" ", raw)
        result = _SPACES_RE.sub(" ", result).strip()

        # Step 1b — strip brackets/parens containing purely technical content.
        # "(2010)" → "2010", "[BluRay]" → "", keeping brackets with title words intact.
        # Year brackets are replaced with the bare year so the boundary detector can
        # still use it (e.g. "Film Title (2010) FRENCH" → "Film Title 2010 FRENCH").
        result = _BRACKET_YEAR_RE.sub(r" \1 ", result)
        result = _BRACKET_TECH_RE.sub(" ", result)
        result = _SPACES_RE.sub(" ", result).strip()

        title = self._find_title_boundary(result)

        # Step 5 — secondary cleanup of any remaining known tags
        if self._release_tags_re:
            title = self._release_tags_re.sub(" ", title)
        title = _SPACES_RE.sub(" ", title).strip()

        return title

    def _find_title_boundary(self, text: str) -> str:
        """
        Return the text slice that corresponds to the title portion only.
        Uses the boundary detection strategy described in extract_clean_title.
        """
        # Season marker is the most unambiguous boundary for series
        season_m = _SEASON_RE.search(text)

        # First strong technical marker (lang / resolution / source)
        strong_m = _STRONG_BOUNDARY_RE.search(text)

        # Determine the earliest reliable anchor position
        anchors = [m.start() for m in (season_m, strong_m) if m and m.start() > 0]
        anchor_pos = min(anchors) if anchors else None

        if anchor_pos is not None:
            if season_m and season_m.start() == anchor_pos:
                # Series: split directly at season marker — no year needed
                return text[:anchor_pos].strip()

            # Movie: find the last year BEFORE the anchor (pos > 0)
            # This preserves title-years like "2049" in "Blade Runner 2049 2017 FRENCH"
            last_year = None
            for ym in _YEAR_RE.finditer(text[:anchor_pos]):
                if ym.start() > 0:
                    last_year = ym

            if last_year:
                return text[:last_year.start()].strip()
            else:
                # No year before the anchor → split at the anchor directly
                return text[:anchor_pos].strip()

        # Last-resort: first year at pos > 0
        for m in _YEAR_RE.finditer(text):
            if m.start() > 0:
                return text[:m.start()].strip()

        # Nothing found — return as-is
        return text

    # ── Introspection for admin test page ────────────────────────────────────

    def analyze_steps(self, raw_title: str) -> dict:
        """Return a dict describing each normalization step (for the test page)."""
        after_integrale = self.remove_integrale(raw_title)
        after_dot_to_space = _SPACES_RE.sub(" ", _DOT_SEP_RE.sub(" ", after_integrale)).strip()

        # Detect boundary details for display
        text = after_dot_to_space
        season_m = _SEASON_RE.search(text)
        strong_m = _STRONG_BOUNDARY_RE.search(text)
        anchors = [m for m in (season_m, strong_m) if m and m.start() > 0]
        anchor = min(anchors, key=lambda m: m.start()) if anchors else None

        boundary_info = "aucune"
        if anchor:
            if season_m and anchor.start() == season_m.start():
                boundary_info = f"saison « {anchor.group()} » à pos {anchor.start()}"
            else:
                last_year = None
                for ym in _YEAR_RE.finditer(text[:anchor.start()]):
                    if ym.start() > 0:
                        last_year = ym
                if last_year:
                    boundary_info = (
                        f"année « {last_year.group()} » à pos {last_year.start()} "
                        f"(fort: « {anchor.group()} »)"
                    )
                else:
                    boundary_info = f"fort « {anchor.group()} » à pos {anchor.start()}"

        after_boundary = self._find_title_boundary(after_dot_to_space)
        after_clean_release = self.clean_release_title(after_boundary)
        after_normalize = self.normalize(after_clean_release)
        after_strip_article = self.strip_leading_article(after_normalize)
        return {
            "raw_title": raw_title,
            "after_integrale": after_integrale,
            "after_dot_to_space": after_dot_to_space,
            "boundary_detected": boundary_info,
            "after_clean_release": after_clean_release,
            "after_normalize": after_normalize,
            "after_strip_article": after_strip_article,
        }
