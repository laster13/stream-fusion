import re
from typing import List

from stream_fusion.utils.filter.language_filter import LanguageFilter
from stream_fusion.utils.filter.language_priority_filter import LanguagePriorityFilter
from stream_fusion.utils.filter.max_size_filter import MaxSizeFilter
from stream_fusion.utils.filter.quality_exclusion_filter import QualityExclusionFilter
from stream_fusion.utils.filter.title_exclusion_filter import TitleExclusionFilter
from stream_fusion.utils.filter.title_matching import get_matcher, get_normalizer
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.logging_config import logger

# Resolution and HDR priority maps (lower = better)
quality_order = {"2160p": 0, "1080p": 1, "720p": 2, "480p": 3}
hdr_order = {"DV": 0, "HDR10+": 1, "HDR10": 2, "HDR": 3}

_YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")
_INTEGRALE_PATTERN = re.compile(r"\b(INTEGRALE|COMPLET|COMPLETE|INTEGRAL)\b", re.IGNORECASE)


def get_hdr_priority(hdr_list) -> int:
    """Return HDR priority value (lower = better). DV > HDR10+ > HDR10 > HDR > SDR."""
    if not hdr_list:
        return 99
    return min((hdr_order[h] for h in hdr_list if h in hdr_order), default=99)


def sort_quality(item: TorrentItem) -> tuple:
    """Return (resolution_priority, is_unknown) tuple for quality-based sorting."""
    logger.trace(f"Filters: Evaluating quality for item: {item.raw_title}")
    if hasattr(item, "_ensure_parsed_data_valid"):
        item._ensure_parsed_data_valid()
    if not item.parsed_data or not hasattr(item.parsed_data, "resolution"):
        return float("inf"), True
    resolution = item.parsed_data.resolution
    return quality_order.get(resolution, float("inf")), resolution is None


def get_item_hdr_priority(item: TorrentItem) -> int:
    """Return the HDR priority of a torrent item."""
    if not item.parsed_data or not hasattr(item.parsed_data, "hdr"):
        return 99
    return get_hdr_priority(getattr(item.parsed_data, "hdr", []))


_CATEGORY_SORT_PRIORITY = {
    "priority_private":    1,
    "intermediary_private": 2,
    "fallback_private":    3,
    "public":              4,
}


def get_indexer_priority_for_sort(indexer, config=None) -> int:
    """Return sort priority for a given indexer derived from its search category (lower = higher priority)."""
    indexer_name = (indexer.split(" ")[0] if indexer and " " in indexer else (indexer or "")).lower()
    categories = config.get("indexerCategories", {}) if config else {}
    category = categories.get(indexer_name, "fallback_private")
    priority = _CATEGORY_SORT_PRIORITY.get(category, 999)
    logger.trace(
        f"Filters: Indexer '{indexer}' -> key '{indexer_name}' -> category '{category}' -> priority {priority}"
    )
    return priority


def items_sort(items, config):
    """Sort items by the method specified in config."""
    sort_method = config["sort"]
    logger.trace(f"Filters: Sorting items by method: {sort_method}")

    def _key_quality(x):
        return (sort_quality(x), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0))

    def _key_size_asc(x):
        return (int(x.size), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0))

    def _key_size_desc(x):
        return (-int(x.size), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0))

    def _key_quality_then_size(x):
        return (sort_quality(x), -int(x.size), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0))

    sort_keys = {
        "quality": _key_quality,
        "sizeasc": _key_size_asc,
        "sizedesc": _key_size_desc,
        "qualitythensize": _key_quality_then_size,
    }

    key_fn = sort_keys.get(sort_method)
    if key_fn is None:
        logger.warning(f"Filters: Unrecognized sort method: {sort_method}. No sorting applied.")
        return items

    sorted_items = sorted(items, key=key_fn)
    logger.trace(f"Filters: Sorting complete — {len(sorted_items)} items sorted by {sort_method}")
    return sorted_items


def filter_out_non_matching_movies(items, year):
    """Filter out movie torrents whose title does not contain a year within ±1 of the target year."""
    logger.debug(f"Filters: Filtering non-matching movies for year: {year}")
    if year is None or (isinstance(year, str) and not year.strip()):
        logger.debug("Filters: No year provided, skipping year filtering")
        return items
    year_min = str(int(year) - 1)
    year_max = str(int(year) + 1)
    year_pattern = re.compile(rf"\b(?:{year_max}|{year}|{year_min})\b")
    logger.debug(f"Filters: YEAR MATCH accepts years in raw_title: {year_min}, {year}, {year_max}")

    filtered_items = []
    for item in items:
        raw_title = getattr(item, "raw_title", "")
        if year_pattern.search(raw_title):
            logger.trace(f"KEEP YEAR | raw_title={raw_title}")
            filtered_items.append(item)
        else:
            logger.trace(f"REJECT YEAR | raw_title={raw_title}")

    logger.info(
        f"Filters: Year filtering summary -> kept={len(filtered_items)} rejected={len(items) - len(filtered_items)}"
    )
    return filtered_items


def filter_out_non_matching_series(items, season, episode):
    """Filter out series torrents that do not match the target season and episode."""
    logger.trace(f"Filters: Filtering non-matching items for season {season} and episode {episode}")
    numeric_season = int(season.replace("S", ""))
    numeric_episode = int(episode.replace("E", ""))
    filtered_items = []

    for item in items:
        if not item.parsed_data or not hasattr(item.parsed_data, "seasons") or not hasattr(item.parsed_data, "episodes"):
            logger.trace(f"Filters: Skipping item with invalid parsed_data: {item.raw_title}")
            continue

        seasons = item.parsed_data.seasons
        episodes = item.parsed_data.episodes

        if not seasons and not episodes:
            if _INTEGRALE_PATTERN.search(item.raw_title):
                logger.trace(f"Filters: Full-series match found for item: {item.raw_title}")
                filtered_items.append(item)
            logger.trace(f"Filters: No season or episode information found for item: {item.raw_title}")
            continue

        if not episodes and numeric_season in seasons:
            logger.trace(f"Filters: Exact season match found for item: {item.raw_title}")
            filtered_items.append(item)
            continue

        if numeric_season in seasons and numeric_episode in episodes:
            logger.trace(f"Filters: Exact season and episode match found for item: {item.raw_title}")
            filtered_items.append(item)

    logger.trace(
        f"Filters: Filtering complete. {len(filtered_items)} matching items found out of {len(items)} total"
    )
    return filtered_items


def remove_non_matching_title(items, titles):
    """Remove items whose parsed title does not match any of the provided media titles."""
    try:
        matcher = get_matcher()
        return matcher.filter_items(items, tuple(titles))
    except RuntimeError:
        # Module not yet initialized (e.g. during tests) — fall back to no-op
        logger.warning("Filters: title_matching module not initialized, skipping title filter")
        return items


def apply_correctness_filters(items, media):
    """Apply correctness filters: confirm the torrent matches the requested media.

    These filters are media-driven (not user-preference-driven) and determine
    whether a torrent is genuinely the right film or series. TMDB ID retroactive
    assignment should happen after this function, before preference filters.
    """
    logger.trace(f"Filters: Initial item count: {len(items)}")

    if media.type == "series":
        logger.trace("Filters: Filtering out non-matching series torrents")
        items = filter_out_non_matching_series(items, media.season, media.episode)
        logger.trace(f"Filters: Item count after season/episode filtering: {len(items)}")

    if media.type == "movie":
        logger.trace("Filters: Filtering out non-matching movie torrents")
        items = filter_out_non_matching_movies(items, media.year)
        logger.trace(f"Filters: Item count after year filtering: {len(items)}")

    logger.trace(f"Filters: Filtering out items not matching titles: {media.titles}")
    items = remove_non_matching_title(items, media.titles)
    logger.trace(f"Filters: Item count after title filtering: {len(items)}")

    return items


def apply_preference_filters(items, media, config, skip_resolution=False):
    """Apply user-preference filters: language, size, exclusions, quality, sorting.

    These filters reflect the user's personal settings and should run after
    TMDB ID retroactive assignment, so that excluded items still get their
    tmdb_id updated in the database.
    """
    filters = {
        "languages": LanguageFilter(config),
        "maxSize": MaxSizeFilter(config, media.type),
        "exclusionKeywords": TitleExclusionFilter(config),
    }
    if not skip_resolution:
        filters["exclusion"] = QualityExclusionFilter(config)

    language_priority_filter = LanguagePriorityFilter(config)

    for filter_name, filter_instance in filters.items():
        try:
            logger.trace(f"Filters: Applying {filter_name} filter: {config[filter_name]}")
            items = filter_instance(items)
            logger.trace(f"Filters: Item count after {filter_name} filter: {len(items)}")
        except Exception as e:
            logger.error(f"Filters: Error while applying {filter_name} filter", exc_info=e)

    try:
        logger.trace("Filters: Applying language priority filter")
        items = language_priority_filter(items)
        logger.trace("Filters: Items sorted by language priority")

        language_groups: dict[int, list] = {}
        for item in items:
            priority = getattr(item, "language_priority", 999)
            language_groups.setdefault(priority, []).append(item)

        items = []
        for priority in sorted(language_groups):
            items.extend(items_sort(language_groups[priority], config))

        logger.trace("Filters: Items sorted by language priority and then by quality")
    except Exception as e:
        logger.error("Filters: Error while applying language priority filter", exc_info=e)

    logger.info(f"Filters: Filtering complete. Final item count: {len(items)}")
    return items


def filter_items(items, media, config, skip_resolution=False):
    """Apply all configured filters to a list of torrent items for the given media.

    Convenience wrapper that chains apply_correctness_filters() and
    apply_preference_filters(). Use the two functions separately in views.py
    to insert TMDB ID retroactive assignment between the two stages.
    """
    logger.debug(f"Filters: Starting item filtering for media: {media.titles[0]}")
    items = apply_correctness_filters(items, media)
    items = apply_preference_filters(items, media, config, skip_resolution=skip_resolution)
    return items


def sort_items(items, config):
    """Sort items according to the method defined in config."""
    if config["sort"] is not None:
        logger.trace(f"Filters: Sorting items according to config: {config['sort']}")
        return items_sort(items, config)
    logger.debug("Filters: No sorting specified, returning items in original order")
    return items


def merge_items(
    cache_items: List[TorrentItem], search_items: List[TorrentItem]
) -> List[TorrentItem]:
    """Merge two item lists, deduplicating across four identity dimensions.

    Deduplication priority (first match wins, with cross-referencing):
      1. info_hash  — same hash = same torrent (100% certainty)
      2. group_id   — same DB group = same content
      3. normalized title + size (512-KB bucket, absolute tolerance)
                   — covers different hashes for the same file across trackers;
                     this catches the most duplicates in practice
      4. (raw_title, size, privacy)  — legacy fallback

    Cross-referencing: when an item matches via one dimension its other dimensions
    are registered under the same key, so subsequent items with a different hash but
    the same normalized title+size will resolve to the same bucket.

    The best representative per bucket is kept (lower indexer priority score,
    then highest seeder count).
    """
    logger.trace(
        f"Filters: Merging cached items ({len(cache_items)}) and search items ({len(search_items)})"
    )
    # Absolute size tolerance: 512 KB.  Covers presence/absence of an NFO file
    # (<10 KB) while being far too small to confuse different encodes (which
    # differ by megabytes).
    _SIZE_BUCKET = 524288  # 512 * 1024 bytes

    merged_dict: dict[tuple, TorrentItem] = {}
    hash_to_key: dict[str, tuple] = {}        # normalized info_hash → assigned dedup key
    group_to_key: dict[int, tuple] = {}        # group_id            → assigned dedup key
    title_size_to_key: dict[tuple, tuple] = {} # (norm_title, size_bucket) → assigned dedup key

    # TitleNormalizer singleton — may be None during cold-start before initialization
    try:
        _normalizer = get_normalizer()
    except Exception:
        _normalizer = None

    def _norm_title(raw_title: str):
        """Return the pipeline-normalized title string, or None on failure."""
        if _normalizer is None or not raw_title:
            return None
        try:
            clean = _normalizer.extract_clean_title(raw_title)
            return _normalizer.normalize(clean)
        except Exception:
            return None

    def _register(key: tuple, norm_hash, gid, ts_keys: list):
        """Register all identity dimensions under the given key for future cross-refs.

        ts_keys is a list of up to two adjacent bucket keys so that items at a
        512-KB bucket boundary are still matched against each other.
        """
        if norm_hash and norm_hash not in hash_to_key:
            hash_to_key[norm_hash] = key
        if gid is not None and gid not in group_to_key:
            group_to_key[gid] = key
        for tsk in ts_keys:
            if tsk not in title_size_to_key:
                title_size_to_key[tsk] = key

    def _dedup_key(item: TorrentItem) -> tuple:
        norm_hash = item.info_hash.lower().strip() if item.info_hash else None
        gid = getattr(item, "group_id", None)
        norm_title = _norm_title(item.raw_title)
        try:
            _size = int(item.size) if item.size else 0
        except (ValueError, TypeError):
            _size = 0

        # Build TWO adjacent bucket keys so items that straddle a 512-KB boundary
        # are still treated as duplicates (cross-boundary matching).
        ts_keys: list = []
        if norm_title and _size > 0:
            bucket = _size // _SIZE_BUCKET
            ts_keys = [(norm_title, bucket), (norm_title, bucket - 1)]

        # ── 1. info_hash already seen ──────────────────────────────────────────
        if norm_hash and norm_hash in hash_to_key:
            key = hash_to_key[norm_hash]
            _register(key, norm_hash, gid, ts_keys)
            return key

        # ── 2. group_id already seen ───────────────────────────────────────────
        if gid is not None and gid in group_to_key:
            key = group_to_key[gid]
            _register(key, norm_hash, gid, ts_keys)
            return key

        # ── 3. normalized title + size already seen (check both adjacent buckets)
        for tsk in ts_keys:
            if tsk in title_size_to_key:
                key = title_size_to_key[tsk]
                _register(key, norm_hash, gid, ts_keys)
                return key

        # ── 4. Brand new item — create a key (hash > group > title_size > legacy)
        if norm_hash:
            key = ("hash", norm_hash)
        elif gid is not None:
            key = ("group", gid)
        elif ts_keys:
            key = ("title_size", ts_keys[0])
        else:
            key = ("legacy", item.raw_title, item.size, item.privacy)

        _register(key, norm_hash, gid, ts_keys)
        return key

    def add_to_merged(item: TorrentItem):
        key = _dedup_key(item)
        existing = merged_dict.get(key)
        if existing is None:
            merged_dict[key] = item
        else:
            existing_priority = get_indexer_priority_for_sort(existing.indexer)
            new_priority = get_indexer_priority_for_sort(item.indexer)
            if new_priority < existing_priority or (
                new_priority == existing_priority
                and (item.seeders or 0) > (existing.seeders or 0)
            ):
                merged_dict[key] = item

    for item in cache_items:
        add_to_merged(item)
    for item in search_items:
        add_to_merged(item)

    merged_items = list(merged_dict.values())
    logger.trace(f"Filters: Merging complete. Total unique items: {len(merged_items)}")
    return merged_items
