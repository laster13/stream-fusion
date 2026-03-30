import re
import unicodedata
from functools import lru_cache
from typing import List

from RTN import title_match

from stream_fusion.utils.filter.language_filter import LanguageFilter
from stream_fusion.utils.filter.language_priority_filter import LanguagePriorityFilter
from stream_fusion.utils.filter.max_size_filter import MaxSizeFilter
from stream_fusion.utils.filter.quality_exclusion_filter import QualityExclusionFilter
from stream_fusion.utils.filter.title_exclusion_filter import TitleExclusionFilter
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.logging_config import logger

# Resolution and HDR priority maps (lower = better)
quality_order = {"2160p": 0, "1080p": 1, "720p": 2, "480p": 3}
hdr_order = {"DV": 0, "HDR10+": 1, "HDR10": 2, "HDR": 3}

# Pre-compiled regex patterns (compiled once at import time)
_INTEGRALE_PATTERN = re.compile(
    r"\b(INTEGRALE|COMPLET|COMPLETE|INTEGRAL)\b", re.IGNORECASE
)
_RELEASE_TAGS_PATTERN = re.compile(
    r"\b("
    r"DIRFIX|PROPER|REPACK|RERIP|REAL|INTERNAL|LIMITED|UNRATED|EXTENDED|"
    r"REMUX|COMPLETE|COMPLET|INTEGRALE|INTEGRAL|READNFO|SUBFORCED|DUBBED|"
    r"MULTI|VOSTFR|TRUEFRENCH|FRENCH|VFQ|VFF|VF2|VOF|VFI"
    r")\b",
    re.IGNORECASE,
)
_YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")
_SPACES_PATTERN = re.compile(r"\s+")
_TMDB_FILTER_PATTERN = re.compile(
    r'[<>"/\\|?*\x00-\x1F'
    r'\u2122\u00AE\u00A9\u2120\u00A1\u00BF\u2013\u2014'
    r'\u2018\u2019\u201C\u201D\u2022\u2026\s]+'
)
_COLON_BEFORE_WORD_PATTERN = re.compile(r":(\S)")
_COLON_SPACES_PATTERN = re.compile(r"\s*:\s*")


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


@lru_cache(maxsize=512)
def normalize_text(text: str) -> str:
    """Normalize text for title comparison: strip accents, lowercase, collapse whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    return _SPACES_PATTERN.sub(" ", text).strip()


@lru_cache(maxsize=512)
def clean_release_title_for_matching(title: str) -> str:
    """Strip release tags and years from a torrent title to isolate the media name."""
    if not title:
        return ""
    cleaned = _RELEASE_TAGS_PATTERN.sub(" ", title)
    cleaned = _YEAR_PATTERN.sub(" ", cleaned)
    return _SPACES_PATTERN.sub(" ", cleaned).strip()


@lru_cache(maxsize=512)
def clean_tmdb_title(title: str) -> str:
    """Sanitize a TMDB title by removing special characters and normalizing colons."""
    cleaned = _COLON_BEFORE_WORD_PATTERN.sub(r" \1", title)
    cleaned = _COLON_SPACES_PATTERN.sub(" ", cleaned)
    return _TMDB_FILTER_PATTERN.sub(" ", cleaned).strip()


def remove_non_matching_title(items, titles):
    """Remove items whose parsed title does not match any of the provided media titles."""
    cleaned_titles = [
        _INTEGRALE_PATTERN.sub("", clean_tmdb_title(t)).strip() for t in titles
    ]
    logger.trace(f"Filters: Removing items not matching titles: {cleaned_titles}")

    def normalize_words(text):
        return normalize_text(text).split()

    def is_ordered_subset(subset: str, full_set: str) -> bool:
        subset_words = normalize_words(subset)
        it = iter(normalize_words(full_set))
        return all(w in it for w in subset_words)

    # Pre-compute normalized forms of TMDB titles once (not N×M times)
    precomputed_titles = [
        {
            "title": t,
            "words": normalize_words(t),
            "normalized": normalize_text(t),
        }
        for t in cleaned_titles
    ]

    # Local cache for RTN title_match to avoid redundant Levenshtein computations
    _title_match_cache: dict[tuple[str, str], bool] = {}

    filtered_items = []
    for item in items:
        if hasattr(item, "_ensure_parsed_data_valid"):
            item._ensure_parsed_data_valid()

        raw = item.parsed_data.parsed_title if (item.parsed_data and hasattr(item.parsed_data, "parsed_title")) else item.raw_title
        cleaned_item_title = _INTEGRALE_PATTERN.sub("", raw).strip()
        cleaned_item_title_for_match = clean_release_title_for_matching(cleaned_item_title)
        item_words = normalize_words(cleaned_item_title_for_match)
        normalized_item_title = normalize_text(cleaned_item_title_for_match)

        matched = False
        match_reason = None
        matched_title = None

        for td in precomputed_titles:
            title = td["title"]
            title_words = td["words"]
            normalized_title = td["normalized"]

            logger.trace(f"Filters: Comparing item title: {cleaned_item_title_for_match} with title: {title}")

            # Case 1: exact match after normalization
            if normalized_item_title == normalized_title:
                matched, match_reason, matched_title = True, "exact_normalized", title
                break

            # Case 2: item title is an ordered subset of the TMDB title
            if is_ordered_subset(cleaned_item_title_for_match, title):
                matched, match_reason, matched_title = True, "ordered_subset", title
                break

            # Case 3: TMDB title is an ordered subset of the item title (reverse)
            if is_ordered_subset(title, cleaned_item_title_for_match):
                matched, match_reason, matched_title = True, "reverse_ordered_subset", title
                break

            # Case 4: fuzzy RTN match — guarded against single-word false positives
            if len(title_words) >= 2 or len(item_words) >= 2:
                _tm_key = (normalized_title, normalized_item_title)
                if _tm_key not in _title_match_cache:
                    _title_match_cache[_tm_key] = title_match(normalized_title, normalized_item_title)
                if _title_match_cache[_tm_key]:
                    matched, match_reason, matched_title = True, "title_match", title
                    break

        if matched:
            logger.trace(
                f"KEEP TITLE | raw_title={getattr(item, 'raw_title', None)} | "
                f"parsed_title={cleaned_item_title} | "
                f"match_title={cleaned_item_title_for_match} | "
                f"matched_with={matched_title} | "
                f"reason={match_reason}"
            )
            filtered_items.append(item)
        else:
            logger.trace(
                f"REJECT TITLE | raw_title={getattr(item, 'raw_title', None)} | "
                f"parsed_title={cleaned_item_title} | "
                f"match_title={cleaned_item_title_for_match} | "
                f"candidate_titles={cleaned_titles}"
            )

    logger.debug(
        f"Filters: Title filtering summary -> kept={len(filtered_items)} rejected={len(items) - len(filtered_items)}"
    )
    logger.trace(
        f"Filters: Title filtering complete. {len(filtered_items)} items kept out of {len(items)} total"
    )
    return filtered_items


def filter_items(items, media, config, skip_resolution=False):
    """Apply all configured filters to a list of torrent items for the given media."""
    logger.debug(f"Filters: Starting item filtering for media: {media.titles[0]}")

    filters = {
        "languages": LanguageFilter(config),
        "maxSize": MaxSizeFilter(config, media.type),
        "exclusionKeywords": TitleExclusionFilter(config),
    }
    if not skip_resolution:
        filters["exclusion"] = QualityExclusionFilter(config)

    language_priority_filter = LanguagePriorityFilter(config)
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
    """Merge two item lists, deduplicating by (raw_title, size, privacy) and keeping the highest-priority source."""
    logger.trace(
        f"Filters: Merging cached items ({len(cache_items)}) and search items ({len(search_items)})"
    )
    merged_dict: dict[tuple, TorrentItem] = {}

    def add_to_merged(item: TorrentItem):
        key = (item.raw_title, item.size, item.privacy)
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
