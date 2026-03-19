import re
from typing import List

from RTN import title_match

from stream_fusion.utils.filter.language_filter import LanguageFilter
from stream_fusion.utils.filter.language_priority_filter import LanguagePriorityFilter
from stream_fusion.utils.filter.max_size_filter import MaxSizeFilter
from stream_fusion.utils.filter.quality_exclusion_filter import QualityExclusionFilter
from stream_fusion.utils.filter.title_exclusion_filter import TitleExclusionFilter
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.logging_config import logger

quality_order = {"2160p": 0, "1080p": 1, "720p": 2, "480p": 3}

hdr_order = {"DV": 0, "HDR10+": 1, "HDR10": 2, "HDR": 3}

def get_hdr_priority(hdr_list):
    """Retourne la priorité HDR (plus petit = meilleur). DV > HDR10+ > HDR10 > HDR > SDR"""
    if not hdr_list:
        return 99  # SDR
    best = 99
    for h in hdr_list:
        if h in hdr_order:
            best = min(best, hdr_order[h])
    return best


def sort_quality(item: TorrentItem):
    """Retourne (resolution_priority, is_unknown) pour le tri."""
    logger.trace(f"Filters: Evaluating quality for item: {item.raw_title}")
    # Ensure parsed_data is valid before accessing it
    if hasattr(item, '_ensure_parsed_data_valid'):
        item._ensure_parsed_data_valid()
    # Check if parsed_data exists and is valid
    if not item.parsed_data or not hasattr(item.parsed_data, 'resolution'):
        return float("inf"), True
    resolution = item.parsed_data.resolution
    priority = quality_order.get(resolution, float("inf"))
    return priority, item.parsed_data.resolution is None


def get_item_hdr_priority(item: TorrentItem):
    """Retourne la priorité HDR d'un item."""
    if not item.parsed_data or not hasattr(item.parsed_data, 'hdr'):
        return 99
    return get_hdr_priority(getattr(item.parsed_data, 'hdr', []))


def get_indexer_priority_for_sort(indexer, config=None):
    """Fonction pour obtenir la priorité de l'indexer lors du tri"""
    is_torbox = config and (config.get("debridDownloader") == "TorBox" or "TorBox" in config.get("service", []))
    if is_torbox:
        indexer_priority = {
            "C411": 1,            # C411/Torr9 prioritaires pour TorBox
            "Torr9": 1,
            "LaCale": 1,
            "Yggtorrent": 2,
            "DMM": 3,
            "Public": 4,
            "Sharewood": 5,
            "Jackett": 6,
        }
    else:
        indexer_priority = {
            "C411": 1,            # C411/Torr9 prioritaires pour TorBox
            "Torr9": 1,
            "LaCale": 1,
            "Yggtorrent": 1,
            "DMM": 3,
            "Public": 4,
            "Sharewood": 5,
            "Jackett": 6,
        }
    indexer_name = indexer.split(' ')[0] if indexer and ' ' in indexer else indexer
    priority = indexer_priority.get(indexer_name, 999)
    logger.trace(f"Filters: Indexer '{indexer}' -> extracted '{indexer_name}' -> priority {priority} (TorBox={is_torbox})")
    return priority

def items_sort(items, config):
    logger.info(f"Filters: Sorting items by method: {config['sort']}")
    if config["sort"] == "quality":
        sorted_items = sorted(items, key=lambda x: (sort_quality(x), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0)))
    elif config["sort"] == "sizeasc":
        sorted_items = sorted(items, key=lambda x: (int(x.size), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0)))
    elif config["sort"] == "sizedesc":
        sorted_items = sorted(items, key=lambda x: (-int(x.size), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0)))
    elif config["sort"] == "qualitythensize":
        sorted_items = sorted(items, key=lambda x: (sort_quality(x), -int(x.size), get_indexer_priority_for_sort(x.indexer, config), get_item_hdr_priority(x), getattr(x, "language_priority", 999), -int(x.seeders or 0)))
    else:
        logger.warning(
            f"Filters: Unrecognized sort method: {config['sort']}. No sorting applied."
        )
        sorted_items = items

    logger.success(
        f"Filters: Sorting complete - Quality/Size first, YggFlix priority at equal quality, seeders as tiebreaker. Number of sorted items: {len(sorted_items)}"
    )
    return sorted_items


def filter_out_non_matching_movies(items, year):
    logger.info(f"Filters: Filtering non-matching movies for year: {year}")
    year_min = str(int(year) - 1)
    year_max = str(int(year) + 1)
    year_pattern = re.compile(rf"\b{year_max}|{year}|{year_min}\b")
    filtered_items = []
    for item in items:
        if year_pattern.search(item.raw_title):
            logger.trace(
                f"Filters: Match found for year {year} in item: {item.raw_title}"
            )
            filtered_items.append(item)
        else:
            logger.trace(
                f"Filters: No match found for year {year} in item: {item.raw_title}"
            )
    return filtered_items


def filter_out_non_matching_series(items, season, episode):
    logger.info(
        f"Filters: Filtering non-matching items for season {season} and episode {episode}"
    )
    filtered_items = []
    clean_season = season.replace("S", "")
    clean_episode = episode.replace("E", "")
    numeric_season = int(clean_season)
    numeric_episode = int(clean_episode)

    integrale_pattern = re.compile(
        r"\b(INTEGRALE|COMPLET|COMPLETE|INTEGRAL)\b", re.IGNORECASE
    )

    for item in items:
        # Ensure parsed_data is valid before accessing it
        if not item.parsed_data or not hasattr(item.parsed_data, 'seasons') or not hasattr(item.parsed_data, 'episodes'):
            logger.trace(f"Filters: Skipping item with invalid parsed_data: {item.raw_title}")
            continue

        if len(item.parsed_data.seasons) == 0 and len(item.parsed_data.episodes) == 0:
            if integrale_pattern.search(item.raw_title):
                logger.trace(
                    f"Filters: Integrale match found for item: {item.raw_title}"
                )
                filtered_items.append(item)
            logger.trace(
                f"Filters: No season or episode information found for item: {item.raw_title}"
            )
            continue
        if (
            len(item.parsed_data.episodes) == 0
            and numeric_season in item.parsed_data.seasons
        ):
            logger.trace(
                f"Filters: Exact season match found for item: {item.raw_title}"
            )
            filtered_items.append(item)
            continue
        if (
            numeric_season in item.parsed_data.seasons
            and numeric_episode in item.parsed_data.episodes
        ):
            logger.trace(
                f"Filters: Exact season and episode match found for item: {item.raw_title}"
            )
            filtered_items.append(item)
            continue

    logger.debug(
        f"Filters: Filtering complete. {len(filtered_items)} matching items found out of {len(items)} total"
    )
    return filtered_items


def clean_tmdb_title(title):
    # Dictionary of characters to filter, grouped by category
    characters_to_filter = {
        "punctuation": r'<>"/\\|?*',
        "control": r"\x00-\x1F",
        "symbols": r"\u2122\u00AE\u00A9\u2120\u00A1\u00BF\u2013\u2014\u2018\u2019\u201C\u201D\u2022\u2026",
        "spaces": r"\s+",
    }

    filter_pattern = "".join([f"[{chars}]" for chars in characters_to_filter.values()])
    cleaned_title = re.sub(r":(\S)", r" \1", title)
    cleaned_title = re.sub(r"\s*:\s*", " ", cleaned_title)
    cleaned_title = re.sub(filter_pattern, " ", cleaned_title)
    cleaned_title = cleaned_title.strip()
    cleaned_title = re.sub(characters_to_filter["spaces"], " ", cleaned_title)

    return cleaned_title


def remove_non_matching_title(items, titles):
    filtered_items = []
    integrale_pattern = re.compile(
        r"\b(INTEGRALE|COMPLET|COMPLETE|INTEGRAL)\b", re.IGNORECASE
    )
    cleaned_titles = [clean_tmdb_title(title) for title in titles]
    cleaned_titles = [
        integrale_pattern.sub("", title).strip() for title in cleaned_titles
    ]
    logger.info(f"Filters: Removing items not matching titles: {cleaned_titles}")

    def normalize_words(text):
        return [w for w in text.lower().split() if w]

    def is_ordered_subset(subset, full_set):
        subset_words = normalize_words(subset)
        full_set_words = normalize_words(full_set)
        subset_index = 0
        for word in full_set_words:
            if subset_index < len(subset_words) and word == subset_words[subset_index]:
                subset_index += 1
        return subset_index == len(subset_words)

    for item in items:
        if hasattr(item, "_ensure_parsed_data_valid"):
            item._ensure_parsed_data_valid()

        if item.parsed_data and hasattr(item.parsed_data, "parsed_title"):
            cleaned_item_title = integrale_pattern.sub(
                "", item.parsed_data.parsed_title
            ).strip()
        else:
            cleaned_item_title = integrale_pattern.sub("", item.raw_title).strip()

        item_words = normalize_words(cleaned_item_title)

        for title in cleaned_titles:
            title_words = normalize_words(title)

            logger.trace(
                f"Filters: Comparing item title: {cleaned_item_title} with title: {title}"
            )

            # Cas 1: égalité exacte après nettoyage
            if cleaned_item_title.lower() == title.lower():
                logger.trace(
                    f"Filters: Exact cleaned title match. Item accepted: {cleaned_item_title}"
                )
                filtered_items.append(item)
                break

            # Cas 2: le titre de l'item est un sous-ensemble ordonné du titre TMDB
            # Exemple utile: item un peu tronqué, mais pas plus long que le vrai titre
            if is_ordered_subset(cleaned_item_title, title):
                logger.trace(
                    f"Filters: Ordered subset match found. Item accepted: {cleaned_item_title}"
                )
                filtered_items.append(item)
                break

            # Cas 3: matching flou RTN, mais on protège les titres très courts
            # pour éviter Paradise -> Hell's Paradise
            if len(title_words) >= 2 or len(item_words) >= 2:
                if title_match(title, cleaned_item_title):
                    logger.trace(
                        f"Filters: title_match() succeeded. Item accepted: {cleaned_item_title}"
                    )
                    filtered_items.append(item)
                    break

        else:
            logger.trace(f"Filters: No match found, item skipped: {cleaned_item_title}")

    logger.debug(
        f"Filters: Title filtering complete. {len(filtered_items)} items kept out of {len(items)} total"
    )
    return filtered_items

def filter_items(items, media, config, skip_resolution=False):
    logger.info(f"Filters: Starting item filtering for media: {media.titles[0]}")
    
    # Préparer les filtres (SANS le filtre de résolution si skip_resolution=True)
    filters = {
        "languages": LanguageFilter(config),
        "maxSize": MaxSizeFilter(config, media.type),
        "exclusionKeywords": TitleExclusionFilter(config),
    }
    
    # Ajouter le filtre de résolution seulement si skip_resolution=False
    if not skip_resolution:
        filters["exclusion"] = QualityExclusionFilter(config)
    
    language_priority_filter = LanguagePriorityFilter(config)

    logger.info(f"Filters: Initial item count: {len(items)}")

    if media.type == "series":
        logger.info(f"Filters: Filtering out non-matching series torrents")
        items = filter_out_non_matching_series(items, media.season, media.episode)
        logger.success(
            f"Filters: Item count after season/episode filtering: {len(items)}"
        )

    if media.type == "movie":
        logger.info(f"Filters: Filtering out non-matching movie torrents")
        items = filter_out_non_matching_movies(items, media.year)
        logger.success(f"Filters: Item count after year filtering: {len(items)}")

    logger.info(f"Filters: Filtering out items not matching titles: {media.titles}")
    items = remove_non_matching_title(items, media.titles)
    logger.success(f"Filters: Item count after title filtering: {len(items)}")

    for filter_name, filter_instance in filters.items():
        try:
            logger.info(
                f"Filters: Applying {filter_name} filter: {config[filter_name]}"
            )
            items = filter_instance(items)
            logger.success(
                f"Filters: Item count after {filter_name} filter: {len(items)}"
            )
        except Exception as e:
            logger.error(
                f"Filters: Error while applying {filter_name} filter", exc_info=e
            )

    try:
        logger.info(f"Filters: Applying language priority filter")
        items = language_priority_filter(items)
        logger.success(f"Filters: Items sorted by language priority")
        
        language_groups = {}
        for item in items:
            priority = getattr(item, 'language_priority', 999)
            if priority not in language_groups:
                language_groups[priority] = []
            language_groups[priority].append(item)
        
        sorted_items = []
        for priority in sorted(language_groups.keys()):
            group_items = language_groups[priority]
            sorted_group = items_sort(group_items, config)
            sorted_items.extend(sorted_group)
            
        items = sorted_items
        logger.success(f"Filters: Items sorted by language priority and then by quality")
    except Exception as e:
        logger.error(f"Filters: Error while applying language priority filter", exc_info=e)
    
    logger.success(f"Filters: Filtering complete. Final item count: {len(items)}")
    return items


def sort_items(items, config):
    if config["sort"] is not None:
        logger.info(f"Filters: Sorting items according to config: {config['sort']}")
        return items_sort(items, config)
    else:
        logger.info("Filters: No sorting specified, returning items in original order")
        return items


def merge_items(
    cache_items: List[TorrentItem], search_items: List[TorrentItem]
) -> List[TorrentItem]:
    logger.info(
        f"Filters: Merging cached items ({len(cache_items)}) and search items ({len(search_items)})"
    )
    merged_dict = {}

    def add_to_merged(item: TorrentItem):
        key = (item.raw_title, item.size, item.privacy)
        if key not in merged_dict:
            merged_dict[key] = item
        else:
            existing_priority = get_indexer_priority_for_sort(merged_dict[key].indexer)
            new_priority = get_indexer_priority_for_sort(item.indexer)

            if new_priority < existing_priority or (new_priority == existing_priority and (item.seeders or 0) > (merged_dict[key].seeders or 0)):
                merged_dict[key] = item

    for item in cache_items:
        add_to_merged(item)
    for item in search_items:
        add_to_merged(item)

    merged_items = list(merged_dict.values())
    logger.success(
        f"Filters: Merging complete. Total unique items: {len(merged_items)}"
    )
    return merged_items
