import time
from typing import List, Union, Set

from RTN import parse

from stream_fusion.logging_config import logger
from stream_fusion.utils.detection import detect_languages
from stream_fusion.utils.yggfilx.yggflix_result import YggflixResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.utils.yggfilx.yggflix_api import YggflixAPI


class YggflixService:
    # Minimum delay between consecutive API calls to avoid triggering 429 rate limits.
    _REQUEST_DELAY = 0.3  # seconds
    # If the title pre-filter keeps >= this ratio of results, a page 2 fetch is worthwhile.
    _PAGE2_TITLE_KEEP_THRESHOLD = 0.8

    def __init__(self, config: dict):
        self.yggflix = YggflixAPI()
        self.has_tmdb = config.get("metadataProvider") == "tmdb"

    def search(self, media: Union[Movie, Series]) -> List[YggflixResult]:
        if isinstance(media, Movie):
            results = self.__search_movie(media)
        elif isinstance(media, Series):
            results = self.__search_series(media)
        else:
            raise TypeError("Only Movie and Series types are allowed as media!")

        return self.__post_process_results(results, media)

    def __filter_out_no_seeders(self, results: List[dict]) -> List[dict]:
        return [result for result in results if result.get("seeders", 0) >= 0]

    def __normalize_title(self, title: str) -> str:
        return " ".join((title or "").split()).strip()

    def __unique_titles(self, titles: List[str]) -> List[str]:
        seen = set()
        unique = []
        for title in titles or []:
            normalized = self.__normalize_title(title)
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique.append(normalized)
        return unique

    def __merge_results(
        self,
        new_items: List[dict],
        merged: List[dict],
        seen_hashes: Set[str],
    ) -> None:
        """Merge new_items into merged, deduplicating by info_hash."""
        for item in new_items:
            info_hash = (item.get("info_hash") or "").lower()
            if info_hash:
                if info_hash in seen_hashes:
                    continue
                seen_hashes.add(info_hash)
            merged.append(item)

    def __normalize_text(self, text: str) -> str:
        """Normalize text for pre-filtering (uses title_matching module if available)."""
        try:
            from stream_fusion.utils.filter.title_matching import get_normalizer
            return get_normalizer().normalize(text)
        except RuntimeError:
            import unicodedata, re
            text = unicodedata.normalize("NFD", text)
            text = "".join(c for c in text if unicodedata.category(c) != "Mn")
            return re.sub(r"\s+", " ", text.lower()).strip()

    def __normalize_for_search(self, title: str) -> str:
        """Light normalization for indexer search queries (uses title_matching module if available)."""
        try:
            from stream_fusion.utils.filter.title_matching import get_normalizer
            return get_normalizer().normalize_for_search(title)
        except RuntimeError:
            return " ".join((title or "").split()).strip()

    def __pre_filter_by_title(self, results: List[dict], normalized_titles: List[str]) -> List[dict]:
        """Keep results whose torrent name contains at least one normalized media title."""
        if not normalized_titles:
            return results
        filtered = []
        for result in results:
            name = self.__normalize_text(result.get("name", ""))
            if any(t in name for t in normalized_titles):
                filtered.append(result)
        return filtered

    def __search_movie(self, media: Movie) -> List[dict]:
        titles = self.__unique_titles(getattr(media, "titles", []) or [])
        year = getattr(media, "year", None)
        normalized_titles = [self.__normalize_text(t) for t in titles]

        logger.info(f"Searching YGG Relay for movie: {media.titles[0] if media.titles else 'unknown'}")

        queries = []
        for title in titles:
            normalized = self.__normalize_for_search(title)
            q = f"{normalized} {year}".strip() if year else normalized
            if q:
                queries.append(q)

        merged_results: List[dict] = []
        seen_hashes: Set[str] = set()

        for i, query in enumerate(queries):
            if i > 0:
                time.sleep(self._REQUEST_DELAY)
            try:
                raw_results = self.yggflix.search_movie(title=query)
                logger.debug(f"YGG Relay movie query '{query}' -> {len(raw_results)} results")
            except Exception as e:
                logger.warning(f"YGG Relay movie query failed '{query}': {e}")
                continue

            if not raw_results:
                continue

            self.__merge_results(raw_results, merged_results, seen_hashes)

            # Decide whether a page 2 fetch is worthwhile based on title pre-filter ratio.
            pre_filtered = self.__pre_filter_by_title(raw_results, normalized_titles)
            keep_ratio = len(pre_filtered) / len(raw_results)
            logger.debug(
                f"YGG Relay: pre-filter '{query}' kept {len(pre_filtered)}/{len(raw_results)} "
                f"(ratio={keep_ratio:.2f})"
            )

            if keep_ratio >= self._PAGE2_TITLE_KEEP_THRESHOLD and len(raw_results) >= 20:
                time.sleep(self._REQUEST_DELAY)
                try:
                    raw_p2 = self.yggflix.search_movie(title=query, offset=100)
                    logger.debug(f"YGG Relay movie page2 '{query}' -> {len(raw_p2)} results")
                    self.__merge_results(raw_p2, merged_results, seen_hashes)
                except Exception as e:
                    logger.warning(f"YGG Relay movie page2 failed '{query}': {e}")

        return merged_results

    def __search_series(self, media: Series) -> List[dict]:
        titles = self.__unique_titles(getattr(media, "titles", []) or [])
        season_num = media.get_season_number()
        episode_num = media.get_episode_number()

        logger.info(f"Searching YGG Relay for series: {media.titles[0] if media.titles else 'unknown'}")

        # Build query list: for each title, generate S##E## first, then S## (for season packs).
        # Both variants are always run — S## catches season packs not tagged with episode numbers.
        queries = []
        for title in titles:
            if season_num is not None and episode_num is not None:
                queries.append(f"{title} S{int(season_num):02d}E{int(episode_num):02d}")
            if season_num is not None:
                queries.append(f"{title} S{int(season_num):02d}")

        merged_results: List[dict] = []
        seen_hashes: Set[str] = set()

        for i, query in enumerate(queries):
            if i > 0:
                # Small delay between requests to stay under the relay's rate limit.
                time.sleep(self._REQUEST_DELAY)
            try:
                raw_results = self.yggflix.search_series(title=query)
                logger.debug(f"YGG Relay series query '{query}' -> {len(raw_results)} results")
            except Exception as e:
                logger.warning(f"YGG Relay series query failed '{query}': {e}")
                continue
            self.__merge_results(raw_results, merged_results, seen_hashes)

        return merged_results

    def __filter_series_results(self, results: List[dict], media: Series) -> List[dict]:
        season_num = media.get_season_number()
        episode_num = media.get_episode_number()

        filtered = []
        for r in results:
            name = r.get("name", "")
            parsed = parse(name)

            if not parsed.seasons:
                filtered.append(r)
                continue

            if season_num in parsed.seasons and not parsed.episodes:
                filtered.append(r)
                continue

            if season_num in parsed.seasons and episode_num in parsed.episodes:
                filtered.append(r)

        if filtered:
            logger.debug(
                f"YGG Relay: pre-filtered {len(results)} -> {len(filtered)} results "
                f"for {media.season}{media.episode}"
            )
            return filtered

        logger.debug(f"YGG Relay: pre-filter found nothing, keeping all {len(results)} results")
        return results

    def __post_process_results(
        self, results: List[dict], media: Union[Movie, Series]
    ) -> List[YggflixResult]:
        if not results:
            logger.debug(f"No results found on YGG Relay for: {media.titles[0]}")
            return []

        results = self.__filter_out_no_seeders(results)

        if isinstance(media, Series):
            results = self.__filter_series_results(results, media)

        results = sorted(results, key=lambda r: r.get("seeders", 0), reverse=True)[:50]
        logger.debug(f"{len(results)} results to process from YGG Relay for: {media.titles[0]}")

        items = []
        for result in results:
            info_hash = result.get("info_hash")
            if not info_hash:
                continue

            item = YggflixResult()
            item.raw_title = result.get("name")
            item.size = result.get("size", 0)
            item.info_hash = info_hash.lower()
            item.magnet = result.get("magnet")
            item.link = result.get("link") or item.magnet
            item.indexer = "Yggtorrent - API"
            item.seeders = result.get("seeders", 0)
            item.privacy = "public"
            item.languages = detect_languages(item.raw_title, default_language="fr")
            item.type = media.type
            item.parsed_data = parse(item.raw_title)
            item.tmdb_id = getattr(media, "tmdb_id", None)
            items.append(item)

        return items
