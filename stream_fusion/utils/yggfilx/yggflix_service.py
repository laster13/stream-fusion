from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Union
from urllib.parse import quote
from RTN import parse

from stream_fusion.logging_config import logger
from stream_fusion.utils.detection import detect_languages
from stream_fusion.utils.yggfilx.yggflix_result import YggflixResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.utils.yggfilx.yggflix_api import YggflixAPI


class YggflixService:

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

    def __build_magnet(self, info_hash: str, title: str) -> str:
        return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(title)}"

    def __search_movie(self, media: Movie) -> List[dict]:
        if not self.has_tmdb:
            raise ValueError("Please use TMDB metadata provider for Yggflix")

        logger.info(f"Searching Yggflix for movie: {media.titles[0]}")
        return self.yggflix.get_movie_torrents(media.tmdb_id)

    def __search_series(self, media: Series) -> List[dict]:
        if not self.has_tmdb:
            raise ValueError("Please use TMDB metadata provider for Yggflix")

        logger.info(f"Searching Yggflix for series: {media.titles[0]}")
        return self.yggflix.get_tvshow_torrents(int(media.tmdb_id))

    def __filter_series_results(self, results: List[dict], media: Series) -> List[dict]:
        season_num = media.get_season_number()
        episode_num = media.get_episode_number()

        filtered = []
        for r in results:
            r_season = r.get("season")
            r_episode = r.get("episode")
            if r_season is None:
                filtered.append(r)
            elif r_season == season_num and r_episode is None:
                filtered.append(r)
            elif r_season == season_num and r_episode == episode_num:
                filtered.append(r)

        if filtered:
            logger.debug(f"Yggflix: pre-filtered {len(results)} → {len(filtered)} results for {media.season}{media.episode}")
        else:
            logger.debug(f"Yggflix: pre-filter found nothing, keeping all {len(results)} results")
            filtered = results
        return filtered

    def __fetch_detail(self, result: dict) -> tuple:
        torrent_id = result.get("id")
        if not torrent_id:
            return result, None
        try:
            detail = self.yggflix.get_torrent_detail(torrent_id)
            return result, detail.get("hash")
        except Exception as e:
            logger.warning(f"Yggflix: failed to get detail for torrent {torrent_id}: {e}")
            return result, None

    def __post_process_results(
        self, results: List[dict], media: Union[Movie, Series]
    ) -> List[YggflixResult]:
        if not results:
            logger.info(f"No results found on Yggflix for: {media.titles[0]}")
            return []

        results = self.__filter_out_no_seeders(results)
        if isinstance(media, Series):
            results = self.__filter_series_results(results, media)
        results = sorted(results, key=lambda r: r.get("seeders", 0), reverse=True)[:50]
        logger.info(f"{len(results)} results to fetch from Yggflix for: {media.titles[0]}")

        items = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.__fetch_detail, r): r for r in results}
            for future in as_completed(futures):
                result, info_hash = future.result()
                if not info_hash:
                    continue
                item = YggflixResult()
                item.raw_title = result["title"]
                item.size = result.get("size", 0)
                item.info_hash = info_hash.lower()
                item.magnet = self.__build_magnet(info_hash, result["title"])
                item.link = item.magnet
                item.indexer = "Yggtorrent - API"
                item.seeders = result.get("seeders", 0)
                item.privacy = "private"
                item.languages = detect_languages(item.raw_title, default_language="fr")
                item.type = media.type
                item.parsed_data = parse(item.raw_title)
                item.tmdb_id = media.tmdb_id
                items.append(item)
                logger.trace(f"Yggflix result: {item}")

        return items
