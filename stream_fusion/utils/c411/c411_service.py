from typing import List, Optional, Union
import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.c411.c411_api import C411API
from stream_fusion.utils.c411.c411_result import C411Result
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class C411Service:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config
        if settings.c411_unique_account and settings.c411_api_key:
            api_key = settings.c411_api_key
        else:
            api_key = config.get("c411ApiKey") or settings.c411_api_key
        self.api = C411API(session=session, api_key=api_key)
        self.has_tmdb = config.get("metadataProvider") == "tmdb"

    async def search(self, media: Union[Movie, Series]) -> List[C411Result]:
        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"C411: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[C411Result]:
        logger.info(f"C411: Searching movie: {media.titles[0]}")
        tmdb_id = str(media.tmdb_id) if self.has_tmdb and media.tmdb_id else None
        title = media.titles[0] if media.titles else None

        if not tmdb_id and not title:
            logger.debug("C411: No TMDB ID or title available, skipping movie search")
            return []

        raw = await self.api.search_movie(
            tmdb_id=tmdb_id,
            title=title,
        )
        logger.info(f"C411: {len(raw)} raw results for movie '{media.titles[0]}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[C411Result]:
        logger.info(f"C411: Searching series (global): {media.titles[0]}")
        tmdb_id = str(media.tmdb_id) if self.has_tmdb and media.tmdb_id else None
        title = media.titles[0] if media.titles else None

        if not tmdb_id and not title:
            logger.debug("C411: No TMDB ID or title available, skipping series search")
            return []

        # Recherche globale (sans saison/épisode) pour tout stocker en Postgres
        raw = await self.api.search_series(
            tmdb_id=tmdb_id,
            title=title,
        )
        logger.info(f"C411: {len(raw)} raw results for '{media.titles[0]}' (global)")
        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[C411Result]:
        results = []
        for item in raw_results:
            try:
                result = C411Result().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.debug(f"C411: Skipping item — {e}")
        return results
