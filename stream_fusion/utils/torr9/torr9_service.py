from typing import List, Optional, Union
import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.torr9.torr9_api import Torr9API
from stream_fusion.utils.torr9.torr9_result import Torr9Result
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class Torr9Service:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config
        if settings.torr9_unique_account and settings.torr9_api_key:
            api_key = settings.torr9_api_key
        else:
            api_key = config.get("torr9ApiKey") or settings.torr9_api_key
        self.api = Torr9API(session=session, api_key=api_key)

    async def search(self, media: Union[Movie, Series]) -> List[Torr9Result]:
        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"Torr9: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[Torr9Result]:
        logger.info(f"Torr9: Searching movie: {media.titles[0]}")
        imdb_id = media.id if media.id and media.id.startswith("tt") else None
        if not imdb_id:
            logger.debug(f"Torr9: No IMDB ID available, skipping search for '{media.titles[0]}'")
            return []

        raw = await self.api.search_movie(imdb_id=imdb_id)
        logger.info(f"Torr9: {len(raw)} raw results for movie '{media.titles[0]}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[Torr9Result]:
        logger.info(f"Torr9: Searching series (global): {media.titles[0]}")
        raw_id = media.id.split(":")[0] if media.id else None
        imdb_id = raw_id if raw_id and raw_id.startswith("tt") else None
        if not imdb_id:
            logger.debug(f"Torr9: No IMDB ID available, skipping search for '{media.titles[0]}'")
            return []

        # Recherche globale (sans saison/épisode) pour tout stocker en Postgres
        raw = await self.api.search_series(imdb_id=imdb_id)
        logger.info(f"Torr9: {len(raw)} raw results for '{media.titles[0]}' (global)")
        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[Torr9Result]:
        results = []
        for item in raw_results:
            try:
                result = Torr9Result().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.debug(f"Torr9: Skipping item - {e}")
        return results
