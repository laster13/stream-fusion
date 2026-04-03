from typing import List, Optional, Union

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.g3mini.g3mini_api import G3MiniAPI
from stream_fusion.utils.g3mini.g3mini_result import G3MiniResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class G3MiniService:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config

        if settings.g3mini_unique_account and settings.g3mini_api_key:
            self.api_key = settings.g3mini_api_key
        else:
            self.api_key = config.get("g3miniApiKey") or settings.g3mini_api_key

        self.base_url = (settings.g3mini_url or "https://gemini-tracker.org").rstrip("/")

        self.api = G3MiniAPI(
            session=session,
            api_key=self.api_key,
            base_url=self.base_url,
        )

    async def search(self, media: Union[Movie, Series]) -> List[G3MiniResult]:
        if not self.api_key:
            logger.debug("G3MINI: No API key configured, skipping search")
            return []

        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"G3MINI: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[G3MiniResult]:
        logger.debug(f"G3MINI: Searching movie: {media.titles[0]}")

        imdb_id = media.id if media.id and media.id.startswith("tt") else None
        tmdb_id = getattr(media, "tmdb_id", None)

        raw = await self.api.search_movie(tmdb_id=tmdb_id, imdb_id=imdb_id)
        logger.debug(f"G3MINI: {len(raw)} raw results for movie '{media.titles[0]}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[G3MiniResult]:
        logger.debug(f"G3MINI: Searching series: {media.titles[0]}")

        raw_id = media.id.split(":")[0] if media.id else None
        imdb_id = raw_id if raw_id and raw_id.startswith("tt") else None
        tmdb_id = getattr(media, "tmdb_id", None)

        season_num = media.get_season_number()
        episode_num = media.get_episode_number()

        raw = await self.api.search_series(
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            season=season_num,
            episode=episode_num,
        )
        logger.debug(
            f"G3MINI: {len(raw)} raw results for '{media.titles[0]}' "
            f"(S{season_num:02d}E{episode_num:02d})"
        )
        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[G3MiniResult]:
        results = []
        for item in raw_results:
            try:
                result = G3MiniResult().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.debug(f"G3MINI: Skipping item - {e}")
            except Exception as e:
                logger.error(f"G3MINI: Unexpected error building item: {e} | item={item}")
        logger.debug(f"G3MINI: Built {len(results)} final result objects")
        return results
