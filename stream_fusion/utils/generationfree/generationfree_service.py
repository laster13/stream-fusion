from typing import List, Optional, Union

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.generationfree.generationfree_api import GenerationFreeAPI
from stream_fusion.utils.generationfree.generationfree_result import GenerationFreeResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class GenerationFreeService:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config

        if settings.generationfree_unique_account and settings.generationfree_api_key:
            self.api_key = settings.generationfree_api_key
        else:
            self.api_key = config.get("generationfreeApiKey") or settings.generationfree_api_key

        self.base_url = (settings.generationfree_url or "https://generation-free.org").rstrip("/")

        self.api = GenerationFreeAPI(
            session=session,
            api_key=self.api_key,
            base_url=self.base_url,
        )

    async def search(self, media: Union[Movie, Series]) -> List[GenerationFreeResult]:
        if not self.api_key:
            logger.debug("GenerationFree: No API key configured, skipping search")
            return []

        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"GenerationFree: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[GenerationFreeResult]:
        logger.info(f"GenerationFree: Searching movie: {media.titles[0]}")

        imdb_id = media.id if media.id and media.id.startswith("tt") else None
        tmdb_id = getattr(media, "tmdb_id", None)

        raw = await self.api.search_movie(
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
        )

        logger.info(f"GenerationFree: {len(raw)} raw results for movie '{media.titles[0]}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[GenerationFreeResult]:
        logger.info(f"GenerationFree: Searching series: {media.titles[0]}")

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

        logger.info(
            f"GenerationFree: {len(raw)} raw results for '{media.titles[0]}' "
            f"(S{season_num:02d}E{episode_num:02d})"
        )
        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[GenerationFreeResult]:
        results = []

        for item in raw_results:
            try:
                result = GenerationFreeResult().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.info(f"GenerationFree: Skipping item - {e} | item={item}")
            except Exception as e:
                logger.error(f"GenerationFree: Unexpected error while building item: {e} | item={item}")

        logger.info(f"GenerationFree: Built {len(results)} final result objects")
        return results