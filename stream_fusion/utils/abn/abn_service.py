from typing import List, Optional, Union

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.abn.abn_api import AbnAPI
from stream_fusion.utils.abn.abn_result import AbnResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class AbnService:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config

        if settings.abn_unique_account and settings.abn_api_key:
            self.api_key = settings.abn_api_key
        else:
            self.api_key = config.get("abnApiKey") or settings.abn_api_key

        self.api = AbnAPI(
            session=session,
            api_key=self.api_key,
            base_url=settings.abn_api_url,
        )

    async def search(self, media: Union[Movie, Series]) -> List[AbnResult]:
        if not self.api_key:
            logger.debug("ABN: No API key configured, skipping search")
            return []

        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"ABN: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[AbnResult]:
        title = media.titles[0] if media.titles else None
        logger.info(f"ABN: Searching movie: {title}")

        raw = await self.api.search_movie(title=title)
        logger.info(f"ABN: {len(raw)} raw results for movie '{title}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[AbnResult]:
        title = media.titles[0] if media.titles else None
        season_num = media.get_season_number()
        episode_num = media.get_episode_number()
        logger.info(f"ABN: Searching series: {title} S{season_num:02d}E{episode_num:02d}")

        raw = await self.api.search_series(
            title=title,
            season=season_num,
            episode=episode_num,
        )
        logger.info(
            f"ABN: {len(raw)} raw results for '{title}' (S{season_num:02d}E{episode_num:02d})"
        )
        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[AbnResult]:
        results = []
        for item in raw_results:
            try:
                result = AbnResult().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.debug(f"ABN: Skipping item - {e}")
            except Exception as e:
                logger.error(f"ABN: Unexpected error building item: {e} | item={item}")
        logger.info(f"ABN: Built {len(results)} final result objects")
        return results
