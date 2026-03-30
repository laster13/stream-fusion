from typing import List, Optional, Union
import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.lacale.lacale_api import LaCaleAPI
from stream_fusion.utils.lacale.lacale_result import LaCaleResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class LaCaleService:
    def __init__(self, config: dict, session: Optional[aiohttp.ClientSession] = None):
        self.config = config

        if getattr(settings, "lacale_unique_account", False) and getattr(settings, "lacale_api_key", None):
            api_key = settings.lacale_api_key
        else:
            api_key = config.get("lacaleApiKey") or getattr(settings, "lacale_api_key", None)

        self.api = LaCaleAPI(session=session, api_key=api_key)
        self.has_tmdb = config.get("metadataProvider") == "tmdb"

    async def search(self, media: Union[Movie, Series]) -> List[LaCaleResult]:
        try:
            if isinstance(media, Movie):
                return await self._search_movie(media)
            elif isinstance(media, Series):
                return await self._search_series(media)
            else:
                raise TypeError("Only Movie and Series are supported")
        except Exception as e:
            logger.error(f"LaCale: Search error: {e}")
            return []

    async def _search_movie(self, media: Movie) -> List[LaCaleResult]:
        logger.info(f"LaCale: Searching movie: {media.titles[0]}")

        tmdb_id = str(media.tmdb_id) if self.has_tmdb and getattr(media, "tmdb_id", None) else None
        imdb_id = media.id if getattr(media, "id", None) and media.id.startswith("tt") else None
        title = media.titles[0] if getattr(media, "titles", None) else None
        year = getattr(media, "year", None)

        raw = await self.api.search_movie(
            title=title,
            year=year,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
        )
        logger.info(f"LaCale: {len(raw)} raw results for movie '{media.titles[0]}'")
        return self._build_results(raw, media)

    async def _search_series(self, media: Series) -> List[LaCaleResult]:
        logger.info(f"LaCale: Searching series: {media.titles[0]}")

        tmdb_id = str(media.tmdb_id) if self.has_tmdb and getattr(media, "tmdb_id", None) else None

        raw_id = media.id.split(":")[0] if getattr(media, "id", None) else None
        imdb_id = raw_id if raw_id and raw_id.startswith("tt") else None

        title = media.titles[0] if getattr(media, "titles", None) else None
        season = media.get_season_number() if hasattr(media, "get_season_number") else None
        episode = media.get_episode_number() if hasattr(media, "get_episode_number") else None

        raw = await self.api.search_series(
            title=title,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            season=season,
            episode=episode,
        )
        logger.info(
            f"LaCale: {len(raw)} raw results for series '{media.titles[0]}' "
            f"(season={season}, episode={episode})"
        )
        return self._build_results(raw, media)

    def _build_results(self, raw_results, media) -> List[LaCaleResult]:
        results = []
        for item in raw_results:
            try:
                result = LaCaleResult().from_api_item(item, media)
                results.append(result)
            except ValueError as e:
                logger.trace(f"LaCale: Skipping item - {e}")
        return results