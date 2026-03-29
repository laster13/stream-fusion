import os
import aiohttp
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from stream_fusion.utils.metdata.metadata_provider_base import MetadataProvider
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


@asynccontextmanager
async def _get_db_session():
    """Lightweight standalone DB session for use outside FastAPI request context."""
    from stream_fusion.settings import settings
    engine = create_async_engine(str(settings.pg_url), pool_size=1, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        await session.close()
        await engine.dispose()


class Cinemeta(MetadataProvider):

    async def get_metadata(self, id, type):
        self.logger.info("Getting metadata for " + type + " with id " + id)

        full_id = id.split(":")
        imdb_id = full_id[0]

        session = await self._get_session()

        # Fetch Cinemeta
        url = f"https://v3-cinemeta.strem.io/meta/{type}/{imdb_id}.json"
        async with session.get(url) as response:
            data = await response.json()

        # Load DB mapping — takes priority over everything
        db_mapping = await self._get_db_mapping(imdb_id)

        # Resolve title
        title = None
        if "name" in data.get("meta", {}):
            title = self.replace_weird_characters(data["meta"]["name"])
        elif db_mapping and db_mapping.title_override:
            title = db_mapping.title_override
            self.logger.info(f"Using DB title override: {imdb_id} → {title}")
        elif type == "series":
            if "videos" in data.get("meta", {}) and data["meta"]["videos"]:
                first_video = data["meta"]["videos"][0] if isinstance(data["meta"]["videos"], list) else None
                if first_video:
                    title = self.replace_weird_characters(first_video.get("name", "Unknown"))
                else:
                    title = "Unknown"
            else:
                title = "Unknown"
        else:
            title = "Unknown"

        # Resolve TMDB ID
        tmdb_id: Optional[str] = None

        if db_mapping and db_mapping.tmdb_id:
            tmdb_id = db_mapping.tmdb_id
            self.logger.info(f"Using DB mapping: IMDB {imdb_id} → TMDB {tmdb_id}")
        else:
            try:
                tmdb_api_key = os.environ.get("TMDB_API_KEY", "")
                if title and title != "Unknown" and tmdb_api_key:
                    tmdb_url = f"https://api.themoviedb.org/3/search/{type}?query={title}&api_key={tmdb_api_key}"
                    async with session.get(tmdb_url, timeout=aiohttp.ClientTimeout(total=5)) as tmdb_response:
                        tmdb_data = await tmdb_response.json()
                        if tmdb_data.get("results"):
                            tmdb_id = str(tmdb_data["results"][0]["id"])
                            self.logger.info(f"Found TMDB ID {tmdb_id} for {type} '{title}'")
            except Exception as e:
                self.logger.warning(f"Failed to find TMDB ID for '{title}': {e}")

        # Override titles with search_titles from DB mapping if defined
        if db_mapping and db_mapping.search_titles:
            titles_to_use = [t for t in db_mapping.search_titles if t]
            self.logger.info(f"Cinemeta: search_titles override for {imdb_id} → {titles_to_use}")
        else:
            titles_to_use = [title]

        if type == "movie":
            result = Movie(
                id=id,
                tmdb_id=tmdb_id,
                titles=titles_to_use,
                year=data["meta"].get("year", 2024),
                languages=["en"]
            )
        else:
            result = Series(
                id=id,
                tmdb_id=tmdb_id,
                titles=titles_to_use,
                season="S{:02d}".format(int(full_id[1])),
                episode="E{:02d}".format(int(full_id[2])),
                languages=["en"]
            )

        self.logger.info("Got metadata for " + type + " with id " + id)
        return result

    async def _get_db_mapping(self, imdb_id: str):
        """Fetch a metadata mapping from the DB. Returns None gracefully on any error."""
        try:
            from stream_fusion.services.postgresql.dao.metadatamapping_dao import MetadataMappingDAO
            async with _get_db_session() as db_session:
                dao = MetadataMappingDAO(db_session)
                return await dao.get_by_imdb_id(imdb_id)
        except Exception as e:
            self.logger.warning(f"Cinemeta: could not fetch DB mapping for {imdb_id}: {e}")
            return None
