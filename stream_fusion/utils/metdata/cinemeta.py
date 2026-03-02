import os
import aiohttp
from typing import Optional

from stream_fusion.utils.metdata.metadata_provider_base import MetadataProvider
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series


class Cinemeta(MetadataProvider):
    # Manual IMDB to TMDB ID mapping for series without proper IMDB linking on TMDB
    IMDB_TO_TMDB_MAPPING = {
        "tt38776705": "272565",  # Culte - 2Be3
    }

    # Manual series title mapping for fallback (when Cinemeta doesn't return series name)
    SERIES_TITLE_MAPPING = {
        "tt38776705": "Culte - 2Be3",
    }

    async def get_metadata(self, id, type):
        self.logger.info("Getting metadata for " + type + " with id " + id)

        full_id = id.split(":")
        imdb_id = full_id[0]

        session = await self._get_session()

        # Requête Cinemeta
        url = f"https://v3-cinemeta.strem.io/meta/{type}/{imdb_id}.json"
        async with session.get(url) as response:
            data = await response.json()

        # Handle missing name field - use fallback if needed
        title = None
        if "name" in data.get("meta", {}):
            title = self.replace_weird_characters(data["meta"]["name"])
        elif type == "series":
            # Check manual series title mapping first
            if imdb_id in self.SERIES_TITLE_MAPPING:
                title = self.SERIES_TITLE_MAPPING[imdb_id]
                self.logger.info(f"Using manual series title mapping: {imdb_id} → {title}")
            elif "videos" in data.get("meta", {}) and data["meta"]["videos"]:
                # Fallback: use first video/episode name as series title
                first_video = data["meta"]["videos"][0] if isinstance(data["meta"]["videos"], list) else None
                if first_video:
                    episode_name = first_video.get("name", "Unknown")
                    title = self.replace_weird_characters(episode_name)
            else:
                title = "Unknown"
        else:
            title = "Unknown"

        # Check manual mapping for TMDB ID first
        tmdb_id = self.IMDB_TO_TMDB_MAPPING.get(imdb_id, None)

        # If not in manual mapping, try to find TMDB ID from title using TMDB API
        if not tmdb_id:
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
        else:
            self.logger.info(f"Using manual mapping: IMDB {imdb_id} → TMDB {tmdb_id}")

        if type == "movie":
            result = Movie(
                id=id,
                tmdb_id=tmdb_id,
                titles=[title],
                year=data["meta"].get("year", 2024),
                languages=["en"]
            )
        else:
            result = Series(
                id=id,
                tmdb_id=tmdb_id,
                titles=[title],
                season="S{:02d}".format(int(full_id[1])),
                episode="E{:02d}".format(int(full_id[2])),
                languages=["en"]
            )

        self.logger.info("Got metadata for " + type + " with id " + id)
        return result
