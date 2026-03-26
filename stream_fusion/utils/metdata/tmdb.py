import asyncio
import aiohttp
from typing import Optional

from stream_fusion.utils.metdata.metadata_provider_base import MetadataProvider
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.settings import settings
from stream_fusion.logging_config import logger


class TMDB(MetadataProvider):
    async def get_metadata(self, id, type):
        self.logger.debug("Getting metadata for " + type + " with id " + id)

        full_id = id.split(":")
        session = await self._get_session()

        result = None

        for lang in self.config['languages']:
            url = f"https://api.themoviedb.org/3/find/{full_id[0]}?api_key={settings.tmdb_api_key}&external_source=imdb_id&language={lang}"

            async with session.get(url) as response:
                data = await response.json()

            logger.trace(data)

            if lang == self.config['languages'][0]:
                if type == "movie":
                    # Vérifier si movie_results n'est pas vide
                    if not data.get("movie_results") or len(data["movie_results"]) == 0:
                        raise ValueError(f"No TMDB results found for movie with IMDB ID {full_id[0]}")

                    result = Movie(
                        id=id,
                        tmdb_id=data["movie_results"][0]["id"],
                        titles=[self.replace_weird_characters(data["movie_results"][0]["title"])],
                        year=data["movie_results"][0]["release_date"][:4],
                        languages=self.config['languages']
                    )
                else:
                    # Vérifier si tv_results n'est pas vide
                    if not data.get("tv_results") or len(data["tv_results"]) == 0:
                        raise ValueError(f"No TMDB results found for series with IMDB ID {full_id[0]}")

                    tmdb_id = data["tv_results"][0]["id"]
                    season_num = int(full_id[1])
                    episode_num = int(full_id[2])

                    result = Series(
                        id=id,
                        tmdb_id=tmdb_id,
                        titles=[self.replace_weird_characters(data["tv_results"][0]["name"])],
                        season="S{:02d}".format(season_num),
                        episode="E{:02d}".format(episode_num),
                        languages=self.config['languages']
                    )
            else:
                if type == "movie":
                    if data.get("movie_results") and len(data["movie_results"]) > 0:
                        result.titles.append(self.replace_weird_characters(data["movie_results"][0]["title"]))
                else:
                    if data.get("tv_results") and len(data["tv_results"]) > 0:
                        result.titles.append(self.replace_weird_characters(data["tv_results"][0]["name"]))

        self.logger.debug("Got metadata for " + type + " with id " + id)
        return result
