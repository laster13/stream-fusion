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
        imdb_id = full_id[0]
        session = await self._get_session()

        # Check DB mapping first — may short-circuit the IMDB find API call
        db_tmdb_id: Optional[str] = None
        db_mapping = None
        try:
            from stream_fusion.services.postgresql.dao.metadatamapping_dao import lookup_mapping_by_imdb_id
            db_mapping = await lookup_mapping_by_imdb_id(imdb_id)
            if db_mapping and db_mapping.tmdb_id:
                db_tmdb_id = str(db_mapping.tmdb_id)
                self.logger.info(f"TMDB: using DB mapping {imdb_id} → tmdb_id={db_tmdb_id}")
        except Exception as e:
            self.logger.warning(f"TMDB: could not fetch DB mapping for {imdb_id}: {e}")

        result = None
        tmdb_endpoint = "movie" if type == "movie" else "tv"

        for lang in self.config['languages']:
            if db_tmdb_id:
                # Direct TMDB lookup by ID — no IMDb find needed
                url = f"https://api.themoviedb.org/3/{tmdb_endpoint}/{db_tmdb_id}?api_key={settings.tmdb_api_key}&language={lang}"
                async with session.get(url) as response:
                    data = await response.json()

                if lang == self.config['languages'][0]:
                    if type == "movie":
                        if not data.get("id"):
                            raise ValueError(f"No TMDB data for movie tmdb_id={db_tmdb_id}")
                        result = Movie(
                            id=id,
                            tmdb_id=int(db_tmdb_id),
                            titles=[self.replace_weird_characters(data.get("title", "Unknown"))],
                            year=(data.get("release_date") or "2024")[:4],
                            languages=self.config['languages']
                        )
                    else:
                        if not data.get("id"):
                            raise ValueError(f"No TMDB data for series tmdb_id={db_tmdb_id}")
                        result = Series(
                            id=id,
                            tmdb_id=int(db_tmdb_id),
                            titles=[self.replace_weird_characters(data.get("name", "Unknown"))],
                            season="S{:02d}".format(int(full_id[1])),
                            episode="E{:02d}".format(int(full_id[2])),
                            languages=self.config['languages']
                        )
                else:
                    if type == "movie" and data.get("title"):
                        result.titles.append(self.replace_weird_characters(data["title"]))
                    elif type != "movie" and data.get("name"):
                        result.titles.append(self.replace_weird_characters(data["name"]))
            else:
                # Original IMDb-based find
                url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={settings.tmdb_api_key}&external_source=imdb_id&language={lang}"
                async with session.get(url) as response:
                    data = await response.json()

                logger.trace(data)

                if lang == self.config['languages'][0]:
                    if type == "movie":
                        if not data.get("movie_results") or len(data["movie_results"]) == 0:
                            raise ValueError(f"No TMDB results found for movie with IMDB ID {imdb_id}")
                        result = Movie(
                            id=id,
                            tmdb_id=data["movie_results"][0]["id"],
                            titles=[self.replace_weird_characters(data["movie_results"][0]["title"])],
                            year=data["movie_results"][0]["release_date"][:4],
                            languages=self.config['languages']
                        )
                    else:
                        if not data.get("tv_results") or len(data["tv_results"]) == 0:
                            raise ValueError(f"No TMDB results found for series with IMDB ID {imdb_id}")
                        result = Series(
                            id=id,
                            tmdb_id=data["tv_results"][0]["id"],
                            titles=[self.replace_weird_characters(data["tv_results"][0]["name"])],
                            season="S{:02d}".format(int(full_id[1])),
                            episode="E{:02d}".format(int(full_id[2])),
                            languages=self.config['languages']
                        )
                else:
                    if type == "movie":
                        if data.get("movie_results") and len(data["movie_results"]) > 0:
                            result.titles.append(self.replace_weird_characters(data["movie_results"][0]["title"]))
                    else:
                        if data.get("tv_results") and len(data["tv_results"]) > 0:
                            result.titles.append(self.replace_weird_characters(data["tv_results"][0]["name"]))

        # Override titles with search_titles from DB mapping if defined
        if db_mapping and db_mapping.search_titles and result:
            result.titles = [t for t in db_mapping.search_titles if t]
            self.logger.info(f"TMDB: search_titles override for {imdb_id} → {result.titles}")

        self.logger.debug("Got metadata for " + type + " with id " + id)
        return result
