import os
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from typing import List, Optional

from RTN import parse

from stream_fusion.utils.jackett.jackett_indexer import JackettIndexer
from stream_fusion.utils.jackett.jackett_result import JackettResult
from stream_fusion.utils.models.movie import Movie
from stream_fusion.utils.models.series import Series
from stream_fusion.utils.detection import detect_languages
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class JackettService:
    def __init__(self, config, session: Optional[aiohttp.ClientSession] = None):
        self.logger = logger

        self.__api_key = settings.jackett_api_key
        self.__base_url = f"{settings.jackett_schema}://{settings.jackett_host}:{settings.jackett_port}/api/v2.0"

        self._external_session = session is not None
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=30)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Retourne la session aiohttp, en crée une si nécessaire."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._external_session = False
        return self._session

    async def close(self):
        """Ferme la session si elle a été créée en interne."""
        if self._session and not self._external_session and not self._session.closed:
            await self._session.close()

    async def search(self, media) -> List[JackettResult]:
        self.logger.info("Started Jackett search for " + media.type + " " + media.titles[0])

        indexers = await self.__get_indexers()

        if isinstance(media, Movie):
            search_func = self.__search_movie_indexer
        elif isinstance(media, Series):
            search_func = self.__search_series_indexer
        else:
            raise TypeError("Only Movie and Series is allowed as media!")

        # Lancer toutes les recherches en parallèle avec asyncio.gather()
        import time
        tasks = []
        for indexer in indexers:
            tasks.append(self.__search_indexer_wrapper(media, indexer, search_func))

        results_nested = await asyncio.gather(*tasks, return_exceptions=True)

        # Aplatir les résultats
        results = []
        for result in results_nested:
            if isinstance(result, Exception):
                self.logger.exception(f"Error in Jackett search: {result}")
            elif result:
                for sublist in result:
                    if sublist:
                        results.extend(sublist)

        return self.__post_process_results(results, media)

    async def __search_indexer_wrapper(self, media, indexer, search_func):
        """Wrapper pour mesurer le temps de recherche par indexer."""
        import time
        self.logger.info(f"Searching on {indexer.title}")
        start_time = time.time()

        try:
            result = await search_func(media, indexer)
            count = len([r for sublist in result for r in sublist]) if result else 0
            self.logger.info(
                f"Search on {indexer.title} took {time.time() - start_time:.2f} seconds and found {count} results"
            )
            return result
        except Exception as e:
            self.logger.exception(f"Error searching on {indexer.title}: {e}")
            return []

    async def __search_movie_indexer(self, movie: Movie, indexer: JackettIndexer) -> List[List[JackettResult]]:
        has_imdb_search_capability = (
            os.getenv("DISABLE_JACKETT_IMDB_SEARCH") != "true"
            and indexer.movie_search_capatabilities is not None
            and 'imdbid' in indexer.movie_search_capatabilities
        )

        if has_imdb_search_capability:
            languages = ['en']
            index_of_language = [index for index, lang in enumerate(movie.languages) if lang == 'en'][0]
            titles = [movie.titles[index_of_language]]
        elif indexer.language == "en":
            languages = movie.languages
            titles = movie.titles
        else:
            index_of_language = [
                index for index, lang in enumerate(movie.languages)
                if lang == indexer.language or lang == 'en'
            ]
            languages = [movie.languages[index] for index in index_of_language]
            titles = [movie.titles[index] for index in index_of_language]

        results = []
        session = await self._get_session()

        for index, lang in enumerate(languages):
            params = {
                'apikey': self.__api_key,
                't': 'movie',
                'cat': '2000',
                'q': titles[index],
                'year': movie.year,
            }

            if has_imdb_search_capability:
                params['imdbid'] = movie.id

            url = f"{self.__base_url}/indexers/{indexer.id}/results/torznab/api"
            url += '?' + '&'.join([f'{k}={v}' for k, v in params.items()])

            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    results.append(self.__get_torrent_links_from_xml(text))
            except Exception:
                self.logger.exception(
                    f"An exception occurred while searching for a movie on Jackett with indexer {indexer.title} and "
                    f"language {lang}."
                )

        return results

    async def __search_series_indexer(self, series: Series, indexer: JackettIndexer) -> List[List[JackettResult]]:
        season = str(int(series.season.replace('S', '')))
        episode = str(int(series.episode.replace('E', '')))

        has_imdb_search_capability = (
            os.getenv("DISABLE_JACKETT_IMDB_SEARCH") != "true"
            and indexer.tv_search_capatabilities is not None
            and 'imdbid' in indexer.tv_search_capatabilities
        )

        if has_imdb_search_capability:
            languages = ['en']
            index_of_language = [index for index, lang in enumerate(series.languages) if lang == 'en'][0]
            titles = [series.titles[index_of_language]]
        elif indexer.language == "en":
            languages = series.languages
            titles = series.titles
        else:
            index_of_language = [
                index for index, lang in enumerate(series.languages)
                if lang == indexer.language or lang == 'en'
            ]
            languages = [series.languages[index] for index in index_of_language]
            titles = [series.titles[index] for index in index_of_language]

        results = []
        session = await self._get_session()

        for index, lang in enumerate(languages):
            params = {
                'apikey': self.__api_key,
                't': 'tvsearch',
                'cat': '5000',
                'q': titles[index],
            }

            if has_imdb_search_capability:
                params['imdbid'] = series.id

            url_title = f"{self.__base_url}/indexers/{indexer.id}/results/torznab/api"
            url_title += '?' + '&'.join([f'{k}={v}' for k, v in params.items()])

            url_season = f"{self.__base_url}/indexers/{indexer.id}/results/torznab/api"
            params_season = {**params, 'season': season}
            url_season += '?' + '&'.join([f'{k}={v}' for k, v in params_season.items()])

            url_ep = f"{self.__base_url}/indexers/{indexer.id}/results/torznab/api"
            params_ep = {**params_season, 'ep': episode}
            url_ep += '?' + '&'.join([f'{k}={v}' for k, v in params_ep.items()])

            try:
                # Lancer les 3 requêtes en parallèle
                async with session.get(url_ep) as response_ep:
                    response_ep.raise_for_status()
                    text_ep = await response_ep.text()
                    data_ep = self.__get_torrent_links_from_xml(text_ep)

                async with session.get(url_season) as response_season:
                    response_season.raise_for_status()
                    text_season = await response_season.text()
                    data_season = self.__get_torrent_links_from_xml(text_season)

                if data_ep:
                    results.append(data_ep)
                if data_season:
                    results.append(data_season)

                if not data_ep and not data_season:
                    async with session.get(url_title) as response_title:
                        response_title.raise_for_status()
                        text_title = await response_title.text()
                        data_title = self.__get_torrent_links_from_xml(text_title)
                        if data_title:
                            results.append(data_title)
            except Exception:
                self.logger.exception(
                    f"An exception occurred while searching for a series on Jackett with indexer {indexer.title} and language {lang}."
                )

        return results

    async def __get_indexers(self) -> List[JackettIndexer]:
        url = f"{self.__base_url}/indexers/all/results/torznab/api?apikey={self.__api_key}&t=indexers&configured=true"

        session = await self._get_session()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                return self.__get_indexer_from_xml(text)
        except Exception:
            self.logger.exception("An exception occurred while getting indexers from Jackett.")
            return []

    def __get_indexer_from_xml(self, xml_content: str) -> List[JackettIndexer]:
        xml_root = ET.fromstring(xml_content)

        indexer_list = []
        for item in xml_root.findall('.//indexer'):
            indexer = JackettIndexer()

            indexer.title = item.find('title').text
            indexer.id = item.attrib['id']
            indexer.link = item.find('link').text
            indexer.type = item.find('type').text
            indexer.language = item.find('language').text.split('-')[0]

            self.logger.info(f"Indexer: {indexer.title} - {indexer.link} - {indexer.type}")

            movie_search = item.find('.//searching/movie-search[@available="yes"]')
            tv_search = item.find('.//searching/tv-search[@available="yes"]')

            if movie_search is not None:
                indexer.movie_search_capatabilities = movie_search.attrib['supportedParams'].split(',')
            else:
                self.logger.info(f"Movie search not available for {indexer.title}")

            if tv_search is not None:
                indexer.tv_search_capatabilities = tv_search.attrib['supportedParams'].split(',')
            else:
                self.logger.info(f"TV search not available for {indexer.title}")

            indexer_list.append(indexer)

        return indexer_list

    def __get_torrent_links_from_xml(self, xml_content: str) -> List[JackettResult]:
        xml_root = ET.fromstring(xml_content)

        result_list = []
        for item in xml_root.findall('.//item'):
            result = JackettResult()

            result.seeders = item.find(
                './/torznab:attr[@name="seeders"]',
                namespaces={'torznab': 'http://torznab.com/schemas/2015/feed'}
            ).attrib['value']
            if int(result.seeders) <= 0:
                continue

            result.raw_title = item.find('title').text
            result.size = item.find('size').text
            result.link = item.find('link').text
            result.indexer = item.find('jackettindexer').text
            result.privacy = item.find('type').text

            magnet = item.find(
                './/torznab:attr[@name="magneturl"]',
                namespaces={'torznab': 'http://torznab.com/schemas/2015/feed'}
            )
            result.magnet = magnet.attrib['value'] if magnet is not None else None

            infoHash = item.find(
                './/torznab:attr[@name="infohash"]',
                namespaces={'torznab': 'http://torznab.com/schemas/2015/feed'}
            )
            result.info_hash = infoHash.attrib['value'] if infoHash is not None else None

            result_list.append(result)

        return result_list

    def __post_process_results(self, results: List[JackettResult], media) -> List[JackettResult]:
        for result in results:
            parsed_result = parse(result.raw_title)

            result.parsed_data = parsed_result
            result.languages = detect_languages(result.raw_title)
            result.type = media.type

            if isinstance(media, Series):
                result.season = media.season
                result.episode = media.episode

        return results
