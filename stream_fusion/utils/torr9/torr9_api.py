import re
import aiohttp
import xml.etree.ElementTree as ET
from typing import List, Optional

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class Torr9RawResult:
    def __init__(self):
        self.raw_title: Optional[str] = None
        self.size: Optional[str] = None
        self.link: Optional[str] = None
        self.indexer: str = "Torr9 - API"
        self.seeders: int = 0
        self.magnet: Optional[str] = None
        self.info_hash: Optional[str] = None
        self.privacy: str = "public"


class Torr9API:
    TORZNAB_NS = {"torznab": "http://torznab.com/schemas/2015/feed"}

    def __init__(self, session: Optional[aiohttp.ClientSession] = None, api_key: Optional[str] = None):
        self.base_url = settings.torr9_url.rstrip("/") + "/api/v1/torznab"
        self.api_key = api_key if api_key is not None else settings.torr9_api_key
        self._external_session = session is not None
        self._session = session
        self._timeout = aiohttp.ClientTimeout(sock_read=8, total=15)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._external_session = False
        return self._session

    async def _request_xml(self, params: dict) -> Optional[str]:
        if not self.api_key:
            logger.warning("Torr9: API key not configured (TORR9_API_KEY), skipping request")
            return None

        params["apikey"] = self.api_key
        session = await self._get_session()

        for attempt in range(2):
            try:
                async with session.get(
                    self.base_url,
                    params=params,
                    allow_redirects=True,
                    timeout=self._timeout,
                ) as response:
                    response.raise_for_status()
                    return await response.text()
            except aiohttp.ClientError as e:
                logger.error(f"Torr9: HTTP error (attempt {attempt + 1}/2): {e}")
            except Exception as e:
                logger.error(f"Torr9: Unexpected error (attempt {attempt + 1}/2): {e}")

        return None

    def _parse_xml(self, xml_content: str) -> List[Torr9RawResult]:
        results = []
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.error(f"Torr9: XML parse error: {e}")
            return results

        for item in root.findall(".//item"):
            try:
                result = Torr9RawResult()

                seeders_el = item.find(
                    './/torznab:attr[@name="seeders"]', self.TORZNAB_NS
                )
                result.seeders = int(seeders_el.attrib["value"]) if seeders_el is not None else 0

                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                result.raw_title = title_el.text

                size_el = item.find("size")
                result.size = size_el.text if size_el is not None else "0"

                link_el = item.find("link")
                result.link = link_el.text if link_el is not None else None

                magnet_el = item.find(
                    './/torznab:attr[@name="magneturl"]', self.TORZNAB_NS
                )
                result.magnet = magnet_el.attrib["value"] if magnet_el is not None else None

                hash_el = item.find(
                    './/torznab:attr[@name="infohash"]', self.TORZNAB_NS
                )
                result.info_hash = hash_el.attrib["value"].lower() if hash_el is not None else None

                type_el = item.find("type")
                result.privacy = type_el.text if type_el is not None else "public"

                if not result.info_hash and result.magnet:
                    m = re.search(r"btih:([a-fA-F0-9]{40})", result.magnet, re.IGNORECASE)
                    if m:
                        result.info_hash = m.group(1).lower()

                if result.info_hash and len(result.info_hash) == 40:
                    results.append(result)

            except Exception as e:
                logger.debug(f"Torr9: Error parsing item: {e}")
                continue

        return results

    async def search_movie(
        self,
        imdb_id: Optional[str] = None,
        title: Optional[str] = None,
    ) -> List[Torr9RawResult]:
        results = []

        if imdb_id:
            params = {"t": "movie", "cat": "2000", "imdbid": imdb_id}
            xml = await self._request_xml(params)
            results = self._parse_xml(xml) if xml else []
            logger.info(f"Torr9: search_movie imdb={imdb_id} -> {len(results)} results")

        if not results and title:
            params = {"t": "movie", "cat": "2000", "q": title}
            xml = await self._request_xml(params)
            results = self._parse_xml(xml) if xml else []
            logger.info(f"Torr9: search_movie title={title} -> {len(results)} results")

        return results

    async def search_series(
        self,
        title: Optional[str] = None,
        imdb_id: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[Torr9RawResult]:
        results = []

        if imdb_id:
            params = {"t": "tvsearch", "cat": "5000", "imdbid": imdb_id}
            if season is not None:
                params["season"] = season
            if episode is not None:
                params["ep"] = episode

            xml = await self._request_xml(params)
            results = self._parse_xml(xml) if xml else []
            logger.info(
                f"Torr9: search_series imdb={imdb_id} s={season} e={episode} -> {len(results)} results"
            )

        if not results and title:
            params = {"t": "tvsearch", "cat": "5000", "q": title}
            if season is not None:
                params["season"] = season
            if episode is not None:
                params["ep"] = episode

            xml = await self._request_xml(params)
            results = self._parse_xml(xml) if xml else []
            logger.info(
                f"Torr9: search_series title={title} s={season} e={episode} -> {len(results)} results"
            )

        return results