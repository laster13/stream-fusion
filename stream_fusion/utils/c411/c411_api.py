import re
import aiohttp
import xml.etree.ElementTree as ET
from typing import List, Optional

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class C411RawResult:
    def __init__(self):
        self.raw_title: Optional[str] = None
        self.size: Optional[str] = None
        self.link: Optional[str] = None
        self.indexer: str = "C411 - API"
        self.seeders: int = 0
        self.magnet: Optional[str] = None
        self.info_hash: Optional[str] = None
        self.privacy: str = "public"


class C411API:
    TORZNAB_NS = {"torznab": "http://torznab.com/schemas/2015/feed"}

    def __init__(self, session: Optional[aiohttp.ClientSession] = None, api_key: Optional[str] = None):
        self.base_url = settings.c411_url.rstrip("/") + "/api"
        self.api_key = api_key if api_key is not None else settings.c411_api_key
        self._external_session = session is not None
        self._session = session
        self._timeout = aiohttp.ClientTimeout(sock_read=2)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._external_session = False
        return self._session

    async def _request_xml(self, params: dict) -> Optional[str]:
        if not self.api_key:
            logger.warning("C411: API key not configured (C411_API_KEY), skipping request")
            return None
        # Torznab standard : apikey en query param (pas Bearer header)
        params["apikey"] = self.api_key
        session = await self._get_session()
        try:
            async with session.get(
                self.base_url, params=params, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(sock_read=2, total=5)
            ) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError as e:
            logger.error(f"C411: HTTP error: {e}")
            return None
        except Exception as e:
            logger.error(f"C411: Unexpected error: {e}")
            return None

    def _parse_xml(self, xml_content: str) -> List[C411RawResult]:
        results = []
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.error(f"C411: XML parse error: {e}")
            return results

        for item in root.findall(".//item"):
            try:
                result = C411RawResult()

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
                result.info_hash = hash_el.attrib["value"] if hash_el is not None else None

                type_el = item.find("type")
                result.privacy = type_el.text if type_el is not None else "public"

                # Extraire hash depuis magnet si manquant
                if not result.info_hash and result.magnet:
                    m = re.search(r"btih:([a-fA-F0-9]{40})", result.magnet, re.IGNORECASE)
                    if m:
                        result.info_hash = m.group(1).lower()

                if result.info_hash and len(result.info_hash) == 40:
                    results.append(result)

            except Exception as e:
                logger.debug(f"C411: Error parsing item: {e}")
                continue

        return results

    async def search_movie(
        self,
        tmdb_id: Optional[str] = None,
        title: Optional[str] = None,
    ) -> List[C411RawResult]:
        params = {"t": "movie", "cat": "2000"}
        if tmdb_id:
            params["tmdbid"] = tmdb_id
        elif title:
            params["q"] = title
        else:
            return []
        xml = await self._request_xml(params)
        results = self._parse_xml(xml) if xml else []
        logger.info(f"C411: search_movie tmdb={tmdb_id} → {len(results)} results")
        return results

    async def search_series(
        self,
        tmdb_id: Optional[str] = None,
        title: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[C411RawResult]:
        params = {"t": "tvsearch", "cat": "5000"}
        if tmdb_id:
            params["tmdbid"] = tmdb_id
        elif title:
            params["q"] = title
        else:
            return []
        if season is not None:
            params["season"] = season
        if episode is not None:
            params["ep"] = episode
        xml = await self._request_xml(params)
        results = self._parse_xml(xml) if xml else []
        logger.info(f"C411: search_series tmdb={tmdb_id} s={season} e={episode} → {len(results)} results")
        return results
