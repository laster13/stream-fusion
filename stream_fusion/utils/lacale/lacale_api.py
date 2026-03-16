import aiohttp
import logging
import xml.etree.ElementTree as ET
from typing import List, Optional


logger = logging.getLogger(__name__)


class LaCaleRawResult:
    def __init__(self):
        self.raw_title: Optional[str] = None
        self.size: Optional[int] = None
        self.link: Optional[str] = None
        self.indexer: str = "LaCale - API"
        self.seeders: int = 0
        self.magnet: Optional[str] = None
        self.info_hash: Optional[str] = None
        self.privacy: str = "private"


class LaCaleAPI:
    TORZNAB_NS = {"torznab": "http://torznab.com/schemas/2015/feed"}

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        api_key: Optional[str] = None,
        timeout: int = 20,
    ):
        self.base_url = "https://la-cale.space/api/external/torznab"
        self.api_key = api_key
        self._external_session = session is not None
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout, trust_env=True)
            self._external_session = False
        return self._session

    async def close(self):
        if self._session and not self._external_session and not self._session.closed:
            await self._session.close()

    async def _request_xml(self, params: dict) -> Optional[str]:
        if not self.api_key:
            logger.warning("LaCale: API key not configured, skipping request")
            return None

        params["apikey"] = self.api_key
        session = await self._get_session()

        try:
            async with session.get(self.base_url, params=params) as response:
                if response.status in (401, 403):
                    masked = (
                        self.api_key[:4] + "..." + self.api_key[-4:]
                        if len(self.api_key) > 8
                        else "***"
                    )
                    logger.error(
                        f"LaCale: Unauthorized/Forbidden ({response.status}). "
                        f"Using key: {masked}. Please update your API key."
                    )
                    return None

                if response.status != 200:
                    body = await response.text()
                    logger.warning(
                        f"LaCale: HTTP {response.status} for params={params}. "
                        f"Body preview: {body[:200]}"
                    )
                    return None

                return await response.text()

        except aiohttp.ClientError as e:
            logger.error(f"LaCale: HTTP error: {e}")
            return None
        except Exception as e:
            logger.error(f"LaCale: Unexpected error: {e}")
            return None

    def _parse_xml(self, xml_text: str) -> List[LaCaleRawResult]:
        results: List[LaCaleRawResult] = []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error(f"LaCale: XML parse error: {e}")
            return results

        items = root.findall(".//item")
        logger.info(f"LaCale: Found {len(items)} results")

        for item in items:
            try:
                result = LaCaleRawResult()

                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    continue
                result.raw_title = title_el.text

                size_el = item.find("size")
                try:
                    result.size = int(size_el.text) if size_el is not None and size_el.text else 0
                except ValueError:
                    result.size = 0

                link_el = item.find("link")
                link_text = link_el.text if link_el is not None else None

                enclosure = item.find("enclosure")
                enclosure_url = enclosure.get("url") if enclosure is not None else None

                result.link = enclosure_url or link_text

                seeders_el = item.find(
                    './/torznab:attr[@name="seeders"]',
                    self.TORZNAB_NS,
                )
                result.seeders = (
                    int(seeders_el.attrib["value"])
                    if seeders_el is not None and seeders_el.attrib.get("value")
                    else 0
                )

                hash_el = item.find(
                    './/torznab:attr[@name="infohash"]',
                    self.TORZNAB_NS,
                )
                result.info_hash = (
                    hash_el.attrib["value"].lower()
                    if hash_el is not None and hash_el.attrib.get("value")
                    else None
                )

                magnet_el = item.find(
                    './/torznab:attr[@name="magneturl"]',
                    self.TORZNAB_NS,
                )
                result.magnet = (
                    magnet_el.attrib["value"]
                    if magnet_el is not None and magnet_el.attrib.get("value")
                    else None
                )

                if result.info_hash:
                    results.append(result)

            except Exception as e:
                logger.debug(f"LaCale: Error parsing item: {e}")
                continue

        return results

    async def search_movie(
        self,
        title: Optional[str] = None,
        year: Optional[int] = None,
        tmdb_id: Optional[str] = None,
        imdb_id: Optional[str] = None,
    ) -> List[LaCaleRawResult]:
        params = {"t": "movie"}

        if tmdb_id:
            params["tmdbid"] = tmdb_id
        elif imdb_id:
            params["imdbid"] = imdb_id.replace("tt", "")
        elif title:
            params["q"] = f"{title} {year}".strip() if year else title
        else:
            return []

        xml = await self._request_xml(params)
        results = self._parse_xml(xml) if xml else []
        logger.info(
            f"LaCale: search_movie tmdb={tmdb_id} imdb={imdb_id} q={params.get('q')} -> {len(results)} results"
        )
        return results

    async def search_series(
        self,
        title: Optional[str] = None,
        tmdb_id: Optional[str] = None,
        imdb_id: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[LaCaleRawResult]:
        params = {"t": "tvsearch"}

        if tmdb_id:
            params["tmdbid"] = tmdb_id
        elif imdb_id:
            params["imdbid"] = imdb_id.replace("tt", "")
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
        logger.info(
            f"LaCale: search_series tmdb={tmdb_id} imdb={imdb_id} q={params.get('q')} "
            f"s={season} e={episode} -> {len(results)} results"
        )
        return results