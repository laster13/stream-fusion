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

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        api_key: Optional[str] = None,
    ):
        self.base_url = settings.c411_url.rstrip("/") + "/api"
        self.api_key = api_key if api_key is not None else settings.c411_api_key
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
            logger.warning("C411: API key not configured (C411_API_KEY), skipping request")
            return None

        params = dict(params)
        params["apikey"] = self.api_key
        safe_params = {k: ("***" if k == "apikey" else v) for k, v in params.items()}
        logger.info(f"C411: request params={safe_params}")
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
                    text = await response.text()
                    logger.info(
                        f"C411: HTTP {response.status} received, XML length={len(text) if text else 0}"
                    )
                    return text
            except aiohttp.ClientError as e:
                logger.error(f"C411: HTTP error (attempt {attempt + 1}/2): {e}")
            except Exception as e:
                logger.error(f"C411: Unexpected error (attempt {attempt + 1}/2): {e}")

        return None

    def _parse_xml(self, xml_content: str) -> List[C411RawResult]:
        results: List[C411RawResult] = []
        total_items = 0
        skipped_no_title = 0
        skipped_invalid_hash = 0
        kept_without_hash = 0

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.error(f"C411: XML parse error: {e}")
            return results

        for item in root.findall(".//item"):
            total_items += 1
            try:
                result = C411RawResult()

                seeders_el = item.find(
                    './/torznab:attr[@name="seeders"]', self.TORZNAB_NS
                )
                result.seeders = (
                    int(seeders_el.attrib["value"]) if seeders_el is not None else 0
                )

                title_el = item.find("title")
                if title_el is None or not title_el.text:
                    skipped_no_title += 1
                    continue
                result.raw_title = title_el.text

                size_el = item.find("size")
                result.size = size_el.text if size_el is not None else "0"

                link_el = item.find("link")
                result.link = link_el.text if link_el is not None else None

                magnet_el = item.find(
                    './/torznab:attr[@name="magneturl"]', self.TORZNAB_NS
                )
                result.magnet = (
                    magnet_el.attrib["value"] if magnet_el is not None else None
                )

                hash_el = item.find(
                    './/torznab:attr[@name="infohash"]', self.TORZNAB_NS
                )
                result.info_hash = (
                    hash_el.attrib["value"].lower().strip() if hash_el is not None else None
                )

                type_el = item.find("type")
                result.privacy = type_el.text if type_el is not None else "public"

                if not result.info_hash and result.magnet:
                    m = re.search(
                        r"btih:([a-fA-F0-9]{40})", result.magnet, re.IGNORECASE
                    )
                    if m:
                        result.info_hash = m.group(1).lower()

                if result.info_hash and len(result.info_hash) == 40:
                    results.append(result)
                elif result.magnet or result.link:
                    kept_without_hash += 1
                    logger.info(
                        "C411: keeping item without valid info_hash "
                        f"| title={result.raw_title} "
                        f"| magnet_present={bool(result.magnet)} "
                        f"| link_present={bool(result.link)}"
                    )
                    results.append(result)
                else:
                    skipped_invalid_hash += 1
                    logger.info(
                        "C411: skipped item without valid hash/link "
                        f"| title={result.raw_title} "
                        f"| magnet_present={bool(result.magnet)} "
                        f"| link_present={bool(result.link)} "
                        f"| raw_info_hash={result.info_hash}"
                    )

            except Exception as e:
                logger.debug(f"C411: Error parsing item: {e}")
                continue

        logger.info(
            "C411: XML parse summary -> "
            f"total_items={total_items}, "
            f"kept={len(results)}, "
            f"kept_without_hash={kept_without_hash}, "
            f"skipped_no_title={skipped_no_title}, "
            f"skipped_invalid_hash={skipped_invalid_hash}"
        )
        return results

    def _merge_and_deduplicate_results(
        self, results: List[C411RawResult]
    ) -> List[C411RawResult]:
        deduped: dict = {}

        for result in results:
            key = (
                result.info_hash
                or result.magnet
                or (result.raw_title, result.size, result.link, result.privacy)
            )

            if key not in deduped:
                deduped[key] = result
            else:
                existing = deduped[key]
                if (result.seeders or 0) > (existing.seeders or 0):
                    deduped[key] = result

        logger.info(
            f"C411: dedup summary -> input={len(results)}, unique={len(deduped)}"
        )
        return list(deduped.values())

    async def search_movie(
        self,
        tmdb_id: Optional[str] = None,
        title: Optional[str] = None,
    ) -> List[C411RawResult]:
        all_results: List[C411RawResult] = []

        if tmdb_id:
            params = {"t": "movie", "tmdbid": tmdb_id}
            xml = await self._request_xml(params)
            tmdb_results = self._parse_xml(xml) if xml else []
            logger.info(f"C411: search_movie tmdb={tmdb_id} → {len(tmdb_results)} results")
            all_results.extend(tmdb_results)

        if title:
            params = {"t": "movie", "q": title}
            xml = await self._request_xml(params)
            title_results = self._parse_xml(xml) if xml else []
            logger.info(f"C411: search_movie title={title} → {len(title_results)} results")
            all_results.extend(title_results)

        final_results = self._merge_and_deduplicate_results(all_results)
        logger.info(f"C411: search_movie merged total → {len(final_results)} results")
        return final_results

    async def search_series(
        self,
        tmdb_id: Optional[str] = None,
        title: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[C411RawResult]:
        all_results: List[C411RawResult] = []

        if tmdb_id:
            params = {"t": "tvsearch", "tmdbid": tmdb_id}
            if season is not None:
                params["season"] = season
            if episode is not None:
                params["ep"] = episode

            xml = await self._request_xml(params)
            tmdb_results = self._parse_xml(xml) if xml else []
            logger.info(
                f"C411: search_series tmdb={tmdb_id} s={season} e={episode} → {len(tmdb_results)} results"
            )
            all_results.extend(tmdb_results)

        if title:
            params = {"t": "tvsearch", "q": title}
            if season is not None:
                params["season"] = season
            if episode is not None:
                params["ep"] = episode

            xml = await self._request_xml(params)
            title_results = self._parse_xml(xml) if xml else []
            logger.info(
                f"C411: search_series title={title} s={season} e={episode} → {len(title_results)} results"
            )
            all_results.extend(title_results)

        final_results = self._merge_and_deduplicate_results(all_results)
        logger.info(
            f"C411: search_series merged total → {len(final_results)} results"
        )
        return final_results
