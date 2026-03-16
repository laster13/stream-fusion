from typing import List, Optional
import re
import requests
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from stream_fusion.settings import settings
from stream_fusion.logging_config import logger


class YggflixAPI:
    TORZNAB_NS = {"torznab": "http://torznab.com/schemas/2015/feed"}

    def __init__(self, pool_connections=10, pool_maxsize=50, max_retries=1, timeout=10):
        self.base_url = settings.yggflix_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=retry_strategy,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _make_request(self, params=None):
        try:
            response = self.session.get(self.base_url, params=params or {}, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            logger.error(f"YGG Relay HTTP error occurred: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"YGG Relay connection error occurred: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"YGG Relay timeout error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"YGG Relay request error: {e}")
            raise

    def _parse_xml(self, xml_text: str) -> List[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error(f"YGG Relay XML Parse Error: {e}")
            return []

        items = root.findall(".//item")
        logger.info(f"YGG Relay found {len(items)} results")

        normalized = []
        for item in items:
            try:
                title = item.findtext("title", "")
                size_text = item.findtext("size", "0")
                link = item.findtext("link", "")

                enclosure = item.find("enclosure")
                download_link = enclosure.get("url", "") if enclosure is not None else link

                info_hash = None
                seeders = 0
                leechers = 0
                magnet_url = None

                for attr in item.findall("torznab:attr", self.TORZNAB_NS):
                    name = attr.get("name")
                    value = attr.get("value")
                    if name == "infohash":
                        info_hash = value.lower() if value else None
                    elif name == "seeders":
                        seeders = int(value) if value else 0
                    elif name == "peers":
                        leechers = int(value) if value else 0
                    elif name == "magneturl":
                        magnet_url = value

                final_link = magnet_url or download_link

                if not info_hash and final_link and "btih:" in final_link:
                    hash_match = re.search(r"btih:([a-fA-F0-9]{40})", final_link, re.IGNORECASE)
                    if hash_match:
                        info_hash = hash_match.group(1).lower()

                normalized.append(
                    {
                        "name": title,
                        "size": int(size_text) if size_text else 0,
                        "tracker_name": "YGG Relay",
                        "info_hash": info_hash,
                        "magnet": final_link if final_link.startswith("magnet:") else None,
                        "link": final_link,
                        "source": "ygg",
                        "seeders": seeders,
                        "leechers": leechers,
                        "privacy": "public",
                    }
                )
            except Exception as e:
                logger.debug(f"YGG Relay parse item error: {e}")
                continue

        return normalized

    def search_movie(self, tmdb_id: Optional[int] = None, title: Optional[str] = None) -> List[dict]:
        params = {"t": "movie"}
        if tmdb_id:
            params["tmdbid"] = tmdb_id
        elif title:
            params["q"] = title
        else:
            return []

        xml_text = self._make_request(params=params)
        return self._parse_xml(xml_text)

    def search_series(
        self,
        tmdb_id: Optional[int] = None,
        title: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[dict]:
        params = {"t": "tvsearch"}
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

        xml_text = self._make_request(params=params)
        return self._parse_xml(xml_text)

    def __del__(self):
        self.session.close()