from typing import Optional, List, Dict, Any
from urllib.parse import urlencode

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class AbnAPI:
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.session = session
        self.api_key = api_key
        self.api_url = (base_url or settings.abn_api_url or "https://api.abn.lol").rstrip("/")

    async def search_movie(self, title: Optional[str] = None) -> List[Dict[str, Any]]:
        if not title:
            return []
        return await self._search(name=title)

    async def search_series(
        self,
        title: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not title:
            return []
        queries = []
        if season is not None and episode is not None:
            queries.append(f"{title} S{season:02d}E{episode:02d}")
        if season is not None:
            queries.append(f"{title} S{season:02d}")
        queries.append(title)

        all_results: List[Dict[str, Any]] = []
        seen: set = set()
        for q in queries:
            results = await self._search(name=q)
            for item in results:
                info_hash = self._extract_hash(item)
                key = info_hash or item.get("name", "")
                if key and key not in seen:
                    seen.add(key)
                    all_results.append(item)

        logger.debug(f"ABN: search_series '{title}' S{season}E{episode} → {len(all_results)} unique results")
        return all_results

    async def _search(self, name: str) -> List[Dict[str, Any]]:
        if not self.api_key:
            logger.debug("ABN: No API key configured, skipping")
            return []

        url = f"{self.api_url}/api/Release/Search"
        params = {
            "name": name,
            "resultByPage": 100,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        log_url = f"{url}?{urlencode(params)}"
        logger.debug(f"ABN: Requesting {log_url}")

        try:
            if self.session is None:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    return await self._do_request(session, url, params, headers)
            return await self._do_request(self.session, url, params, headers)
        except Exception as e:
            logger.warning(f"ABN: Request failed: {e}")
            return []

    async def _do_request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Dict[str, Any],
        headers: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        try:
            async with session.get(url, params=params, headers=headers, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"ABN: HTTP {response.status}")
                    return []
                data = await response.json(content_type=None)
                if isinstance(data, dict) and isinstance(data.get("data"), list):
                    results = data["data"]
                elif isinstance(data, list):
                    results = data
                else:
                    results = []
                logger.debug(f"ABN: Got {len(results)} raw results")
                return [r for r in results if isinstance(r, dict)]
        except Exception as e:
            logger.warning(f"ABN: HTTP request error: {e}")
            return []

    @staticmethod
    def _extract_hash(item: Dict[str, Any]) -> Optional[str]:
        raw = item.get("infoHash") or item.get("info_hash") or item.get("hash")
        if raw:
            return str(raw).strip().lower()
        return None
