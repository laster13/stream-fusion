import asyncio
import hashlib
from typing import Optional, List, Dict, Any
from urllib.parse import urlencode

import aiohttp
import bencodepy

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class G3MiniAPI:
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.session = session
        self.api_key = api_key
        self.base_url = (base_url or settings.g3mini_url or "https://gemini-tracker.org").rstrip("/")

    async def search_movie(
        self,
        tmdb_id: Optional[str | int] = None,
        imdb_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params_list: List[Dict[str, Any]] = []

        if tmdb_id:
            params_list.append({"tmdbId": str(tmdb_id)})
        if imdb_id:
            params_list.append({"imdbId": imdb_id.replace("tt", "")})

        return await self._search_all(params_list)

    async def search_series(
        self,
        tmdb_id: Optional[str | int] = None,
        imdb_id: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        params_list: List[Dict[str, Any]] = []

        if season is not None and episode is not None:
            if tmdb_id:
                params_list.append({"tmdbId": str(tmdb_id), "seasonNumber": season, "episodeNumber": episode})
            if imdb_id:
                params_list.append({"imdbId": imdb_id.replace("tt", ""), "seasonNumber": season, "episodeNumber": episode})
            if tmdb_id:
                params_list.append({"tmdbId": str(tmdb_id), "seasonNumber": season})
            if imdb_id:
                params_list.append({"imdbId": imdb_id.replace("tt", ""), "seasonNumber": season})
        else:
            if tmdb_id:
                params_list.append({"tmdbId": str(tmdb_id)})
            if imdb_id:
                params_list.append({"imdbId": imdb_id.replace("tt", "")})

        return await self._search_all(params_list)

    async def _search_all(self, params_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.api_key:
            logger.debug("G3MINI: No API key configured, skipping")
            return []
        if not params_list:
            return []

        if self.session is None:
            async with aiohttp.ClientSession(trust_env=True) as session:
                return await self._run_requests(session, params_list)
        return await self._run_requests(self.session, params_list)

    async def _run_requests(
        self,
        session: aiohttp.ClientSession,
        params_list: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        tasks = [self._search_tracker(session, params) for params in params_list]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        all_results: List[Dict[str, Any]] = []
        for resp in responses:
            if isinstance(resp, Exception):
                logger.warning(f"G3MINI: Ignored failed search task: {resp}")
                continue
            all_results.extend(resp)

        # For items without a hash in the API response, fetch the .torrent file to compute it
        items_needing_hash = [item for item in all_results if not self._extract_hash(item)]
        if items_needing_hash:
            logger.debug(f"G3MINI: API returned no hash — fetching {len(items_needing_hash)} torrent file(s) to compute hash")
            fetch_tasks = [self._fetch_hash_from_torrent(session, item) for item in items_needing_hash]
            computed_hashes = await asyncio.gather(*fetch_tasks)
            for item, h in zip(items_needing_hash, computed_hashes):
                if h:
                    item["_computed_hash"] = h
                    logger.trace(f"G3MINI: Computed hash {h} for torrent id={item.get('id')}")

        unique_results: Dict[str, Dict[str, Any]] = {}
        for item in all_results:
            info_hash = self._extract_hash(item)
            if info_hash and info_hash not in unique_results:
                unique_results[info_hash] = item

        logger.debug(f"G3MINI: Found {len(unique_results)} unique torrents after deduplication")
        return list(unique_results.values())

    async def _search_tracker(
        self,
        session: aiohttp.ClientSession,
        query_params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/torrents/filter"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        params = {"perPage": 100, **query_params}

        log_url = f"{url}?{urlencode(params)}"
        logger.debug(f"G3MINI: Requesting {log_url}")

        try:
            async with session.get(url, params=params, headers=headers, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"G3MINI: HTTP {response.status}")
                    return []
                data = await response.json(content_type=None)
                if isinstance(data, dict) and isinstance(data.get("data"), list):
                    results = data["data"]
                elif isinstance(data, list):
                    results = data
                else:
                    results = []
                logger.debug(f"G3MINI: {len(results)} raw items for params {query_params}")
                return [r for r in results if isinstance(r, dict)]
        except Exception as e:
            logger.warning(f"G3MINI: Request failed: {e}")
            return []

    async def _fetch_hash_from_torrent(
        self,
        session: aiohttp.ClientSession,
        item: Dict[str, Any],
    ) -> Optional[str]:
        """Download the .torrent file for an item and compute its info_hash."""
        attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
        download_link = attrs.get("download_link")
        if not download_link:
            logger.debug(f"G3MINI: No download_link for torrent id={item.get('id')}")
            return None
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            async with session.get(download_link, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    logger.debug(f"G3MINI: download_link returned HTTP {resp.status} for id={item.get('id')}")
                    return None
                torrent_bytes = await resp.read()
                return self._compute_info_hash(torrent_bytes)
        except Exception as e:
            logger.debug(f"G3MINI: Could not fetch torrent for id={item.get('id')}: {e}")
            return None

    @staticmethod
    def _compute_info_hash(torrent_bytes: bytes) -> Optional[str]:
        """Extract info_hash from raw .torrent file content via bencode + SHA1."""
        try:
            metadata = bencodepy.decode(torrent_bytes)
            info = metadata.get(b"info") or metadata.get("info")
            if not info:
                logger.debug("G3MINI: No 'info' dict in torrent metadata")
                return None
            return hashlib.sha1(bencodepy.encode(info)).hexdigest().lower()
        except Exception as e:
            logger.debug(f"G3MINI: Failed to compute info_hash from torrent bytes: {e}")
            return None

    @staticmethod
    def _extract_hash(item: Dict[str, Any]) -> Optional[str]:
        attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
        raw = (
            item.get("_computed_hash")
            or item.get("infoHash")
            or item.get("info_hash")
            or item.get("hash")
            or attrs.get("infoHash")
            or attrs.get("info_hash")
            or attrs.get("hash")
        )
        if raw:
            return str(raw).strip().lower()
        return None
