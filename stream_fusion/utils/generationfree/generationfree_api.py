from typing import Optional, List, Dict, Any
from urllib.parse import urlencode

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class GenerationFreeAPI:
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        self.session = session
        self.api_key = api_key
        self.base_url = (base_url or settings.generationfree_url or "https://generation-free.org").rstrip("/")

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
                params_list.append(
                    {
                        "tmdbId": str(tmdb_id),
                        "seasonNumber": season,
                        "episodeNumber": episode,
                    }
                )
            if imdb_id:
                params_list.append(
                    {
                        "imdbId": imdb_id.replace("tt", ""),
                        "seasonNumber": season,
                        "episodeNumber": episode,
                    }
                )

            if tmdb_id:
                params_list.append(
                    {
                        "tmdbId": str(tmdb_id),
                        "seasonNumber": season,
                    }
                )
            if imdb_id:
                params_list.append(
                    {
                        "imdbId": imdb_id.replace("tt", ""),
                        "seasonNumber": season,
                    }
                )
        else:
            if tmdb_id:
                params_list.append({"tmdbId": str(tmdb_id)})
            if imdb_id:
                params_list.append({"imdbId": imdb_id.replace("tt", "")})

        return await self._search_all(params_list)

    async def _search_all(self, params_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.api_key:
            logger.debug("GenerationFree: No API key configured, skipping API search")
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
        responses = await aiohttp.helpers.asyncio.gather(*tasks, return_exceptions=True)

        all_results: List[Dict[str, Any]] = []
        for resp in responses:
            if isinstance(resp, Exception):
                logger.warning(f"GenerationFree: Ignored failed search task: {resp}")
                continue
            all_results.extend(resp)

        unique_results: dict[str, Dict[str, Any]] = {}
        for item in all_results:
            attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
            info_hash = (
                item.get("info_hash")
                or item.get("hash")
                or attrs.get("info_hash")
                or attrs.get("hash")
            )
            if info_hash:
                info_hash = str(info_hash).strip().lower()
                if info_hash not in unique_results:
                    unique_results[info_hash] = item

        logger.info(
            f"GenerationFree: Found {len(unique_results)} unique torrents after deduplication"
        )
        return list(unique_results.values())

    async def _search_tracker(
        self,
        session: aiohttp.ClientSession,
        query_params: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/torrents/filter"
        params = {
            "api_token": self.api_key,
            **query_params,
        }

        full_url = f"{url}?{urlencode(params)}"
        log_url = full_url.replace(self.api_key, "***TOKEN***")

        logger.info(f"GenerationFree: Requesting {log_url}")

        try:
            async with session.get(full_url, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"GenerationFree: HTTP {response.status}")
                    return []

                data = await response.json(content_type=None)

                results: List[Dict[str, Any]] = []
                if isinstance(data, dict) and isinstance(data.get("data"), list):
                    results = data["data"]
                elif isinstance(data, list):
                    results = data

                cleaned_results: List[Dict[str, Any]] = []
                for res in results:
                    if not isinstance(res, dict):
                        continue
                    cleaned_results.append(res)

                logger.info(
                    f"GenerationFree: Found {len(cleaned_results)} raw items for params {query_params}"
                )
                return cleaned_results

        except Exception as e:
            logger.warning(f"GenerationFree: Request failed: {e}")
            return []