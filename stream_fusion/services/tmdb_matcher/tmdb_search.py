"""TMDB title-search client for the orphan-matching background job.

Unlike the main TMDB provider (which resolves via IMDb ID), this client
uses the TMDB *search* endpoints to find a movie or TV show by title.
"""
from __future__ import annotations

from typing import Optional

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class TmdbCandidate:
    __slots__ = ("tmdb_id", "title", "year", "item_type")

    def __init__(self, tmdb_id: int, title: str, year: Optional[int], item_type: str) -> None:
        self.tmdb_id = tmdb_id
        self.title = title
        self.year = year
        self.item_type = item_type


class TmdbSearchClient:
    """Async TMDB search client (title-based).

    A single aiohttp.ClientSession is reused across all calls.
    Call `await close()` when done (or use as an async context manager).
    """

    _BASE = "https://api.themoviedb.org/3"

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> "TmdbSearchClient":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    # ── Public API ────────────────────────────────────────────────────────────

    async def search_movie(
        self, title: str, year: Optional[int] = None, max_results: int = 3
    ) -> list[TmdbCandidate]:
        """Search TMDB for a movie by title (+ optional year filter)."""
        params: dict = {"api_key": settings.tmdb_api_key, "query": title}
        if year:
            params["year"] = year
        return await self._fetch(f"{self._BASE}/search/movie", params, "movie", max_results)

    async def search_tv(
        self, title: str, year: Optional[int] = None, max_results: int = 3
    ) -> list[TmdbCandidate]:
        """Search TMDB for a TV show by title (+ optional first_air_date_year filter)."""
        params: dict = {"api_key": settings.tmdb_api_key, "query": title}
        if year:
            params["first_air_date_year"] = year
        return await self._fetch(f"{self._BASE}/search/tv", params, "series", max_results)

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch(
        self, url: str, params: dict, item_type: str, max_results: int
    ) -> list[TmdbCandidate]:
        session = self._get_session()
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"TmdbSearchClient: HTTP {resp.status} for {url} params={params}")
                    return []
                data = await resp.json()
        except Exception as exc:
            logger.warning(f"TmdbSearchClient: request error for {url}: {exc}")
            return []

        results = data.get("results", [])[:max_results]
        candidates: list[TmdbCandidate] = []
        for r in results:
            if item_type == "movie":
                raw_title = r.get("title") or r.get("original_title") or ""
                raw_year = (r.get("release_date") or "")[:4]
            else:
                raw_title = r.get("name") or r.get("original_name") or ""
                raw_year = (r.get("first_air_date") or "")[:4]

            year_int: Optional[int] = None
            if raw_year.isdigit():
                year_int = int(raw_year)

            candidates.append(TmdbCandidate(
                tmdb_id=r["id"],
                title=raw_title,
                year=year_int,
                item_type=item_type,
            ))
        return candidates
