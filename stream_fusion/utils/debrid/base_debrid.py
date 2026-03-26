from collections import deque
import asyncio
import re
import time

import aiohttp
import jsonpickle as _jp
from aiohttp_socks import ProxyConnector

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class BaseDebrid:
    def __init__(self, config, session: aiohttp.ClientSession = None):
        self.config = config
        self.logger = logger
        self._external_session = session is not None
        self._session = session

        # Rate limiters
        self.global_limit = 250
        self.global_period = 60
        self.torrent_limit = 1
        self.torrent_period = 1

        self.global_requests = deque()
        self.torrent_requests = deque()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session with optional proxy support."""
        if self._session is None or self._session.closed:
            connector = None
            if settings.proxy_url:
                self.logger.debug(f"BaseDebrid: Using proxy: {settings.proxy_url}")
                connector = ProxyConnector.from_url(str(settings.proxy_url))

            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
        return self._session

    async def close(self):
        """Close the session if we own it."""
        if self._session and not self._external_session and not self._session.closed:
            await self._session.close()

    async def _rate_limit(self, requests_queue, limit, period):
        """Async rate limiter using asyncio.sleep."""
        current_time = time.time()

        while requests_queue and requests_queue[0] <= current_time - period:
            requests_queue.popleft()

        if len(requests_queue) >= limit:
            sleep_time = requests_queue[0] - (current_time - period)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        requests_queue.append(time.time())

    async def _global_rate_limit(self):
        await self._rate_limit(self.global_requests, self.global_limit, self.global_period)

    async def _torrent_rate_limit(self):
        await self._rate_limit(self.torrent_requests, self.torrent_limit, self.torrent_period)

    async def json_response(self, url, method="get", data=None, headers=None, files=None, timeout=30, retry_on_429=True):
        """Make an async HTTP request and return JSON response."""
        await self._global_rate_limit()
        if "torrents" in url:
            await self._torrent_rate_limit()

        session = await self._get_session()
        request_timeout = aiohttp.ClientTimeout(total=timeout)
        max_attempts = 5

        for attempt in range(max_attempts):
            try:
                # Prepare request kwargs
                kwargs = {
                    "headers": headers,
                    "timeout": request_timeout
                }

                if method == "get":
                    async with session.get(url, **kwargs) as response:
                        await self._log_and_raise(response)
                        return await self._parse_json_response(response, attempt, max_attempts)

                elif method == "post":
                    if files:
                        # Handle file uploads with FormData
                        form_data = aiohttp.FormData()
                        if data:
                            for key, value in data.items():
                                form_data.add_field(key, str(value))
                        for key, file_tuple in files.items():
                            if isinstance(file_tuple, tuple):
                                filename, file_content, content_type = file_tuple
                                form_data.add_field(key, file_content, filename=filename, content_type=content_type)
                            else:
                                form_data.add_field(key, file_tuple)
                        async with session.post(url, data=form_data, **kwargs) as response:
                            await self._log_and_raise(response)
                            return await self._parse_json_response(response, attempt, max_attempts)
                    else:
                        async with session.post(url, data=data, **kwargs) as response:
                            await self._log_and_raise(response)
                            return await self._parse_json_response(response, attempt, max_attempts)

                elif method == "put":
                    async with session.put(url, data=data, **kwargs) as response:
                        await self._log_and_raise(response)
                        return await self._parse_json_response(response, attempt, max_attempts)

                elif method == "delete":
                    async with session.delete(url, **kwargs) as response:
                        await self._log_and_raise(response)
                        return await self._parse_json_response(response, attempt, max_attempts)
                else:
                    raise ValueError(f"BaseDebrid: Unsupported HTTP method: {method}")

            except aiohttp.ClientResponseError as e:
                status_code = e.status
                if status_code == 429:
                    if not retry_on_429:
                        self.logger.warning("BaseDebrid: Rate limit exceeded. No retry configured, returning None immediately.")
                        return None
                    wait_time = 2**attempt + 1
                    self.logger.warning(
                        f"BaseDebrid: Rate limit exceeded. Attempt {attempt + 1}/{max_attempts}. Waiting for {wait_time} seconds."
                    )
                    await asyncio.sleep(wait_time)
                elif 400 <= status_code < 500:
                    self.logger.error(
                        f"BaseDebrid: Client error occurred: {e}. Status code: {status_code}"
                    )
                    return None
                elif 500 <= status_code < 600:
                    self.logger.error(
                        f"BaseDebrid: Server error occurred: {e}. Status code: {status_code}"
                    )
                    if attempt < max_attempts - 1:
                        wait_time = 2**attempt + 1
                        self.logger.info(
                            f"BaseDebrid: Retrying in {wait_time} seconds..."
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        return None
                else:
                    self.logger.error(
                        f"BaseDebrid: Unexpected HTTP error occurred: {e}. Status code: {status_code}"
                    )
                    return None

            except aiohttp.ClientConnectorError as e:
                self.logger.error(f"BaseDebrid: Connection error occurred: {e}")
                if attempt < max_attempts - 1:
                    wait_time = 2**attempt + 1
                    self.logger.info(f"BaseDebrid: Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    return None

            except asyncio.TimeoutError:
                self.logger.error(f"BaseDebrid: Request timed out")
                if attempt < max_attempts - 1:
                    wait_time = 2**attempt + 1
                    self.logger.info(f"BaseDebrid: Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    return None

            except aiohttp.ClientError as e:
                self.logger.error(f"BaseDebrid: An unexpected error occurred: {e}")
                return None

        self.logger.error(
            "BaseDebrid: Max attempts reached. Unable to complete request."
        )
        return None

    async def _log_and_raise(self, response):
        """Log response body and headers on error before raising."""
        if response.status >= 400:
            try:
                body = await response.text()
                service = self.__class__.__name__
                url = str(response.url)
                headers = dict(response.headers)
                self.logger.warning(f"{service}: HTTP {response.status} on {url} - body: {body[:500]} - headers: {headers}")
            except Exception:
                pass
        response.raise_for_status()

    async def _parse_json_response(self, response, attempt, max_attempts):
        """Parse JSON from response with error handling."""
        try:
            return await response.json()
        except Exception as json_err:
            text = await response.text()
            self.logger.error(f"BaseDebrid: Invalid JSON response: {json_err}")
            self.logger.debug(f"BaseDebrid: Response content: {text[:200]}...")
            if attempt < max_attempts - 1:
                wait_time = 2**attempt + 1
                self.logger.info(f"BaseDebrid: Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            return None

    async def wait_for_ready_status(self, check_status_func, timeout=30, interval=5):
        """Async wait for ready status with polling."""
        self.logger.info(f"BaseDebrid: Waiting for {timeout} seconds for caching.")
        start_time = time.time()
        while time.time() - start_time < timeout:
            if await check_status_func():
                self.logger.info("BaseDebrid: File is ready!")
                return True
            await asyncio.sleep(interval)
        self.logger.info(f"BaseDebrid: Waiting timed out.")
        return False

    async def download_torrent_file(self, download_url):
        """Async download of torrent file."""
        session = await self._get_session()
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.get(download_url, timeout=timeout) as response:
            response.raise_for_status()
            return await response.read()

    async def get_stream_link(self, query, ip=None):
        raise NotImplementedError

    async def add_magnet_or_torrent(self, magnet, torrent_download=None, ip=None):
        raise NotImplementedError

    async def add_magnet(self, magnet, ip=None):
        raise NotImplementedError

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        raise NotImplementedError

    # ---------------------------------------------------------------------------
    # Generic Redis availability cache (issue #77)
    # Results are shared across all users of the instance. Private data (tokenized
    # links, user-specific URLs) is stripped by _sanitize_for_cache() before storage.
    # ---------------------------------------------------------------------------

    # TTLs — Redis L1
    _AVAIL_TTL_CACHED     = 10 * 24 * 3600  # 10 days (debrid caches are stable long-term)
    _AVAIL_TTL_NOT_CACHED = 20 * 60          # 20 minutes (re-check frequently for not-cached)
    # TTL — PostgreSQL L2 (persistent, survives Redis restarts)
    _PG_TTL_CACHED        = 30 * 24 * 3600  # 30 days

    @property
    def service_name(self) -> str:
        """Unique provider identifier used as a Redis cache key segment. Override per provider."""
        return self.__class__.__name__.lower()

    def _avail_cache_key(self, h: str) -> str:
        return f"debrid_avail:{self.service_name}:{h}"

    def _extract_hash(self, value: str):
        """Extract a 40-char hex info-hash from a magnet URI or raw hash string."""
        if not value:
            return None
        value = str(value).strip().lower()
        if value.startswith("magnet:?"):
            m = re.search(r"btih:([0-9a-f]{40})", value, re.IGNORECASE)
            return m.group(1).lower() if m else None
        if len(value) == 40 and all(c in "0123456789abcdef" for c in value):
            return value
        return None

    def _sanitize_for_cache(self, item: dict) -> dict:
        """
        Strip private/user-specific fields before writing to the shared Redis cache.
        Fields always removed: link, url, stream_link (may contain tokenized private URLs).
        Override per provider for a stricter field whitelist.
        """
        return {k: v for k, v in item.items() if k not in ("link", "url", "stream_link")}

    def _index_results_by_hash(self, response) -> dict:
        """
        Build a {hash: item} mapping from a get_availability_bulk() response.
        Only available (cached) hashes should be included.
        Must be overridden per provider — each has its own response format.
        """
        raise NotImplementedError

    def _reconstruct_response(self, items: list):
        """
        Rebuild a get_availability_bulk()-compatible response from a list of sanitized items.
        Must be overridden per provider to match the format expected by TorrentSmartContainer.
        """
        raise NotImplementedError

    async def get_availability_bulk_cached(
        self, hashes_or_magnets, ip, redis_client, db_session=None
    ):
        """
        Two-level caching wrapper around get_availability_bulk() — shared across all users.

        Flow:
          1. L1 — Batch Redis MGET for all hashes.
          2. L2 — For Redis misses: query PostgreSQL (only confirmed-cached, non-expired rows).
                  Warm Redis for PG hits so future requests stay fast.
          3. Live API call for remaining misses.
          4. Write results:
               Cached     → Redis (10d) + PostgreSQL (30d)
               Not-cached → Redis sentinel only (20 min), never written to PostgreSQL.
          5. Reconstruct a provider-compatible response from all hits.

        Security: _sanitize_for_cache() ensures no private token or link leaks into
        the shared caches (e.g. StremThru tokenized links are stripped).

        Graceful degradation: if redis_client or db_session is None the corresponding
        layer is skipped transparently.
        """
        hashes = [h for h in (self._extract_hash(x) for x in hashes_or_magnets) if h]
        if not hashes:
            return self._reconstruct_response([])

        hits = []
        to_check = []

        # --- L1: Redis batch lookup ---
        if redis_client:
            try:
                raw_values = await redis_client.mget(
                    [self._avail_cache_key(h) for h in hashes]
                )
                for h, raw in zip(hashes, raw_values):
                    if raw is not None:
                        val = _jp.decode(raw)
                        if val.get("status") != "not_cached":
                            hits.append(val)
                        # not_cached sentinel → treat as unavailable, skip
                    else:
                        to_check.append(h)
                redis_hit_count = len(hashes) - len(to_check)
                if redis_hit_count:
                    logger.info(
                        f"{self.__class__.__name__}: Redis cache hit "
                        f"{redis_hit_count}/{len(hashes)} hashes"
                    )
            except Exception as e:
                logger.warning(
                    f"{self.__class__.__name__}: Redis lookup failed ({e}), skipping cache"
                )
                to_check = list(hashes)
        else:
            to_check = list(hashes)

        # --- L2: PostgreSQL lookup for Redis misses ---
        if to_check and db_session:
            try:
                from stream_fusion.services.postgresql.dao.debridcache_dao import DebridCacheDAO
                pg_hits = await DebridCacheDAO(db_session).get_batch(to_check, self.service_name)
                if pg_hits:
                    logger.debug(
                        f"{self.__class__.__name__}: PG cache hit "
                        f"{len(pg_hits)}/{len(to_check)} hashes — warming Redis"
                    )
                    # Warm Redis so next request is served from L1
                    if redis_client:
                        try:
                            pipe = redis_client.pipeline()
                            for h, data in pg_hits.items():
                                pipe.set(
                                    self._avail_cache_key(h),
                                    _jp.encode(data),
                                    ex=self._AVAIL_TTL_CACHED,
                                )
                            await pipe.execute()
                        except Exception as e:
                            logger.warning(
                                f"{self.__class__.__name__}: Redis warm-up failed ({e})"
                            )
                    hits.extend(pg_hits.values())
                    to_check = [h for h in to_check if h not in pg_hits]
            except Exception as e:
                logger.warning(
                    f"{self.__class__.__name__}: PG L2 lookup failed ({e}), continuing to API"
                )

        # --- Live API call for remaining misses ---
        if to_check:
            api_response = await self.get_availability_bulk(to_check, ip)
            by_hash = self._index_results_by_hash(api_response)

            # Write to Redis
            if redis_client:
                try:
                    pipe = redis_client.pipeline()
                    for h in to_check:
                        if h in by_hash:
                            safe = self._sanitize_for_cache(by_hash[h])
                            pipe.set(
                                self._avail_cache_key(h),
                                _jp.encode(safe),
                                ex=self._AVAIL_TTL_CACHED,
                            )
                            hits.append(safe)
                        else:
                            # Not cached: write short-lived sentinel, never write to PG
                            pipe.set(
                                self._avail_cache_key(h),
                                _jp.encode({"status": "not_cached"}),
                                ex=self._AVAIL_TTL_NOT_CACHED,
                            )
                    await pipe.execute()
                except Exception as e:
                    logger.warning(
                        f"{self.__class__.__name__}: Redis write failed ({e})"
                    )
                    hits.extend(by_hash.values())
            else:
                hits.extend(by_hash.values())

            # Write confirmed-cached results to PostgreSQL (not-cached never stored in PG)
            if db_session and by_hash:
                try:
                    from stream_fusion.services.postgresql.dao.debridcache_dao import DebridCacheDAO
                    cached_entries = [self._sanitize_for_cache(v) for v in by_hash.values()]
                    await DebridCacheDAO(db_session).upsert_batch(
                        cached_entries, self.service_name, self._PG_TTL_CACHED
                    )
                except Exception as e:
                    logger.warning(
                        f"{self.__class__.__name__}: PG write failed ({e})"
                    )

        return self._reconstruct_response(hits)

    async def invalidate_availability_cache(
        self, info_hash: str, redis_client, db_session=None
    ) -> None:
        """Invalidate both Redis and PostgreSQL cache entries for a hash.

        Call this when get_stream_link() returns no_cache_video_url, confirming that
        a previously cached hash is a false positive (stale data).
        The next search for the same hash will re-query the debrid API.
        """
        h = self._extract_hash(info_hash) or info_hash.lower()
        # Remove from Redis L1
        if redis_client:
            try:
                await redis_client.delete(self._avail_cache_key(h))
            except Exception as e:
                logger.warning(
                    f"{self.__class__.__name__}: Redis invalidate failed for {h} ({e})"
                )
        # Remove from PostgreSQL L2
        if db_session:
            try:
                from stream_fusion.services.postgresql.dao.debridcache_dao import DebridCacheDAO
                await DebridCacheDAO(db_session).invalidate(h, self.service_name)
            except Exception as e:
                logger.warning(
                    f"{self.__class__.__name__}: PG invalidate failed for {h} ({e})"
                )
        logger.debug(
            f"{self.__class__.__name__}: Cache invalidated for {h} — false positive confirmed"
        )

    async def _get_stremthru_community_cache(
        self,
        hashes: list,
        store_name: str,
        token: str,
        debrid_code_filter: str | None = None,
        ip: str | None = None,
    ) -> tuple:
        """
        Query StremThru as optional community cache.

        No local Redis — intentional: StremThru manages its own external cache.
        Redis caching is handled by the caller (RD/AD) via get_availability_bulk_cached().

        Args:
            hashes: list of 40-char hex hashes
            store_name: StremThru store name (e.g. "realdebrid")
            token: store authentication token
            debrid_code_filter: if set, filter results by this code (e.g. "AD")
            ip: optional client IP

        Returns:
            (cached_items: list[dict], remaining_hashes: list[str])
            Never raises exceptions.
        """
        if not getattr(settings, "stremthru_url", None):
            return [], hashes

        try:
            from stream_fusion.utils.stremthru.client import StremThruClient
            session = await self._get_session()
            client = StremThruClient(settings.stremthru_url, store_name, token, session)
            results = await client.check_availability(hashes, ip)

            if debrid_code_filter:
                results = [r for r in results if r.get("debrid") == debrid_code_filter]

            cached_hashes = {r["hash"] for r in results}
            remaining = [h for h in hashes if h not in cached_hashes]
            return results, remaining

        except Exception as e:
            logger.warning(f"{self.__class__.__name__}: StremThru community cache unavailable ({e})")
            return [], hashes
