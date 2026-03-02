from collections import deque
import asyncio
import time

import aiohttp
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
