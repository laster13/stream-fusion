import asyncio
import aiohttp
from typing import Optional

from stream_fusion.settings import settings
from stream_fusion.logging_config import logger


class AsyncRateLimiter:
    """Rate limiter async pour contrôler le débit des requêtes."""

    def __init__(self, calls_per_second: float = 1.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Attend le temps nécessaire avant d'autoriser une nouvelle requête."""
        async with self._lock:
            import time
            now = time.time()
            time_since_last_call = now - self.last_call
            if time_since_last_call < self.min_interval:
                await asyncio.sleep(self.min_interval - time_since_last_call)
            self.last_call = time.time()


class SharewoodAPI:
    def __init__(
        self,
        sharewood_passkey: str,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: int = 10,
    ):
        self.base_url = f"{settings.sharewood_url}/api"
        if not sharewood_passkey or len(sharewood_passkey) != 32:
            raise ValueError("Sharewood passkey must be 32 characters long")
        self.sharewood_passkey = sharewood_passkey
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.rate_limiter = AsyncRateLimiter(calls_per_second=1)

        self._external_session = session is not None
        self._session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        """Retourne la session aiohttp, en crée une si nécessaire."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
            self._external_session = False
        return self._session

    async def close(self):
        """Ferme la session si elle a été créée en interne."""
        if self._session and not self._external_session and not self._session.closed:
            await self._session.close()

    async def _make_request(self, method: str, endpoint: str, params: dict = None):
        """Effectue une requête HTTP async avec rate limiting."""
        await self.rate_limiter.acquire()

        url = f"{self.base_url}/{self.sharewood_passkey}/{endpoint}"
        session = await self._get_session()

        try:
            async with session.request(method, url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"An error occurred during the request: {e}")
            raise

    async def get_last_torrents(self, category: int = None, subcategory: int = None, limit: int = 25):
        """Get the last torrents, optionally filtered by category or subcategory."""
        params = {}
        if category:
            params["category"] = category
        if subcategory:
            params["subcategory"] = subcategory
        if limit and 1 <= limit <= 25:
            params["limit"] = limit
        return await self._make_request("GET", "last-torrents", params=params)

    async def search(self, query: str, category: int = None, subcategory: int = None):
        """Search for torrents, optionally filtered by category or subcategory."""
        params = {"name": query}
        if category:
            params["category"] = category
        if subcategory:
            params["subcategory"] = subcategory
        return await self._make_request("GET", "search", params=params)

    async def get_video_torrents(self, limit: int = 25):
        """Get the last video torrents."""
        return await self.get_last_torrents(category=1, limit=limit)

    async def get_audio_torrents(self, limit: int = 25):
        """Get the last audio torrents."""
        return await self.get_last_torrents(category=2, limit=limit)

    async def get_application_torrents(self, limit: int = 25):
        """Get the last application torrents."""
        return await self.get_last_torrents(category=3, limit=limit)

    async def get_ebook_torrents(self, limit: int = 25):
        """Get the last ebook torrents."""
        return await self.get_last_torrents(category=4, limit=limit)

    async def get_game_torrents(self, limit: int = 25):
        """Get the last game torrents."""
        return await self.get_last_torrents(category=5, limit=limit)

    async def get_training_torrents(self, limit: int = 25):
        """Get the last training torrents."""
        return await self.get_last_torrents(category=6, limit=limit)

    async def get_adult_torrents(self, limit: int = 25):
        """Get the last adult torrents."""
        return await self.get_last_torrents(category=7, limit=limit)

    async def download_torrent(self, torrent_id: int) -> bytes:
        """Download a specific torrent file."""
        await self.rate_limiter.acquire()

        url = f"{self.base_url}/{self.sharewood_passkey}/{torrent_id}/download"
        session = await self._get_session()

        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.read()
        except aiohttp.ClientError as e:
            logger.error(f"An error occurred while downloading the torrent: {e}")
            raise
