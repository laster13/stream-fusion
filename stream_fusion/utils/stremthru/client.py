"""
StremThruClient — High-level wrapper around the vendored StremThru SDK.

Responsibilities:
- Normalizes SDK responses to the stream-fusion internal format
- Parallel batch requests via asyncio.gather() for availability checks
- Shared aiohttp session with BaseDebrid (zero redundant connections)
- No Redis: caching is handled by StremThruDebrid (inherits BaseDebrid)
"""
import asyncio
import io
import re

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.utils.stremthru.sdk.client import StremThruSDK
from stream_fusion.utils.stremthru.sdk.error import StremThruError

# Mapping from store_name to 2-letter debrid code (consistent with get_underlying_debrid_code)
DEBRID_CODE_MAP = {
    "realdebrid": "RD",
    "alldebrid": "AD",
    "torbox": "TB",
    "premiumize": "PM",
    "debridlink": "DL",
    "easydebrid": "ED",
    "offcloud": "OC",
    "pikpak": "PK",
}


class StremThruClient:
    """
    stream-fusion adapter around the vendored StremThru SDK.

    Usage:
        client = StremThruClient(base_url, store_name, token, session)
        results = await client.check_availability(hashes)
        link = await client.generate_link(file_link)
    """

    def __init__(
        self,
        base_url: str,
        store_name: str,
        token: str,
        session: aiohttp.ClientSession | None = None,
    ):
        self._sdk = StremThruSDK(
            base_url=base_url,
            auth={"store": store_name, "token": token},
            user_agent="stream-fusion",
            timeout=10,
        )
        self._session = session
        self.store_name = store_name
        self.debrid_code = DEBRID_CODE_MAP.get(store_name, store_name[:2].upper())

    # ------------------------------------------------------------------
    # Availability
    # ------------------------------------------------------------------

    async def check_availability(self, hashes: list[str], ip: str | None = None) -> list[dict]:
        """
        Check cache availability for a list of hashes.
        Chunks of 50 are sent in parallel via asyncio.gather().

        Returns only items with status="cached", in normalized format:
            [{"hash", "status": "cached", "files": [{name, index, size}], "store_name", "debrid"}]
        """
        if not hashes:
            return []

        magnets = [f"magnet:?xt=urn:btih:{h}" for h in hashes]
        chunks = [magnets[i:i + 50] for i in range(0, len(magnets), 50)]

        tasks = [
            self._sdk.store.check_magnet(chunk, session=self._session)
            for chunk in chunks
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for resp in responses:
            if isinstance(resp, Exception):
                logger.warning(f"StremThru: chunk check error on {self.store_name}: {resp}")
                continue
            for item in (resp.data or {}).get("items", []):
                if item.get("status") == "cached":
                    results.append(self._normalize_availability_item(item))

        logger.debug(
            f"StremThru: check_availability {self.store_name} — "
            f"{len(results)} cached / {len(hashes)} requested"
        )
        return results

    def _normalize_availability_item(self, item: dict) -> dict:
        """Convert an SDK item to the stream-fusion internal format (without tokenized links)."""
        raw_hash = item.get("hash", "")
        normalized_hash = self._normalize_hash(raw_hash) or raw_hash.lower()
        return {
            "hash": normalized_hash,
            "status": "cached",
            "files": [
                {
                    "name": f.get("name", ""),
                    "index": f.get("index"),
                    "size": f.get("size", 0),
                }
                for f in item.get("files", [])
            ],
            "store_name": self.store_name,
            "debrid": self.debrid_code,
        }

    # ------------------------------------------------------------------
    # Magnet operations
    # ------------------------------------------------------------------

    async def add_magnet(
        self,
        magnet: str,
        torrent_bytes: bytes | None = None,
        ip: str | None = None,
    ) -> dict | None:
        """
        Add a magnet or .torrent file to the store.
        Returns the raw SDK data dict (id, files, hash, status, ...).
        """
        try:
            if torrent_bytes:
                resp = await self._sdk.store.add_magnet(
                    torrent=io.BytesIO(torrent_bytes),
                    client_ip=ip,
                    session=self._session,
                )
            else:
                normalized = self._to_magnet(magnet)
                if not normalized:
                    logger.error(f"StremThru: invalid magnet for add_magnet: {str(magnet)[:80]}")
                    return None
                resp = await self._sdk.store.add_magnet(
                    magnet=normalized,
                    client_ip=ip,
                    session=self._session,
                )
            return dict(resp.data) if resp.data else None
        except StremThruError as e:
            logger.warning(f"StremThru: add_magnet error on {self.store_name}: {e}")
            return None

    async def get_magnet_info(self, magnet_id: str) -> dict | None:
        """Retrieve information for a previously added magnet (id, files, status, ...)."""
        try:
            resp = await self._sdk.store.get_magnet(magnet_id, session=self._session)
            return dict(resp.data) if resp.data else None
        except StremThruError as e:
            logger.warning(f"StremThru: get_magnet_info error on {self.store_name}: {e}")
            return None

    # ------------------------------------------------------------------
    # Link generation
    # ------------------------------------------------------------------

    async def generate_link(self, link: str, ip: str | None = None) -> str | None:
        """Generate a final streaming URL from a StremThru tokenized link."""
        try:
            resp = await self._sdk.store.generate_link(link, client_ip=ip, session=self._session)
            return (resp.data or {}).get("link")
        except StremThruError as e:
            logger.warning(f"StremThru: generate_link error on {self.store_name}: {e}")
            return None

    # ------------------------------------------------------------------
    # Account
    # ------------------------------------------------------------------

    async def check_premium(self, ip: str | None = None) -> bool:
        """Check whether the store account subscription is premium."""
        try:
            resp = await self._sdk.store.get_user(session=self._session)
            return (resp.data or {}).get("subscription_status") == "premium"
        except StremThruError:
            return False

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _normalize_hash(self, value: str | None) -> str | None:
        """Normalize a hash or magnet URI to a 40-char lowercase hex string."""
        if not value:
            return None
        value = str(value).strip()
        if value.startswith("magnet:?"):
            match = re.search(r"btih:([a-fA-F0-9]{40})", value, re.IGNORECASE)
            return match.group(1).lower() if match else None
        value = value.lower()
        if len(value) == 40 and all(c in "0123456789abcdef" for c in value):
            return value
        return None

    def _to_magnet(self, value: str | None) -> str | None:
        """Convert a hash or magnet URI to a canonical magnet URL."""
        if not value:
            return None
        if isinstance(value, str) and value.startswith("magnet:?"):
            return value
        normalized = self._normalize_hash(value)
        return f"magnet:?xt=urn:btih:{normalized}" if normalized else None
