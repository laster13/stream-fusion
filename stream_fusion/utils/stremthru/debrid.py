"""
StremThruDebrid — BaseDebrid implementation using StremThru as backend.

StremThru is a proxy/middleware that unifies multiple debrid services (RD, AD, TB, PM, DL, ED, OC, PP)
behind a common API. This class inherits from BaseDebrid to benefit from Redis caching,
rate limiting, and session management.

HTTP operations are delegated to StremThruClient which uses the vendored SDK.
"""
import re
from urllib.parse import unquote

import aiohttp

from stream_fusion.logging_config import logger
from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.debrid.debrid_exceptions import DebridError
from stream_fusion.settings import settings
from stream_fusion.utils.general import (
    season_episode_in_filename,
    smart_episode_fallback,
    is_video_file,
)
from stream_fusion.utils.stremthru.client import StremThruClient


class StremThruDebrid(BaseDebrid):
    """
    Debrid implementation using StremThru as proxy to any supported store.

    Inherits from BaseDebrid:
    - Shared Redis cache (TTL 3d/10min) via get_availability_bulk_cached()
    - Rate limiting (250 req/60s global, 1 req/1s torrents)
    - aiohttp session management with proxy support
    - Retry with exponential backoff
    """

    def __init__(self, config, session: aiohttp.ClientSession = None):
        super().__init__(config, session)
        self.config = config
        self.stremthru_url = settings.stremthru_url or "https://stremthru.13377001.xyz"
        self.store_name: str | None = None
        self.token: str | None = None
        self._client: StremThruClient | None = None

        if not self.store_name:
            self.auto_detect_store()

    # ------------------------------------------------------------------
    # BaseDebrid cache interface
    # ------------------------------------------------------------------

    @property
    def service_name(self) -> str:
        """Unique identifier for Redis keys — includes store name to avoid collisions."""
        return f"stremthru_{self.store_name or 'generic'}"

    def _index_results_by_hash(self, response) -> dict:
        """StremThru returns a list of {"hash": ..., "status": "cached", ...}"""
        if not isinstance(response, list):
            return {}
        return {item["hash"]: item for item in response if item.get("hash")}

    def _reconstruct_response(self, items: list):
        """StremThru providers consume a flat list."""
        return items

    def _sanitize_for_cache(self, item: dict) -> dict:
        """
        Strict whitelist before writing to shared Redis cache.
        Strips user-specific tokenized links from nested file objects.
        Only hash, status, store_name, debrid and files (without link) are kept.
        """
        sanitized = {
            k: v for k, v in item.items()
            if k in ("hash", "status", "store_name", "debrid")
        }
        if "files" in item:
            sanitized["files"] = [
                {
                    "name": f.get("name", ""),
                    "index": f.get("index"),
                    "size": f.get("size", 0),
                }
                for f in (item.get("files") or [])
                if isinstance(f, dict)
            ]
        return sanitized

    # ------------------------------------------------------------------
    # Store configuration
    # ------------------------------------------------------------------

    def auto_detect_store(self):
        """Auto-detect which store to use based on available tokens."""
        priority_order = [
            ("realdebrid", "RDToken"),
            ("premiumize", "PMToken"),
            ("torbox", "TBToken"),
            ("alldebrid", "ADToken"),
            ("debridlink", "DLToken"),
            ("easydebrid", "EDToken"),
            ("offcloud", "OCCredentials"),
            ("pikpak", "PPCredentials"),
        ]

        for store_name, token_key in priority_order:
            token = self.config.get(token_key)
            if token:
                token_str = token if isinstance(token, str) else str(token)
                if len(token_str.strip()) > 5:
                    logger.info(
                        f"StremThru: auto-detected {store_name} via {token_key}"
                    )
                    self.set_store_credentials(store_name, token_str)
                    return

        logger.warning("StremThru: no store auto-detected")

    def set_store_credentials(self, store_name: str, token: str):
        """Configure the store and create the corresponding StremThruClient."""
        self.store_name = store_name
        self.token = token
        self._client = StremThruClient(
            base_url=self.stremthru_url,
            store_name=store_name,
            token=token,
            session=self._session,
        )

    # ------------------------------------------------------------------
    # BaseDebrid abstract methods
    # ------------------------------------------------------------------

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """Check torrent availability via StremThru."""
        if not hashes_or_magnets:
            return []

        if not self.store_name:
            self.auto_detect_store()

        if not self._client:
            logger.warning("StremThru: no store configured for get_availability_bulk")
            return []

        hashes = [self._normalize_hash(h) for h in hashes_or_magnets]
        hashes = [h for h in hashes if h]

        if not hashes:
            logger.warning("StremThru: no valid hashes to check")
            return []

        return await self._client.check_availability(hashes, ip)

    async def add_magnet(self, magnet, ip=None, torrent_file_content=None):
        """Add a magnet or .torrent file to StremThru."""
        if not self.store_name:
            self.auto_detect_store()

        if not self._client:
            logger.error("StremThru: no store configured for add_magnet")
            return None

        try:
            result = await self._client.add_magnet(magnet, torrent_file_content, ip)
            if result is None:
                logger.error(f"StremThru: add_magnet failed on {self.store_name}")
            return result
        except DebridError:
            raise
        except Exception as e:
            logger.warning(f"StremThru: add_magnet error on {self.store_name}: {e}")
            return None

    async def get_stream_link(self, query, config=None, ip=None):
        """Generate a streaming link from a query."""
        try:
            logger.debug(f"StremThru: generating stream link for {query}")

            if not self.store_name:
                self.auto_detect_store()

            if not self._client:
                logger.error("StremThru: no store configured for get_stream_link")
                return None

            stream_type = query.get("type")
            if not stream_type:
                logger.error("StremThru: missing media type in query")
                return None

            season = query.get("season")
            episode = query.get("episode")
            magnet_url = query.get("magnet")
            info_hash = query.get("infoHash")
            file_idx = query.get("file_index", query.get("fileIdx"))

            if magnet_url and not info_hash:
                info_hash = self._normalize_hash(magnet_url)

            if not info_hash:
                logger.error("StremThru: no hash in query")
                return None

            info_hash = self._normalize_hash(info_hash)
            if not info_hash:
                logger.error("StremThru: invalid hash in query")
                return None

            magnet = magnet_url or f"magnet:?xt=urn:btih:{info_hash}"

            magnet_data = None
            magnet_info = None

            # Try to use availability cache (with playable links)
            cached_files = await self.get_availability_bulk([info_hash], ip)
            if cached_files:
                files = cached_files[0].get("files", [])
                has_playable_links = any(
                    isinstance(f, dict) and f.get("link") for f in files
                )
                if files and has_playable_links:
                    logger.info(
                        f"StremThru: {info_hash} cached with playable links"
                    )
                    magnet_data = {
                        "files": files,
                        "id": info_hash,
                        "status": "cached",
                    }
                else:
                    logger.info(
                        f"StremThru: {info_hash} cached but no links, fetching full data"
                    )

            if not magnet_data:
                logger.debug(f"StremThru: adding magnet {magnet} via {self.store_name}")
                magnet_info = await self.add_magnet(magnet, ip)

                if not magnet_info:
                    logger.error(f"StremThru: could not add magnet {info_hash}")
                    return None

                magnet_data = magnet_info

                if "files" not in magnet_data:
                    magnet_id = magnet_info.get("id")
                    if magnet_id:
                        logger.debug(
                            f"StremThru: fetching magnet info {magnet_id}"
                        )
                        magnet_data = await self.get_magnet_info(magnet_info, ip)

                    if not magnet_data:
                        logger.error("StremThru: could not retrieve magnet info")
                        return None

            if "files" not in magnet_data:
                logger.error(
                    f"StremThru: no files in magnet {magnet_data.get('id', '?')}"
                )
                return None

            files = magnet_data.get("files", [])
            if not isinstance(files, list) or not files:
                logger.error("StremThru: no usable files in torrent")
                return None

            normalized_files = [
                {
                    "index": f.get("index"),
                    "name": f.get("name", ""),
                    "size": f.get("size", 0),
                    "link": f.get("link"),
                }
                for f in files
                if isinstance(f, dict)
            ]

            if not normalized_files:
                logger.error("StremThru: no valid files after normalization")
                return None

            target_file = None

            # Series file selection
            if stream_type == "series" and season and episode:
                try:
                    numeric_season = int(str(season).replace("S", ""))
                    numeric_episode = int(str(episode).replace("E", ""))

                    for file in normalized_files:
                        file_name = file.get("name", "")
                        if not is_video_file(file_name):
                            continue
                        if season_episode_in_filename(file_name, numeric_season, numeric_episode):
                            target_file = file
                            logger.info(
                                f"StremThru: file found by NAME: {file_name} (index: {file.get('index')})"
                            )
                            break
                except Exception as e:
                    logger.warning(f"StremThru: name-based search error: {e}")

            # File selection by index
            if target_file is None and file_idx is not None:
                try:
                    idx = int(file_idx)
                    target_file = next(
                        (
                            f for f in normalized_files
                            if f.get("index") is not None and int(f.get("index")) == idx
                        ),
                        None,
                    )
                except (TypeError, ValueError):
                    target_file = None

                if target_file:
                    logger.info(
                        f"StremThru: file found by INDEX {file_idx}: {target_file.get('name')}"
                    )
                    if stream_type == "movie" and self._is_text_or_subtitle_file(
                        target_file.get("name", "")
                    ):
                        logger.warning(
                            f"StremThru: file at index {file_idx} is not a video: {target_file.get('name')}"
                        )
                        target_file = None

            # Smart fallback for series
            if target_file is None and stream_type == "series" and season and episode:
                try:
                    video_files = [
                        f for f in normalized_files if is_video_file(f.get("name", ""))
                    ]
                    if video_files:
                        numeric_season = int(str(season).replace("S", ""))
                        numeric_episode = int(str(episode).replace("E", ""))
                        fallback_file = smart_episode_fallback(
                            video_files, numeric_season, numeric_episode
                        )
                        if fallback_file:
                            target_file = fallback_file
                            logger.info(
                                f"StremThru: file found by FALLBACK: {target_file.get('name')} (index: {target_file.get('index')})"
                            )
                except Exception as e:
                    logger.warning(f"StremThru: smart fallback error: {e}")

            # Final fallback: largest video file
            if target_file is None:
                target_file = self._select_best_video_file(normalized_files)
                if target_file:
                    logger.info(
                        f"StremThru: best file selected: {target_file.get('name')} (index: {target_file.get('index')})"
                    )
                else:
                    logger.error("StremThru: no video file found")
                    return None

            if not target_file or not target_file.get("link"):
                logger.error("StremThru: target file missing or has no link")
                return None

            torrent_id = (
                (magnet_info or {}).get("id")
                or magnet_data.get("id", "")
                or info_hash
            )
            file_id = target_file.get("index", "")

            if stream_type == "series" and season and episode:
                logger.info(
                    f"StremThru: final selection {season}{episode} in {torrent_id}, "
                    f"file: {target_file.get('name')} (index: {file_id})"
                )
            else:
                logger.info(
                    f"StremThru: final selection {target_file.get('name')} (index: {file_id}) in {torrent_id}"
                )

            # TorBox special case: bypass CDN via direct requestdl
            if (
                self.store_name == "torbox"
                and isinstance(target_file.get("link"), str)
                and target_file["link"].startswith("stremthru://store/torbox/")
            ):
                stream_link = await self._torbox_direct_link(target_file["link"], ip)
                if stream_link:
                    return stream_link
                logger.warning("StremThru-TorBox: requestdl failed, falling back to StremThru")

            # Generate link via StremThru
            stream_link = await self._client.generate_link(target_file["link"], ip)
            if stream_link:
                logger.info(f"StremThru: link generated: {stream_link}")
                return stream_link

            logger.error("StremThru: stream link generation failed")
            return None

        except DebridError:
            raise
        except Exception as e:
            logger.warning(
                f"StremThru: get_stream_link error on {self.store_name}: {e}"
            )
            return None

    async def get_magnet_info(self, magnet_info, ip=None):
        """Retrieve magnet information (with fallback to existing data)."""
        if isinstance(magnet_info, dict):
            if "files" in magnet_info and "id" in magnet_info:
                logger.debug(
                    f"StremThru: using already-available magnet info for {magnet_info.get('id')}"
                )
                return magnet_info
            magnet_id = magnet_info.get("id")
            if not magnet_id:
                logger.error("StremThru: no magnet ID in provided info")
                return None
        else:
            magnet_id = magnet_info

        result = await self._client.get_magnet_info(str(magnet_id))

        if result is None and isinstance(magnet_info, dict) and "files" in magnet_info:
            logger.debug("StremThru: falling back to already-available files")
            return magnet_info

        return result

    async def check_premium(self, ip=None):
        """Check whether the store subscription is active."""
        if not self._client:
            return False
        return await self._client.check_premium(ip)

    async def start_background_caching(self, magnet, query=None):
        """Start downloading a magnet in the background."""
        logger.info(
            f"StremThru: starting background download via {self.store_name}"
        )

        if not self.store_name:
            self.auto_detect_store()

        if not self._client:
            logger.error("StremThru: no store configured for start_background_caching")
            return False

        try:
            torrent_file_content = None

            if self.store_name == "torbox":
                logger.info("StremThru-TorBox: using magnet only")
            elif query and query.get("torrent_download"):
                torrent_download = unquote(query["torrent_download"])

                if not torrent_download.startswith("magnet:"):
                    logger.info(
                        f"StremThru: downloading .torrent for {self.store_name}: {torrent_download[:100]}"
                    )
                    try:
                        session = await self._get_session()
                        import aiohttp as _aiohttp
                        timeout = _aiohttp.ClientTimeout(total=10)
                        async with session.get(torrent_download, timeout=timeout) as response:
                            if response.status == 200:
                                content = await response.read()
                                if content and len(content) > 0 and content[0:1] == b"d":
                                    torrent_file_content = content
                                    logger.info(
                                        f"StremThru: .torrent downloaded and validated for {self.store_name}"
                                    )
                                else:
                                    logger.warning(
                                        f"StremThru: non-torrent content for {self.store_name}, falling back to magnet"
                                    )
                            else:
                                logger.warning(
                                    f"StremThru: could not download .torrent for {self.store_name}: {response.status}"
                                )
                    except Exception as e:
                        logger.warning(
                            f"StremThru: .torrent download error for {self.store_name}: {e}, falling back to magnet"
                        )

            result = None
            if torrent_file_content:
                try:
                    result = await self.add_magnet(magnet, torrent_file_content=torrent_file_content)
                except Exception as e:
                    logger.warning(
                        f"StremThru: .torrent add failed for {self.store_name}: {e}, falling back to magnet"
                    )
                    result = None

            if not result:
                logger.info(f"StremThru: adding via magnet for {self.store_name}")
                result = await self.add_magnet(magnet, torrent_file_content=None)

            if not result:
                logger.error(f"StremThru: background download start failed via {self.store_name}")
                return False

            magnet_id = result.get("id")
            if not magnet_id:
                logger.error(f"StremThru: no ID returned by {self.store_name}")
                return False

            logger.info(
                f"StremThru: background download started via {self.store_name}, ID: {magnet_id}"
            )
            return True

        except DebridError:
            raise
        except Exception as e:
            logger.error(
                f"StremThru: start_background_caching error via {self.store_name}: {e}"
            )
            return False

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def get_underlying_debrid_code(store_name=None):
        """Return the 2-letter code of the underlying debrid service."""
        from stream_fusion.utils.stremthru.client import DEBRID_CODE_MAP
        return DEBRID_CODE_MAP.get(store_name)

    def _normalize_hash(self, value):
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

    def _to_magnet(self, value):
        """Convert a hash or magnet URI to a canonical magnet URL."""
        if not value:
            return None
        if isinstance(value, str) and value.startswith("magnet:?"):
            return value
        normalized = self._normalize_hash(value)
        return f"magnet:?xt=urn:btih:{normalized}" if normalized else None

    def _is_text_or_subtitle_file(self, file_name: str) -> bool:
        """Check if the file is a text, image, or subtitle file (non-video)."""
        if not file_name:
            return False
        return any(
            file_name.lower().endswith(ext)
            for ext in (
                ".nfo", ".txt",
                ".jpg", ".jpeg", ".png", ".gif", ".webp",
                ".srt", ".sub", ".idx", ".ass", ".ssa", ".vtt",
            )
        )

    def _select_best_video_file(self, files):
        """Select the best video file (largest) or the first available file."""
        if not isinstance(files, list):
            return None

        video_files = [
            f for f in files
            if isinstance(f, dict) and is_video_file(f.get("name", ""))
        ]

        if video_files:
            return max(video_files, key=lambda x: x.get("size", 0))

        valid = [f for f in files if isinstance(f, dict)]
        return valid[0] if valid else None

    async def _torbox_direct_link(self, link_token: str, ip=None) -> str | None:
        """
        Bypass StremThru CDN for TorBox: direct requestdl call.
        The tokenized link format is stremthru://store/torbox/{base64(torrent_id:file_id)}
        """
        try:
            import base64 as _b64
            token_part = link_token.split("stremthru://store/torbox/")[1]
            decoded = _b64.b64decode(token_part + "==").decode()
            tb_torrent_id, tb_file_id = decoded.split(":")
            tb_url = (
                "https://api.torbox.app/v1/api/torrents/requestdl"
                f"?token={self.token}&torrent_id={tb_torrent_id}"
                f"&file_id={tb_file_id}&zip_link=false"
            )
            logger.info("StremThru-TorBox: direct requestdl call (CDN bypass)")
            session = await self._get_session()
            import aiohttp as _aiohttp
            timeout = _aiohttp.ClientTimeout(total=30)
            async with session.get(tb_url, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success") and data.get("data"):
                        stream_link = data["data"]
                        logger.info(f"StremThru-TorBox: direct link generated: {stream_link}")
                        return stream_link
        except Exception as e:
            logger.warning(f"StremThru-TorBox: requestdl error: {e}")
        return None
