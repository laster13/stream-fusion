import re
import uuid
import asyncio
import aiohttp
from urllib.parse import unquote

from fastapi import HTTPException
from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.general import get_info_hash_from_magnet, season_episode_in_filename, is_video_file
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


class Torbox(BaseDebrid):
    def __init__(self, config, session: aiohttp.ClientSession = None):
        super().__init__(config, session)
        self.base_url = f"{settings.tb_base_url}/{settings.tb_api_version}/api"
        self.token = settings.tb_token if settings.tb_unique_account else self.config["TBToken"]
        logger.info(f"Torbox: Initialized with base URL: {self.base_url}")

    def get_headers(self):
        if settings.tb_unique_account:
            if not settings.proxied_link:
                logger.warning("TorBox: Unique account enabled, but proxied link is disabled. This may lead to account ban.")
                logger.warning("TorBox: Please enable proxied link in the settings.")
                raise HTTPException(status_code=500, detail="Proxied link is disabled.")
            if settings.tb_token:
                return {"Authorization": f"Bearer {settings.tb_token}"}
            else:
                logger.warning("TorBox: Unique account enabled, but no token provided. Please provide a token in the env.")
                raise HTTPException(status_code=500, detail="TorBox token is not provided.")
        else:
            return {"Authorization": f"Bearer {self.config['TBToken']}"}

    def _normalize_hash_value(self, value):
        if not value:
            return None
        value = str(value).strip().lower()
        if value.startswith("magnet:?"):
            match = re.search(r"btih:([a-f0-9]{40})", value, re.IGNORECASE)
            if match:
                return match.group(1).lower()
        if len(value) == 40 and all(c in "0123456789abcdef" for c in value):
            return value
        return None

    def _build_result_item(self, hash_value, instant=False, files=None, source="torbox"):
        return {
            "hash": hash_value,
            "instant": bool(instant),
            "files": files if isinstance(files, list) else [],
            "source": source,
        }

    async def add_magnet(self, magnet, ip=None, privacy="private"):
        logger.info(f"Torbox: Adding magnet: {magnet[:50]}...")
        url = f"{self.base_url}/torrents/createtorrent"
        data = {
            "magnet": magnet,
            "seed": 3,
            "allow_zip": "false"
        }
        response = await self.json_response(url, method='post', headers=self.get_headers(), data=data, retry_on_429=False)
        logger.info(f"Torbox: Add magnet response: {response}")
        return response

    async def add_torrent(self, torrent_file, privacy="private"):
        logger.info("Torbox: Adding torrent file")
        url = f"{self.base_url}/torrents/createtorrent"
        data = {
            "seed": 3,
            "allow_zip": "false"
        }
        files = {
            "file": (str(uuid.uuid4()) + ".torrent", torrent_file, 'application/x-bittorrent')
        }
        response = await self.json_response(url, method='post', headers=self.get_headers(), data=data, files=files, retry_on_429=False)
        logger.info(f"Torbox: Add torrent file response: {response}")
        return response

    async def get_torrent_info(self, torrent_id):
        logger.info(f"Torbox: Getting info for torrent ID: {torrent_id}")
        url = f"{self.base_url}/torrents/mylist?bypass_cache=true&id={torrent_id}"
        response = await self.json_response(url, headers=self.get_headers())
        logger.debug(f"Torbox: Torrent info response: {response}")
        return response

    async def control_torrent(self, torrent_id, operation):
        """Control a torrent (delete, pause, resume, reannounce). Uses JSON body per API spec."""
        logger.info(f"Torbox: Controlling torrent ID: {torrent_id}, operation: {operation}")
        url = f"{self.base_url}/torrents/controltorrent"
        json_data = {
            "torrent_id": torrent_id,
            "operation": operation
        }
        response = await self.json_response(url, method='post', headers=self.get_headers(), json_data=json_data)
        logger.info(f"Torbox: Control torrent response: {response}")
        return response

    async def delete_torrent(self, torrent_id):
        logger.info(f"Torbox: Deleting torrent ID: {torrent_id}")
        return await self.control_torrent(torrent_id, "delete")

    async def _delete_torrents_background(self, torrent_ids):
        """Non-blocking background cleanup of temporary torrents."""
        logger.debug(f"Torbox: Background cleanup of {len(torrent_ids)} torrent(s)")
        success = 0
        for torrent_id in torrent_ids:
            try:
                await self.delete_torrent(torrent_id)
                success += 1
                await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"Torbox: Could not delete torrent {torrent_id}: {e}")
        logger.debug(f"Torbox: Background cleanup done — {success}/{len(torrent_ids)} deleted")

    async def request_download_link(self, torrent_id, file_id=None, zip_link=False, ip=None):
        """Request download link with retry logic. Passes user_ip for CDN region selection."""
        logger.info(f"Torbox: Requesting download link for torrent ID: {torrent_id}, file ID: {file_id}, zip link: {zip_link}")
        url = f"{self.base_url}/torrents/requestdl?token={self.token}&torrent_id={torrent_id}&file_id={file_id}&zip_link={str(zip_link).lower()}"
        if ip:
            url += f"&user_ip={ip}"
        logger.info(f"Torbox: Requesting URL: {url}")

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = await self.json_response(url, headers=self.get_headers())
                logger.info(f"Torbox: Request download link response: {response}")
                return response
            except (HTTPException, asyncio.TimeoutError) as e:
                if attempt < max_attempts - 1:
                    logger.warning(f"Torbox: Retry {attempt + 1}/{max_attempts} for download link request")
                    await asyncio.sleep(2)
                else:
                    raise
        return None

    async def get_stream_link(self, query, config=None, ip=None):
        magnet = query['magnet']
        stream_type = query['type']
        file_index = int(query['file_index']) if query['file_index'] is not None else None
        season = query['season']
        episode = query['episode']
        torrent_download = query["torrent_download"]
        torrent_file_content = query.get("torrent_file_content", None)
        if torrent_download:
            torrent_download = unquote(torrent_download)

        info_hash = get_info_hash_from_magnet(magnet)
        logger.info(f"Torbox: Getting stream link for {stream_type} with hash: {info_hash}")

        # Check if the torrent is already added
        existing_torrent = await self._find_existing_torrent(info_hash)

        if existing_torrent:
            logger.info(f"Torbox: Found existing torrent with ID: {existing_torrent['id']}")
            torrent_id = existing_torrent["id"]
            torrent_response = await self.get_torrent_info(torrent_id)
            if not torrent_response or "data" not in torrent_response:
                logger.error("Torbox: Failed to get torrent info.")
                return None
            torrent_info = torrent_response["data"]
        else:
            add_response = await self.add_magnet_or_torrent(magnet, torrent_download, torrent_file_content)
            if not add_response or "torrent_id" not in add_response:
                logger.error("Torbox: Failed to add or find torrent.")
                return None
            torrent_id = add_response["torrent_id"]
            torrent_response = await self.get_torrent_info(torrent_id)
            if not torrent_response or "data" not in torrent_response:
                logger.error("Torbox: Failed to get torrent info.")
                return None
            torrent_info = torrent_response["data"]

        logger.info(f"Torbox: Working with torrent ID: {torrent_id}")

        # Wait for the torrent to be ready
        if not await self._wait_for_torrent_completion(torrent_id):
            logger.warning("Torbox: Torrent not ready, caching in progress.")
            return settings.no_cache_video_url

        # Select the appropriate file
        file_id = self._select_file(torrent_info, stream_type, file_index, season, episode)

        if file_id is None:
            logger.error("Torbox: No matching file found.")
            return settings.no_cache_video_url

        # Request the download link
        download_link_response = await self.request_download_link(torrent_id, file_id, ip=ip)

        if not download_link_response or "data" not in download_link_response:
            logger.error("Torbox: Failed to get download link.")
            return settings.no_cache_video_url

        logger.info(f"Torbox: Got download link: {download_link_response['data']}")
        return download_link_response['data']

    # --- Provider overrides for the generic BaseDebrid cache (issue #77) ---

    @property
    def service_name(self) -> str:
        return "torbox"

    def _index_results_by_hash(self, response) -> dict:
        if not isinstance(response, dict):
            return {}
        return {
            item["hash"]: item
            for item in response.get("data", [])
            if item.get("hash")
        }

    def _reconstruct_response(self, items: list):
        return {"success": True, "detail": "From cache.", "data": items}

    def _sanitize_for_cache(self, item: dict) -> dict:
        return {
            "hash": item.get("hash"),
            "files": [
                {k: v for k, v in f.items() if k in ("name", "id", "size")}
                for f in item.get("files", [])
            ],
        }

    async def _lookup_stremthru(self, hashes, ip=None):
        """Query StremThru as community cache for TorBox (debrid code 'TB')."""
        if not hashes:
            return {}, []
        try:
            token = self.config.get("TBToken", "")
            st_results, still_remaining = await self._get_stremthru_community_cache(
                hashes, "torbox", token, debrid_code_filter="TB", ip=ip
            )
            cached = {}
            for item in st_results:
                hash_value = self._normalize_hash_value(item.get("hash"))
                if not hash_value:
                    continue
                cached[hash_value] = self._build_result_item(
                    hash_value,
                    instant=True,
                    files=item.get("files", []),
                    source="stremthru",
                )
            if cached:
                logger.debug(
                    f"Torbox: StremThru found {len(cached)} cached, {len(still_remaining)} remaining for bulk check"
                )
            return cached, still_remaining
        except Exception as e:
            logger.debug(f"Torbox: StremThru lookup failed: {str(e)}")
            return {}, hashes

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        logger.info(f"Torbox: Checking availability for {len(hashes_or_magnets)} hashes/magnets")

        # Normalize and deduplicate inputs while preserving order
        cleaned_hashes = list(
            dict.fromkeys(
                h for h in (self._normalize_hash_value(v) for v in hashes_or_magnets) if h
            )
        )

        if not cleaned_hashes:
            return {"success": True, "detail": "No hashes to check.", "data": []}

        result_by_hash = {}

        # Step 1: StremThru community cache (positive results only)
        if settings.stremthru_url:
            stremthru_cached, remaining_hashes = await self._lookup_stremthru(cleaned_hashes, ip)
            result_by_hash.update(stremthru_cached)
        else:
            remaining_hashes = cleaned_hashes

        # Step 2: TorBox native checkcached endpoint — POST JSON body, batches of 100
        batch_size = 100
        for i in range(0, len(remaining_hashes), batch_size):
            batch = remaining_hashes[i: i + batch_size]
            logger.debug(f"Torbox: Checking batch of {len(batch)} hashes (batch {i // batch_size + 1})")
            url = f"{self.base_url}/torrents/checkcached?format=list&list_files=true"
            response = await self.json_response(
                url,
                method='post',
                headers=self.get_headers(),
                json_data={"hashes": batch},
            )
            if response and response.get("success") and response.get("data"):
                for item in response["data"]:
                    h = self._normalize_hash_value(item.get("hash"))
                    if h:
                        result_by_hash[h] = self._build_result_item(
                            h,
                            instant=True,
                            files=item.get("files", []),
                            source="torbox_direct",
                        )
            else:
                logger.debug(f"Torbox: No cached results for batch {i // batch_size + 1}")

        cached_count = len(result_by_hash)
        logger.info(f"Torbox: Availability check done — {cached_count}/{len(cleaned_hashes)} cached")

        return {
            "success": True,
            "detail": "Torrent cache status retrieved successfully.",
            "data": [result_by_hash[h] for h in cleaned_hashes if h in result_by_hash],
        }

    async def _find_existing_torrent(self, info_hash):
        logger.info(f"Torbox: Searching for existing torrent with hash: {info_hash}")
        torrents = await self.json_response(f"{self.base_url}/torrents/mylist", headers=self.get_headers())
        if torrents and "data" in torrents:
            for torrent in torrents["data"]:
                if torrent["hash"].lower() == info_hash.lower():
                    logger.info(f"Torbox: Found existing torrent with ID: {torrent['id']}")
                    return torrent
        logger.info("Torbox: No existing torrent found")
        return None

    async def add_magnet_or_torrent(self, magnet, torrent_download=None, torrent_file_content=None, ip=None, privacy="private"):
        logger.debug("Torbox: Adding magnet or torrent")

        if torrent_file_content is not None:
            logger.info("Torbox: Attempting to add cached .torrent file")
            try:
                upload_response = await self.add_torrent(torrent_file_content, privacy)
                if upload_response and upload_response.get("success") and upload_response.get("data"):
                    logger.info("Torbox: Successfully added cached .torrent")
                    return upload_response["data"]
                logger.warning("Torbox: Failed to add cached .torrent")
            except Exception as e:
                logger.warning(f"Torbox: Exception with cached .torrent: {str(e)}")

        if torrent_download is not None:
            logger.info("Torbox: Downloading and adding .torrent file")
            try:
                torrent_file = await self.download_torrent_file(torrent_download)
                upload_response = await self.add_torrent(torrent_file, privacy)
                if upload_response and upload_response.get("success") and upload_response.get("data"):
                    logger.info("Torbox: Successfully added downloaded .torrent")
                    return upload_response["data"]
                logger.warning("Torbox: Failed to add downloaded .torrent, falling back to magnet")
            except Exception as e:
                logger.warning(f"Torbox: Exception downloading .torrent: {str(e)}")

        logger.info("Torbox: Adding magnet link")
        response = await self.add_magnet(magnet, ip, privacy)

        if not response or "data" not in response or response["data"] is None:
            logger.error("Torbox: Failed to add magnet/torrent")
            return None

        return response["data"]

    async def start_background_caching(self, magnet, query=None):
        """Start caching a magnet/torrent in the background via TorBox asynccreatetorrent."""
        logger.info("Torbox: Starting background caching for magnet")
        url = f"{self.base_url}/torrents/asynccreatetorrent"

        if query and isinstance(query, dict):
            raw = query.get("torrent_download")
            if raw:
                torrent_download = unquote(raw)
                try:
                    torrent_file = await self.download_torrent_file(torrent_download)
                    files = {
                        "file": (str(uuid.uuid4()) + ".torrent", torrent_file, 'application/x-bittorrent')
                    }
                    file_data = {"seed": 1, "allow_zip": "false"}
                    response = await self.json_response(
                        url, method='post', headers=self.get_headers(), data=file_data, files=files
                    )
                    if response and response.get("success"):
                        logger.info("Torbox: Background caching started via .torrent file")
                        return True
                except Exception as e:
                    logger.warning(f"Torbox: .torrent download failed for background caching: {e}")

        try:
            data = {"magnet": magnet, "seed": 1, "allow_zip": "false"}
            response = await self.json_response(
                url, method='post', headers=self.get_headers(), data=data, retry_on_429=False
            )
            if response and response.get("success"):
                logger.info("Torbox: Background caching started via magnet")
                return True
            logger.error(f"Torbox: Failed to start background caching: {response}")
            return False
        except Exception as e:
            logger.error(f"Torbox: start_background_caching error: {e}")
            return False

    async def _wait_for_torrent_completion(self, torrent_id, timeout=60, interval=10):
        logger.info(f"Torbox: Waiting for torrent completion, ID: {torrent_id}")

        READY_STATES = {"cached", "completed", "uploading"}

        async def check_status():
            torrent_info = await self.get_torrent_info(torrent_id)
            if torrent_info and "data" in torrent_info:
                data = torrent_info["data"]
                state = data.get("download_state", "")
                files = data.get("files", [])
                logger.info(f"Torbox: Current torrent state: {state}, files: {len(files)}")
                return state in READY_STATES and len(files) > 0
            return False

        result = await self.wait_for_ready_status(check_status, timeout, interval)
        if result:
            logger.info("Torbox: Torrent is ready")
        else:
            logger.warning("Torbox: Torrent completion timeout")
        return result

    def _select_file(self, torrent_info, stream_type, file_index, season, episode):
        logger.info(f"Torbox: Selecting file for {stream_type}, file_index: {file_index}, season: {season}, episode: {episode}")
        files = torrent_info.get("files", [])

        if stream_type == "movie":
            if file_index is not None:
                logger.info(f"Torbox: Selected file index {file_index} for movie")
                return file_index
            largest_file = max(files, key=lambda x: x["size"])
            logger.info(f"Torbox: Selected largest file (ID: {largest_file['id']}, Size: {largest_file['size']}) for movie")
            return largest_file["id"]

        elif stream_type == "series":
            if file_index is not None:
                logger.info(f"Torbox: Selected file index {file_index} for series")
                return file_index

            try:
                numeric_season = int(season.replace("S", ""))
                numeric_episode = int(episode.replace("E", ""))
            except (ValueError, TypeError):
                logger.error(f"Torbox: Invalid season/episode format: {season}/{episode}")
                return None

            logger.info(f"Torbox: DEBUG - Processing {len(files)} files total")
            for i, file in enumerate(files):
                logger.debug(f"Torbox: DEBUG - File {i+1}: {file['short_name']} (size: {file['size']}, is_video: {is_video_file(file['short_name'])})")

            matching_files = []
            for file in files:
                if is_video_file(file["short_name"]):
                    logger.debug(f"Torbox: Checking video file: {file['short_name']}")
                    if season_episode_in_filename(file["short_name"], numeric_season, numeric_episode):
                        logger.info(f"Torbox: ✓ RTN match for {file['short_name']}")
                        matching_files.append(file)
                    else:
                        logger.debug(f"Torbox: ✗ No RTN match for {file['short_name']}")

            logger.info(f"Torbox: {len(matching_files)} files found with RTN for S{numeric_season:02d}E{numeric_episode:02d}")

            if matching_files:
                largest_matching_file = max(matching_files, key=lambda x: x["size"])
                logger.info(f"Torbox: Selected largest matching file (ID: {largest_matching_file['id']}, Name: {largest_matching_file['short_name']}, Size: {largest_matching_file['size']}) for series")
                return largest_matching_file["id"]
            else:
                logger.warning(f"Torbox: No matching files found for S{numeric_season:02d}E{numeric_episode:02d}, trying smart fallback")
                from stream_fusion.utils.general import smart_episode_fallback

                fallback_files = [
                    {
                        "name": file["short_name"],
                        "size": file["size"],
                        "index": file["id"]
                    }
                    for file in files if is_video_file(file["short_name"])
                ]

                logger.info(f"Torbox: Calling smart fallback with {len(fallback_files)} files")

                fallback_file = smart_episode_fallback(fallback_files, numeric_season, numeric_episode)
                if fallback_file:
                    logger.info(f"Torbox: Smart fallback selected: {fallback_file.get('name')} (ID: {fallback_file.get('index')})")
                    return fallback_file.get('index')
                else:
                    logger.info("Torbox: Smart fallback found nothing, trying final fallback for single file")
                    video_files = [f for f in files if is_video_file(f["short_name"])]
                    logger.debug(f"Torbox: DEBUG - Found {len(video_files)} video files in final fallback")

                    if len(video_files) == 1:
                        single_file = video_files[0]
                        logger.info(f"Torbox: Single video file detected, using: {single_file['short_name']} (ID: {single_file['id']})")
                        return single_file["id"]
                    else:
                        logger.error(f"Torbox: Smart fallback also failed for S{numeric_season:02d}E{numeric_episode:02d}")
                        logger.error(f"Torbox: Found {len(video_files)} video files, expected exactly 1 for single file fallback")
                        return None
