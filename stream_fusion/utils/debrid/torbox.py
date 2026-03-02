from itertools import islice
import uuid
import asyncio
import aiohttp

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

    async def add_magnet(self, magnet, ip=None, privacy="private"):
        logger.info(f"Torbox: Adding magnet: {magnet[:50]}...")
        url = f"{self.base_url}/torrents/createtorrent"
        seed = 3
        data = {
            "magnet": magnet,
            "seed": seed,
            "allow_zip": "false"
        }
        response = await self.json_response(url, method='post', headers=self.get_headers(), data=data, retry_on_429=False)
        logger.info(f"Torbox: Add magnet response: {response}")
        return response

    async def add_torrent(self, torrent_file, privacy="private"):
        logger.info("Torbox: Adding torrent file")
        url = f"{self.base_url}/torrents/createtorrent"
        seed = 3
        data = {
            "seed": seed,
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
        logger.info(f"Torbox: Controlling torrent ID: {torrent_id}, operation: {operation}")
        url = f"{self.base_url}/torrents/controltorrent"
        data = {
            "torrent_id": torrent_id,
            "operation": operation
        }
        response = await self.json_response(url, method='post', headers=self.get_headers(), data=data)
        logger.info(f"Torbox: Control torrent response: {response}")
        return response

    async def request_download_link(self, torrent_id, file_id=None, zip_link=False):
        """Request download link with retry logic"""
        logger.info(f"Torbox: Requesting download link for torrent ID: {torrent_id}, file ID: {file_id}, zip link: {zip_link}")
        url = f"{self.base_url}/torrents/requestdl?token={self.token}&torrent_id={torrent_id}&file_id={file_id}&zip_link={str(zip_link).lower()}"
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
        if torrent_download:
            from urllib.parse import unquote
            torrent_download = unquote(torrent_download)

        info_hash = get_info_hash_from_magnet(magnet)
        logger.info(f"Torbox: Getting stream link for {stream_type} with hash: {info_hash}")

        # Check if the torrent is already added
        existing_torrent = await self._find_existing_torrent(info_hash)

        if existing_torrent:
            logger.info(f"Torbox: Found existing torrent with ID: {existing_torrent['id']}")
            torrent_id = existing_torrent["id"]
            # Get full torrent info with files
            torrent_response = await self.get_torrent_info(torrent_id)
            if not torrent_response or "data" not in torrent_response:
                logger.error("Torbox: Failed to get torrent info.")
                return None
            torrent_info = torrent_response["data"]
        else:
            # Add the magnet or torrent file
            add_response = await self.add_magnet_or_torrent(magnet, torrent_download)
            if not add_response or "torrent_id" not in add_response:
                logger.error("Torbox: Failed to add or find torrent.")
                return None
            torrent_id = add_response["torrent_id"]
            # Get full torrent info with files
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
        download_link_response = await self.request_download_link(torrent_id, file_id)

        if not download_link_response or "data" not in download_link_response:
            logger.error("Torbox: Failed to get download link.")
            return settings.no_cache_video_url

        logger.info(f"Torbox: Got download link: {download_link_response['data']}")
        return download_link_response['data']

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        logger.info(f"Torbox: Checking availability for {len(hashes_or_magnets)} hashes/magnets")

        all_results = []

        for i in range(0, len(hashes_or_magnets), 50):
            batch = list(islice(hashes_or_magnets, i, i + 50))
            logger.info(f"Torbox: Checking batch of {len(batch)} hashes/magnets (batch {i//50 + 1})")
            url = f"{self.base_url}/torrents/checkcached?hash={','.join(batch)}&format=list&list_files=true"
            response = await self.json_response(url, headers=self.get_headers())

            if response and response.get("success") and response["data"]:
                all_results.extend(response["data"])
            else:
                logger.debug(f"Torbox: No cached availability for batch {i//50 + 1}")
                return None

        logger.info(f"Torbox: Availability check completed for all {len(hashes_or_magnets)} hashes/magnets")
        return {
            "success": True,
            "detail": "Torrent cache status retrieved successfully.",
            "data": all_results
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

    async def add_magnet_or_torrent(self, magnet, torrent_download=None, ip=None, privacy="private"):
        # Always use magnet: TorBox ignores seed=3 with .torrent file uploads
        logger.info("Torbox: Adding magnet (ignoring .torrent to preserve seed settings)")
        response = await self.add_magnet(magnet, ip, privacy)

        logger.info(f"Torbox: Add torrent response: {response}")

        if not response or "data" not in response or response["data"] is None:
            logger.error("Torbox: Failed to add magnet/torrent")
            return None

        return response["data"]

    async def _wait_for_torrent_completion(self, torrent_id, timeout=60, interval=10):
        logger.info(f"Torbox: Waiting for torrent completion, ID: {torrent_id}")

        async def check_status():
            torrent_info = await self.get_torrent_info(torrent_id)
            if torrent_info and "data" in torrent_info:
                files = torrent_info["data"].get("files", [])
                logger.info(f"Torbox: Current torrent status: {torrent_info['data']['download_state']}")
                return True if len(files) > 0 else False
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
