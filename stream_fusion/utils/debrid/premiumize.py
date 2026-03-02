import asyncio
import aiohttp
import time

from stream_fusion.settings import settings
from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.general import get_info_hash_from_magnet, season_episode_in_filename
from stream_fusion.logging_config import logger


class Premiumize(BaseDebrid):
    def __init__(self, config, session: aiohttp.ClientSession = None):
        super().__init__(config, session)
        self.base_url = "https://www.premiumize.me/api"
        self.api_key = config.get('PMToken') or settings.pm_token
        if not self.api_key:
            logger.error("No Premiumize API key found in config or settings")
            raise ValueError("Premiumize API key is required")

        self._token_checked = False

    async def _ensure_token_checked(self):
        """Lazy token check - called before first API operation"""
        if not self._token_checked:
            await self._check_token()
            self._token_checked = True

    async def _check_token(self):
        """Vérifier la validité du token en appelant l'API account/info"""
        url = f"{self.base_url}/account/info"
        response = await self.json_response(
            url,
            method='post',
            data={'apikey': self.api_key}
        )

        if not response or response.get("status") != "success":
            logger.error(f"Invalid Premiumize API key: {self.api_key}")
            raise ValueError("Invalid Premiumize API key")

        logger.info("Premiumize API key is valid")

    async def add_magnet(self, magnet, ip=None):
        await self._ensure_token_checked()
        url = f"{self.base_url}/transfer/create?apikey={self.api_key}"

        info_hash = get_info_hash_from_magnet(magnet)
        is_season_pack = self._check_if_season_pack(magnet)

        form = {
            'src': magnet,
            'folder_name': f"season_pack_{info_hash}" if is_season_pack else None
        }

        response = await self.json_response(url, method='post', data=form)

        if is_season_pack and response and response.get("status") == "success":
            transfer_id = response.get("id")
            if transfer_id:
                if await self._wait_for_season_pack(transfer_id):
                    folder_details = await self.get_folder_or_file_details(transfer_id)
                    if folder_details and folder_details.get("content"):
                        video_files = [f for f in folder_details["content"]
                                     if f.get("mime_type", "").startswith("video/")]
                        if video_files:
                            largest_file = max(video_files, key=lambda x: x.get("size", 0))
                            response["selected_file"] = {
                                "id": largest_file.get("id"),
                                "name": largest_file.get("name"),
                                "size": largest_file.get("size"),
                                "link": largest_file.get("link"),
                                "stream_link": largest_file.get("stream_link")
                            }

        return response

    def _check_if_season_pack(self, magnet):
        """Vérifie si le magnet link correspond à un pack de saison"""
        name = magnet.lower()
        season_indicators = [
            "complete.season",
            "season.complete",
            "s01.complete",
            "saison.complete",
            "season.pack",
            "pack.saison",
            ".s01.",
            ".s02.",
            ".s03.",
            ".s04.",
            ".s05.",
            "saison"
        ]
        return any(indicator in name for indicator in season_indicators)

    async def _wait_for_season_pack(self, transfer_id, timeout=300, max_retries=10):
        """Attend que tous les fichiers d'un pack de saison soient disponibles"""
        start_time = time.time()
        retry_count = 0

        while time.time() - start_time < timeout and retry_count < max_retries:
            try:
                transfer_info = await self.get_folder_or_file_details(transfer_id)

                if transfer_info and transfer_info.get("status") == "success":
                    if transfer_info.get("content"):
                        video_files = [f for f in transfer_info["content"]
                                     if f.get("mime_type", "").startswith("video/")]
                        if video_files:
                            logger.info(f"Season pack ready with {len(video_files)} video files")
                            return True

                retry_count += 1
                elapsed = time.time() - start_time
                logger.debug(f"Waiting for season pack: {elapsed:.1f}s elapsed, retry {retry_count}/{max_retries}")
                await asyncio.sleep(5)
            except Exception as e:
                logger.warning(f"Error waiting for season pack: {e}")
                retry_count += 1
                await asyncio.sleep(5)

        logger.warning(f"Season pack timeout after {time.time() - start_time:.1f}s or {retry_count} retries")
        return False

    async def add_torrent(self, torrent_file):
        await self._ensure_token_checked()
        url = f"{self.base_url}/transfer/create?apikey={self.api_key}"
        form = {'file': torrent_file}
        return await self.json_response(url, method='post', data=form)

    async def list_transfers(self):
        await self._ensure_token_checked()
        url = f"{self.base_url}/transfer/list?apikey={self.api_key}"
        return await self.json_response(url)

    async def get_folder_or_file_details(self, item_id, is_folder=True):
        """Get folder or file details"""
        await self._ensure_token_checked()
        try:
            if is_folder:
                logger.debug(f"Getting folder details with id: {item_id}")
                url = f"{self.base_url}/folder/list?id={item_id}&apikey={self.api_key}"
            else:
                logger.debug(f"Getting file details with id: {item_id}")
                url = f"{self.base_url}/item/details?id={item_id}&apikey={self.api_key}"

            response = await self.json_response(url)

            if response is None:
                logger.warning(f"No response from Premiumize API for item {item_id}")
                return None

            return response

        except asyncio.TimeoutError as e:
            logger.error(f"Timeout getting details for item {item_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting details for item {item_id}: {e}")
            return None

    async def get_availability(self, hash):
        """Get availability for a single hash"""
        await self._ensure_token_checked()
        if not hash:
            return {"transcoded": [False]}

        url = f"{self.base_url}/cache/check?apikey={self.api_key}&items[]={hash}"
        response = await self.json_response(url)

        if not response or response.get("status") != "success":
            logger.error("Invalid response from Premiumize API")
            return {"transcoded": [False]}

        return {
            "transcoded": response.get("transcoded", [False])
        }

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """Get availability for multiple hashes or magnets"""
        await self._ensure_token_checked()
        if not hashes_or_magnets:
            return {}

        logger.info(f"Checking availability for {len(hashes_or_magnets)} items")
        logger.debug(f"Using Premiumize API key: {self.api_key}")

        url = f"{self.base_url}/cache/check"
        response = await self.json_response(
            url,
            method='post',
            data={
                'apikey': self.api_key,
                'items[]': hashes_or_magnets
            }
        )

        logger.info(f"Raw Premiumize response: {response}")

        if not response or response.get("status") != "success":
            logger.error("Invalid response from Premiumize API")
            return {}

        result = {}
        for i, hash_or_magnet in enumerate(hashes_or_magnets):
            is_available = bool(response.get("response", [])[i]) if isinstance(response.get("response", []), list) and i < len(response["response"]) else False

            filename = None
            if isinstance(response.get("filename", []), list) and i < len(response["filename"]):
                filename = response["filename"][i]

            filesize = 0
            if isinstance(response.get("filesize", []), list) and i < len(response["filesize"]):
                try:
                    filesize = int(response["filesize"][i]) if response["filesize"][i] is not None else 0
                except (ValueError, TypeError):
                    filesize = 0

            result[hash_or_magnet] = {
                "transcoded": is_available,
                "filename": filename,
                "filesize": filesize
            }

        logger.info(f"Formatted response: {result}")
        logger.info(f"Got availability for {len(result)} items")
        return result

    async def start_background_caching(self, magnet, query=None):
        """Start caching a magnet link in the background."""
        await self._ensure_token_checked()
        logger.info(f"Starting background caching for magnet")

        try:
            response = await self.json_response(
                f"{self.base_url}/transfer/create",
                method="post",
                data={"apikey": self.api_key, "src": magnet}
            )

            if not response or response.get("status") != "success":
                logger.error("Failed to start background caching")
                return False

            transfer_id = response.get("id")
            if not transfer_id:
                logger.error("No transfer ID returned")
                return False

            logger.info(f"Successfully started background caching with transfer ID: {transfer_id}")
            return True
        except Exception as e:
            logger.error(f"Error starting background caching: {str(e)}")
            return False

    async def get_stream_link(self, query, config=None, ip=None, global_timeout=30):
        """Get a stream link for a magnet link with timeout protection"""
        await self._ensure_token_checked()
        start_time = time.time()

        if not query:
            return None

        logger.info(f"Getting stream link for magnet (global timeout: {global_timeout}s)")

        try:
            season = None
            episode = None
            if isinstance(query, dict):
                magnet = query.get("magnet")
                if not magnet:
                    logger.error("No magnet link in query")
                    return None

                if query.get("type") == "series" and query.get("season") and query.get("episode"):
                    season = query["season"].replace("S", "") if isinstance(query["season"], str) else query["season"]
                    episode = query["episode"].replace("E", "") if isinstance(query["episode"], str) else query["episode"]
                    try:
                        season = int(season)
                        episode = int(episode)
                    except (ValueError, TypeError):
                        logger.error(f"Invalid season/episode format: {season}/{episode}")
                        return None
            else:
                magnet = query

            def check_timeout(operation_name):
                elapsed = time.time() - start_time
                if elapsed > global_timeout:
                    logger.error(f"{operation_name} exceeded global timeout ({elapsed:.1f}s > {global_timeout}s)")
                    return True
                return False

            # Essayer d'abord le téléchargement direct
            if check_timeout("Direct download"):
                return None

            try:
                response = await self.json_response(
                    f"{self.base_url}/transfer/directdl",
                    method="post",
                    data={"apikey": self.api_key, "src": magnet}
                )

                if response and response.get("status") == "success":
                    logger.info("Got direct download response")
                    if "content" in response and response["content"]:
                        if season is not None and episode is not None:
                            matching_files = []
                            for file in response["content"]:
                                filename = file.get("path", "").split("/")[-1]
                                if season_episode_in_filename(filename, season, episode):
                                    matching_files.append(file)

                            if matching_files:
                                selected_file = max(matching_files, key=lambda x: x.get("size", 0))
                                stream_link = selected_file.get("stream_link") or selected_file.get("link")
                                if stream_link:
                                    logger.info(f"Found matching episode stream link: {stream_link[:50]}...")
                                    return stream_link

                        video_files = [f for f in response["content"]
                                     if isinstance(f.get("path", ""), str) and
                                     f.get("path", "").lower().endswith((".mkv", ".mp4", ".avi", ".m4v"))]

                        if video_files:
                            largest_file = max(video_files, key=lambda x: x.get("size", 0))
                            stream_link = largest_file.get("stream_link") or largest_file.get("link")
                            if stream_link:
                                logger.info(f"Found stream link: {stream_link[:50]}...")
                                return stream_link
                    elif response.get("location"):
                        logger.info(f"Found direct location: {response['location'][:50]}...")
                        return response["location"]

            except Exception as e:
                logger.warning(f"Error in direct download: {str(e)}")

            if check_timeout("Add magnet"):
                return None

            response = await self.add_magnet(magnet, ip)
            if not response or response.get("status") != "success":
                logger.error("Failed to add magnet")
                return None

            transfer_id = response.get("id")
            if not transfer_id:
                logger.error("No transfer ID in response")
                return None

            if check_timeout("Wait for transfer"):
                return None

            remaining_timeout = global_timeout - (time.time() - start_time)
            if not await self._wait_for_season_pack(transfer_id, timeout=int(remaining_timeout)):
                logger.error("Transfer timed out")
                return None

            if check_timeout("Get folder details"):
                return None

            folder_details = await self.get_folder_or_file_details(transfer_id)
            if not folder_details or not folder_details.get("content"):
                logger.error("No content in folder details")
                return None

            video_files = [f for f in folder_details["content"]
                          if isinstance(f.get("mime_type", ""), str) and
                          f.get("mime_type", "").startswith("video/")]

            if season is not None and episode is not None:
                matching_files = []
                for file in video_files:
                    filename = file.get("name", "")
                    if season_episode_in_filename(filename, season, episode):
                        matching_files.append(file)

                if matching_files:
                    selected_file = max(matching_files, key=lambda x: x.get("size", 0))
                    logger.info(f"Selected matching episode file: {selected_file.get('name')}")
                    return selected_file.get("stream_link")

            if video_files:
                selected_file = max(video_files, key=lambda x: x.get("size", 0))
                logger.info(f"Selected largest video file: {selected_file.get('name')}")
                return selected_file.get("stream_link")

            logger.error("No suitable video file found")
            return None

        except Exception as e:
            logger.error(f"Exception in get_stream_link: {str(e)}")
            return None
        finally:
            elapsed = time.time() - start_time
            logger.debug(f"get_stream_link completed in {elapsed:.1f}s")
