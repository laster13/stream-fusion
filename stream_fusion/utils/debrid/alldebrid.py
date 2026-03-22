# alldebrid.py - AllDebrid v4.1 API (using magnet/status for files)
import re
import uuid
import aiohttp
from urllib.parse import unquote

from fastapi import HTTPException

from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.general import season_episode_in_filename
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings


def flatten_files(files, result=None):
    if result is None:
        result = []

    if not isinstance(files, list):
        return result

    for file_item in files:
        if not isinstance(file_item, dict):
            continue

        result.append(file_item)

        children = file_item.get("e", [])
        if children and isinstance(children, list):
            flatten_files(children, result)

    return result


class AllDebrid(BaseDebrid):
    def __init__(self, config, session: aiohttp.ClientSession = None):
        super().__init__(config, session)
        self.base_url = f"{settings.ad_base_url}/{settings.ad_api_version}/"
        self.agent = settings.ad_user_app

    def get_headers(self):
        if settings.ad_unique_account:
            if not settings.proxied_link:
                logger.warning("AllDebrid: Unique account enabled, but proxied link is disabled. This may lead to account ban.")
                logger.warning("AllDebrid: Please enable proxied link in the settings.")
                raise HTTPException(status_code=500, detail="Proxied link is disabled.")
            if settings.ad_token:
                return {"Authorization": f"Bearer {settings.ad_token}"}
            logger.warning("AllDebrid: Unique account enabled, but no token provided. Please provide a token in the env.")
            raise HTTPException(status_code=500, detail="AllDebrid token is not provided.")

        return {"Authorization": f"Bearer {self.config.get('ADToken')}"}

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

        return value

    def _extract_uploaded_ids(self, response):
        uploaded_ids = []

        if not isinstance(response, dict):
            return uploaded_ids

        magnets = response.get("data", {}).get("magnets", [])
        if not isinstance(magnets, list):
            return uploaded_ids

        for item in magnets:
            if not isinstance(item, dict):
                continue
            magnet_id = item.get("id")
            if magnet_id is not None:
                uploaded_ids.append(str(magnet_id))

        return uploaded_ids

    async def delete_magnet(self, magnet_id, ip=None):
        url = f"{self.base_url}magnet/delete?id={magnet_id}&agent={self.agent}"
        return await self.json_response(url, method="get", headers=self.get_headers())

    async def delete_magnets(self, magnet_ids, ip=None):
        if not magnet_ids:
            return

        success = 0
        for magnet_id in magnet_ids:
            try:
                response = await self.delete_magnet(magnet_id, ip)
                if response and response.get("status") == "success":
                    success += 1
            except Exception as e:
                logger.debug(f"AllDebrid: Failed to delete magnet {magnet_id}: {str(e)}")

        logger.info(f"AllDebrid: Deleted {success}/{len(magnet_ids)} uploaded magnets after availability check")

    async def add_magnet(self, magnet, ip=None):
        url = f"{self.base_url}magnet/upload?agent={self.agent}"
        data = {"magnets[]": magnet}
        return await self.json_response(url, method="post", headers=self.get_headers(), data=data)

    async def add_torrent(self, torrent_file, ip=None):
        url = f"{self.base_url}magnet/upload/file?agent={self.agent}"
        files = {"files[]": (str(uuid.uuid4()) + ".torrent", torrent_file, "application/x-bittorrent")}
        return await self.json_response(url, method="post", headers=self.get_headers(), files=files)

    async def get_magnet_files(self, id, ip=None):
        url = f"{settings.ad_base_url}/v4/magnet/files"
        data = {"id[]": id, "agent": self.agent}
        return await self.json_response(url, method="post", headers=self.get_headers(), data=data)

    async def unrestrict_link(self, link, ip=None):
        url = f"{self.base_url}link/unlock?agent={self.agent}&link={link}"
        return await self.json_response(url, method="get", headers=self.get_headers())

    async def get_stream_link(self, query, config=None, ip=None):
        magnet = query["magnet"]
        stream_type = query["type"]
        torrent_download = (
            unquote(query["torrent_download"])
            if query["torrent_download"] is not None
            else None
        )
        torrent_file_content = query.get("torrent_file_content", None)

        torrent_id = await self.add_magnet_or_torrent(
            magnet, torrent_download, torrent_file_content, ip
        )
        torrent_id = str(torrent_id) if torrent_id else ""
        logger.info(f"AllDebrid: Torrent ID: {torrent_id}")

        if not torrent_id or torrent_id.startswith("Error"):
            logger.error(f"AllDebrid: Failed to add torrent: {torrent_id}")
            return settings.no_cache_video_url

        logger.info(f"AllDebrid: Retrieving files for torrent ID: {torrent_id}")
        try:
            files_response = await self.get_magnet_files(torrent_id, ip)
            logger.debug(f"AllDebrid: Files response: {files_response}")

            if not files_response:
                logger.error("AllDebrid: Null response from get_magnet_files")
                return settings.no_cache_video_url

            if files_response.get("status") != "success":
                logger.warning(
                    f"AllDebrid: get_magnet_files returned error: {files_response.get('error')}"
                )
                return settings.no_cache_video_url

            if "data" not in files_response:
                logger.error("AllDebrid: No data in files response")
                return settings.no_cache_video_url

            magnets = files_response["data"].get("magnets", [])
            if not magnets:
                logger.error("AllDebrid: No magnet data in files response")
                return settings.no_cache_video_url

            magnet_data = magnets[0]
            files = flatten_files(magnet_data.get("files", []))
            logger.info(f"AllDebrid: Retrieved {len(files)} files (after flattening)")

        except Exception as e:
            logger.error(f"AllDebrid: Error getting magnet files: {str(e)}")
            return settings.no_cache_video_url

        link = settings.no_cache_video_url

        if stream_type == "movie":
            logger.info("AllDebrid: Finding largest file for movie")
            try:
                largest_file = max(files, key=lambda x: x.get("s", 0)) if files else None

                if largest_file and largest_file.get("l"):
                    link = largest_file["l"]
                    logger.info("AllDebrid: Found movie link")
                else:
                    logger.error("AllDebrid: No valid link found for movie")
            except Exception as e:
                logger.error(f"AllDebrid: Error processing movie: {str(e)}")

        elif stream_type == "series":
            numeric_season = int(query["season"].replace("S", ""))
            numeric_episode = int(query["episode"].replace("E", ""))
            logger.info(f"AllDebrid: Finding S{numeric_season:02d}E{numeric_episode:02d}")

            try:
                file_index = query.get("file_index")
                if file_index is not None:
                    try:
                        idx = int(file_index) - 1
                        if 0 <= idx < len(files):
                            indexed_file = files[idx]
                            if indexed_file.get("l"):
                                link = indexed_file["l"]
                                logger.info(f"AllDebrid: Found episode link using file_index={file_index}")
                    except (TypeError, ValueError):
                        logger.debug(f"AllDebrid: Invalid file_index={file_index}")

                if link == settings.no_cache_video_url:
                    matching_files = []
                    for file_info in files:
                        if not isinstance(file_info, dict):
                            continue

                        filename = file_info.get("n", "")

                        if season_episode_in_filename(
                            filename, numeric_season, numeric_episode
                        ):
                            logger.debug(f"AllDebrid: ✓ Match: {filename}")
                            matching_files.append(file_info)
                            continue

                        episode_patterns = [
                            rf"[Ss]{numeric_season:02d}[Ee]{numeric_episode:02d}",
                            rf"[Ss]{numeric_season}[Ee]{numeric_episode:02d}",
                            rf"{numeric_season:02d}x{numeric_episode:02d}",
                            rf"{numeric_season}x{numeric_episode:02d}",
                            rf"[Ss]eason.{numeric_season:02d}.*[Ee]{numeric_episode:02d}",
                            rf"[Ss]eason.{numeric_season}.*[Ee]{numeric_episode:02d}",
                            rf"[Ss]{numeric_season:02d}.*[Ee]pisode.{numeric_episode:02d}",
                            rf"[Ss]{numeric_season}.*[Ee]pisode.{numeric_episode:02d}",
                            rf"\b{numeric_episode:03d}\b",
                            rf"[Ee]p\.?\s*{numeric_episode:03d}\b",
                            rf"[Ee]pisode\s*{numeric_episode:03d}\b",
                        ]

                        for pattern in episode_patterns:
                            if re.search(pattern, filename, re.IGNORECASE):
                                logger.debug(f"AllDebrid: ✓ Match with pattern: {filename}")
                                matching_files.append(file_info)
                                break

                    if matching_files:
                        target_file = max(matching_files, key=lambda x: x.get("s", 0))
                        if target_file.get("l"):
                            link = target_file["l"]
                            logger.info("AllDebrid: Found episode link by filename match")
                        else:
                            logger.error("AllDebrid: Matching file has no link")
                    else:
                        logger.warning(
                            f"AllDebrid: No files found for S{numeric_season:02d}E{numeric_episode:02d}"
                        )

            except Exception as e:
                logger.error(f"AllDebrid: Error processing series: {str(e)}")

        else:
            logger.error("AllDebrid: Unsupported stream type.")
            raise HTTPException(status_code=500, detail="Unsupported stream type.")

        if link == settings.no_cache_video_url:
            logger.info("AllDebrid: No link found, returning no-cache URL")
            return link

        logger.info("AllDebrid: Retrieved link successfully")

        try:
            unlocked_response = await self.unrestrict_link(link, ip)
            if (
                unlocked_response
                and unlocked_response.get("status") == "success"
                and "data" in unlocked_response
            ):
                final_link = unlocked_response["data"].get("link", link)
                logger.info("AllDebrid: Link unrestricted")
                return final_link
        except Exception as e:
            logger.debug(f"AllDebrid: Could not unrestrict link: {str(e)}")

        return link

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """
        Vérifie la disponibilité réelle côté AllDebrid en uploadant les magnets/hashes.
        Un torrent est considéré instant/cached si:
        - ready == True
        - ou statusCode == 4
        """
        if len(hashes_or_magnets) == 0:
            logger.info("AllDebrid: No hashes to check")
            return {"status": "success", "data": {"magnets": []}}

        cleaned_hashes = []
        for value in hashes_or_magnets:
            normalized = self._normalize_hash_value(value)
            if normalized:
                cleaned_hashes.append(normalized)

        if not cleaned_hashes:
            logger.info("AllDebrid: No valid hashes to check")
            return {"status": "success", "data": {"magnets": []}}

        batch_size = 20
        result_magnets = []

        for i in range(0, len(cleaned_hashes), batch_size):
            batch = cleaned_hashes[i:i + batch_size]

            url = f"{self.base_url}magnet/upload?agent={self.agent}"
            data = {"magnets[]": batch}
            uploaded_ids = []

            try:
                response = await self.json_response(
                    url,
                    method="post",
                    headers=self.get_headers(),
                    data=data,
                )

                uploaded_ids = self._extract_uploaded_ids(response)

                if not response or response.get("status") != "success":
                    error = response.get("error", {}) if isinstance(response, dict) else {}
                    error_code = error.get("code") if isinstance(error, dict) else None

                    if error_code == "MAGNET_TOO_MANY_ACTIVE":
                        logger.warning(
                            "AllDebrid: Active magnet limit reached during availability check. "
                            "Skipping batch without marking items as instant."
                        )
                    else:
                        logger.warning(f"AllDebrid: Upload batch failed: {response}")

                    for hash_value in batch:
                        result_magnets.append(
                            {
                                "hash": hash_value,
                                "instant": False,
                                "files": [],
                            }
                        )
                    continue

                magnets_data = response.get("data", {}).get("magnets", [])
                logger.info(
                    f"AllDebrid: Batch {i // batch_size + 1} returned {len(magnets_data)} magnets"
                )

                seen_hashes = set()

                for magnet_data in magnets_data:
                    raw_hash_value = magnet_data.get("hash") or magnet_data.get("magnet")
                    hash_value = self._normalize_hash_value(raw_hash_value)

                    ready = magnet_data.get("ready", False)
                    status_code = magnet_data.get("statusCode")

                    if not ready and status_code == 4:
                        ready = True

                    if hash_value:
                        seen_hashes.add(hash_value)
                        result_magnets.append(
                            {
                                "hash": hash_value,
                                "instant": bool(ready),
                                "files": magnet_data.get("files", []),
                            }
                        )

                for hash_value in batch:
                    if hash_value not in seen_hashes:
                        result_magnets.append(
                            {
                                "hash": hash_value,
                                "instant": False,
                                "files": [],
                            }
                        )

            except Exception as e:
                logger.error(f"AllDebrid: Error checking availability batch: {str(e)}")
                for hash_value in batch:
                    result_magnets.append(
                        {
                            "hash": hash_value,
                            "instant": False,
                            "files": [],
                        }
                    )
            finally:
                try:
                    await self.delete_magnets(uploaded_ids, ip)
                except Exception as e:
                    logger.debug(f"AllDebrid: Cleanup failed after availability check: {str(e)}")

        ready_count = sum(1 for item in result_magnets if item.get("instant") is True)
        logger.info(
            f"AllDebrid: Availability check completed - {ready_count}/{len(result_magnets)} instant"
        )

        return {"status": "success", "data": {"magnets": result_magnets}}

    async def add_magnet_or_torrent(
        self, magnet, torrent_download=None, torrent_file_content=None, ip=None
    ):
        logger.debug("AllDebrid: Adding magnet or torrent")
        torrent_id = ""

        if torrent_file_content is not None:
            logger.info("AllDebrid: Attempting to add cached .torrent file")
            try:
                upload_response = await self.add_torrent(torrent_file_content, ip)
                if upload_response and upload_response.get("status") == "success":
                    files = upload_response.get("data", {}).get("files", [])
                    if files:
                        torrent_id = files[0].get("id")
                        if torrent_id:
                            logger.info(
                                f"AllDebrid: Successfully added cached .torrent, ID: {torrent_id}"
                            )
                            return str(torrent_id)
                logger.warning("AllDebrid: Failed to add cached .torrent")
            except Exception as e:
                logger.warning(f"AllDebrid: Exception with cached .torrent: {str(e)}")

        if torrent_download is not None:
            logger.info("AllDebrid: Downloading and adding .torrent file")
            try:
                torrent_file = await self.download_torrent_file(torrent_download)
                upload_response = await self.add_torrent(torrent_file, ip)

                if upload_response and upload_response.get("status") == "success":
                    files = upload_response.get("data", {}).get("files", [])
                    if files:
                        torrent_id = files[0].get("id")
                        if torrent_id:
                            logger.info(
                                f"AllDebrid: Successfully added downloaded .torrent, ID: {torrent_id}"
                            )
                            return str(torrent_id)
                logger.warning(
                    "AllDebrid: Failed to add downloaded .torrent, falling back to magnet"
                )
            except Exception as e:
                logger.warning(f"AllDebrid: Exception downloading .torrent: {str(e)}")

        logger.info("AllDebrid: Adding magnet link")
        magnet_response = await self.add_magnet(magnet, ip)

        if not magnet_response or magnet_response.get("status") != "success":
            logger.error(f"AllDebrid: Failed to add magnet: {magnet_response}")
            return "Error: Failed to add magnet."

        try:
            magnets = magnet_response.get("data", {}).get("magnets", [])
            if magnets:
                torrent_id = magnets[0].get("id")
                logger.info(f"AllDebrid: Successfully added magnet, ID: {torrent_id}")
                return str(torrent_id)
        except Exception as e:
            logger.error(f"AllDebrid: Exception extracting magnet ID: {str(e)}")

        return "Error: Could not extract torrent ID."