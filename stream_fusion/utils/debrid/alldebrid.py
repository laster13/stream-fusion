import re
import uuid
import aiohttp
import asyncio
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
        # Rate limit: 600 req/60s = 10 req/s, safely under the official 12 req/s AD limit
        self.global_limit = 600
        self.global_period = 60

    def get_headers(self):
        if settings.ad_unique_account:
            if not settings.proxied_link:
                logger.warning(
                    "AllDebrid: Unique account enabled, but proxied link is disabled. This may lead to account ban."
                )
                logger.warning(
                    "AllDebrid: Please enable proxied link in the settings."
                )
                raise HTTPException(
                    status_code=500, detail="Proxied link is disabled."
                )
            if settings.ad_token:
                return {"Authorization": f"Bearer {settings.ad_token}"}
            logger.warning(
                "AllDebrid: Unique account enabled, but no token provided. Please provide a token in the env."
            )
            raise HTTPException(
                status_code=500, detail="AllDebrid token is not provided."
            )

        token = self.config.get("ADToken")
        if not token:
            raise HTTPException(
                status_code=500, detail="AllDebrid token is not provided."
            )

        return {"Authorization": f"Bearer {token}"}

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

    def _extract_hashes_from_response(self, response):
        found = {}
        if not isinstance(response, dict):
            return found

        magnets = response.get("data", {}).get("magnets", [])
        if not isinstance(magnets, list):
            return found

        for item in magnets:
            if not isinstance(item, dict):
                continue
            raw_hash_value = item.get("hash") or item.get("magnet")
            hash_value = self._normalize_hash_value(raw_hash_value)
            if hash_value:
                found[hash_value] = item

        return found

    def _build_result_item(self, hash_value, instant=False, files=None, source="ad"):
        return {
            "hash": hash_value,
            "instant": bool(instant),
            "files": files if isinstance(files, list) else [],
            "source": source,
        }

    # --- Provider overrides for the generic BaseDebrid cache (issue #77) ---

    @property
    def service_name(self) -> str:
        return "alldebrid"

    def _index_results_by_hash(self, response) -> dict:
        if not isinstance(response, dict):
            return {}
        # Only include hashes confirmed as instantly available
        return {
            m["hash"]: m
            for m in response.get("data", {}).get("magnets", [])
            if m.get("hash") and m.get("instant")
        }

    def _reconstruct_response(self, items: list):
        return {"status": "success", "data": {"magnets": items}}

    def _sanitize_for_cache(self, item: dict) -> dict:
        # AD availability items contain no private links — whitelist safe fields only
        return {
            "hash": item.get("hash"),
            "instant": item.get("instant", False),
            "files": item.get("files", []),
        }

    async def _lookup_stremthru(self, hashes, ip=None):
        """
        Query StremThru as community cache for AllDebrid.
        Uses the _get_stremthru_community_cache() hook from BaseDebrid.
        Filters only AllDebrid results (debrid_code AD).
        """
        if not hashes:
            return {}, []

        try:
            token = self.config.get("ADToken", "")
            st_results, still_remaining = await self._get_stremthru_community_cache(
                hashes, "alldebrid", token, debrid_code_filter="AD", ip=ip
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
                    f"AllDebrid: StremThru found {len(cached)} cached, {len(still_remaining)} remaining for bulk check"
                )

            return cached, still_remaining

        except Exception as e:
            logger.debug(f"AllDebrid: StremThru lookup failed: {str(e)}")
            return {}, hashes

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
                logger.debug(
                    f"AllDebrid: Failed to delete magnet {magnet_id}: {str(e)}"
                )

        logger.debug(
            f"AllDebrid: Deleted {success}/{len(magnet_ids)} uploaded magnets after availability check"
        )

    async def _delete_magnets_background(self, magnet_ids, ip=None):
        """Progressively delete temp magnets added during availability check. Non-blocking."""
        logger.debug(f"AllDebrid: background cleanup of {len(magnet_ids)} temp magnet(s)")
        success = 0
        for magnet_id in magnet_ids:
            try:
                response = await self.delete_magnet(magnet_id, ip)
                if response and response.get("status") == "success":
                    success += 1
                    logger.trace(f"AllDebrid: Deleted temp magnet {magnet_id}")
            except Exception as e:
                logger.debug(f"AllDebrid: Could not delete temp magnet {magnet_id}: {e}")
        logger.debug(f"AllDebrid: background cleanup done — {success}/{len(magnet_ids)} deleted")

    async def add_magnet(self, magnet, ip=None):
        url = f"{self.base_url}magnet/upload?agent={self.agent}"
        data = {"magnets[]": magnet}
        return await self.json_response(
            url, method="post", headers=self.get_headers(), data=data
        )

    async def add_torrent(self, torrent_file, ip=None):
        url = f"{self.base_url}magnet/upload/file?agent={self.agent}"
        files = {
            "files[]": (
                str(uuid.uuid4()) + ".torrent",
                torrent_file,
                "application/x-bittorrent",
            )
        }
        return await self.json_response(
            url, method="post", headers=self.get_headers(), files=files
        )

    async def get_magnet_files(self, id, ip=None):
        url = f"{settings.ad_base_url}/v4/magnet/files"
        data = {"id[]": id, "agent": self.agent}
        return await self.json_response(
            url, method="post", headers=self.get_headers(), data=data
        )

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
        logger.debug(f"AllDebrid: Torrent ID: {torrent_id}")

        if not torrent_id or torrent_id.startswith("Error"):
            logger.error(f"AllDebrid: Failed to add torrent: {torrent_id}")
            return settings.no_cache_video_url

        logger.debug(f"AllDebrid: Retrieving files for torrent ID: {torrent_id}")
        try:
            files_response = await self.get_magnet_files(torrent_id, ip)
            logger.trace(f"AllDebrid: Files response: {files_response}")

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
            logger.debug(
                f"AllDebrid: Retrieved {len(files)} files (after flattening)"
            )

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
            logger.debug(
                f"AllDebrid: Finding S{numeric_season:02d}E{numeric_episode:02d}"
            )

            try:
                file_index = query.get("file_index")
                if file_index is not None:
                    try:
                        idx = int(file_index) - 1
                        if 0 <= idx < len(files):
                            indexed_file = files[idx]
                            if indexed_file.get("l"):
                                link = indexed_file["l"]
                                logger.info(
                                    f"AllDebrid: Found episode link using file_index={file_index}"
                                )
                    except (TypeError, ValueError):
                        logger.debug(f"AllDebrid: Invalid file_index={file_index}")

                if link == settings.no_cache_video_url:
                    matching_files = []
                    for file_info in files:
                        if not isinstance(file_info, dict):
                            continue

                        filename = file_info.get("n", "")
                        if not filename:
                            continue

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
                                logger.debug(
                                    f"AllDebrid: ✓ Match with pattern: {filename}"
                                )
                                matching_files.append(file_info)
                                break

                    if matching_files:
                        target_file = max(
                            matching_files, key=lambda x: x.get("s", 0)
                        )
                        if target_file.get("l"):
                            link = target_file["l"]
                            logger.debug(
                                "AllDebrid: Found episode link by filename match"
                            )
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

        logger.debug("AllDebrid: Retrieved link successfully")

        try:
            unlocked_response = await self.unrestrict_link(link, ip)
            if (
                unlocked_response
                and unlocked_response.get("status") == "success"
                and "data" in unlocked_response
            ):
                final_link = unlocked_response["data"].get("link", link)
                logger.debug("AllDebrid: Link unrestricted")
                return final_link
        except Exception as e:
            logger.debug(f"AllDebrid: Could not unrestrict link: {str(e)}")

        return link

    async def _check_availability_batch_direct(self, batch, ip=None):
        url = f"{self.base_url}magnet/upload?agent={self.agent}"
        data = {"magnets[]": batch}
        uploaded_ids = []
        result_magnets = []

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
                        self._build_result_item(
                            hash_value, instant=False, files=[], source="ad_direct"
                        )
                    )
                return result_magnets

            magnets_data = response.get("data", {}).get("magnets", [])
            logger.debug(
                f"AllDebrid: Direct batch returned {len(magnets_data)} magnets"
            )

            seen_hashes = set()

            for magnet_data in magnets_data:
                raw_hash_value = magnet_data.get("hash") or magnet_data.get("magnet")
                hash_value = self._normalize_hash_value(raw_hash_value)

                # Only `ready` is available in the /magnet/upload response.
                # `statusCode` only exists in /magnet/status — never use it here.
                ready = magnet_data.get("ready", False)

                if hash_value:
                    seen_hashes.add(hash_value)
                    result_magnets.append(
                        self._build_result_item(
                            hash_value,
                            instant=bool(ready),
                            files=magnet_data.get("files", []),
                            source="ad_direct",
                        )
                    )

            for hash_value in batch:
                if hash_value not in seen_hashes:
                    result_magnets.append(
                        self._build_result_item(
                            hash_value, instant=False, files=[], source="ad_direct"
                        )
                    )

            return result_magnets

        except Exception as e:
            logger.error(f"AllDebrid: Error checking availability batch: {str(e)}")
            for hash_value in batch:
                result_magnets.append(
                    self._build_result_item(
                        hash_value, instant=False, files=[], source="ad_direct"
                    )
                )
            return result_magnets

        finally:
            if uploaded_ids:
                asyncio.create_task(self._delete_magnets_background(uploaded_ids, ip))

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """
        AllDebrid availability check — called by get_availability_bulk_cached() in BaseDebrid.

        Strategy:
          1. StremThru community cache (positive results only, never cached locally —
             StremThru manages its own cache and must always be queried fresh).
          2. Direct AllDebrid bulk upload/check/delete for remaining hashes.

        Caching is handled by the BaseDebrid wrapper — do not add Redis logic here.
        """
        if not hashes_or_magnets:
            return {"status": "success", "data": {"magnets": []}}

        # Normalize and deduplicate inputs while preserving order
        cleaned_hashes = list(
            dict.fromkeys(
                h for h in (self._normalize_hash_value(v) for v in hashes_or_magnets) if h
            )
        )

        if not cleaned_hashes:
            return {"status": "success", "data": {"magnets": []}}

        result_by_hash = {}

        # Step 1: StremThru community cache (positive results only)
        if settings.stremthru_url:
            stremthru_cached, remaining_hashes = await self._lookup_stremthru(
                cleaned_hashes, ip
            )
            result_by_hash.update(stremthru_cached)
            # StremThru results are NOT stored in Redis — StremThru manages its own
            # community cache and must always be queried fresh for up-to-date data.
        else:
            remaining_hashes = cleaned_hashes

        # Step 2: direct AllDebrid bulk upload/check/delete for remaining hashes
        batch_size = 20
        direct_results = []

        for i in range(0, len(remaining_hashes), batch_size):
            batch = remaining_hashes[i : i + batch_size]
            batch_results = await self._check_availability_batch_direct(batch, ip)
            direct_results.extend(batch_results)

            # Small defensive pause between batches to stay within rate limits
            if i + batch_size < len(remaining_hashes):
                await asyncio.sleep(0.15)

        for item in direct_results:
            h = item.get("hash")
            if h:
                result_by_hash[h] = item

        # Fill in hashes absent from the API response as not-cached
        for h in cleaned_hashes:
            if h not in result_by_hash:
                result_by_hash[h] = self._build_result_item(
                    h, instant=False, files=[], source="fallback"
                )

        ordered_results = [result_by_hash[h] for h in cleaned_hashes]
        ready_count = sum(1 for item in ordered_results if item.get("instant"))
        logger.debug(
            f"AllDebrid: Cache check complete — {ready_count}/{len(ordered_results)} cached"
        )

        # Normalize to the format expected by _update_availability_alldebrid
        # in TorrentSmartContainer — only instantly available hashes are included
        # so that non-cached hashes are not incorrectly marked as "AD" available.
        return {
            "status": "success",
            "data": {
                "magnets": [
                    {"hash": item["hash"], "instant": item["instant"], "files": item.get("files", [])}
                    for item in ordered_results
                    if item.get("instant")
                ]
            },
        }

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
                logger.warning(
                    f"AllDebrid: Exception with cached .torrent: {str(e)}"
                )

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
                logger.warning(
                    f"AllDebrid: Exception downloading .torrent: {str(e)}"
                )

        logger.debug("AllDebrid: Adding magnet link")
        magnet_response = await self.add_magnet(magnet, ip)

        if not magnet_response or magnet_response.get("status") != "success":
            logger.error(f"AllDebrid: Failed to add magnet: {magnet_response}")
            return "Error: Failed to add magnet."

        try:
            magnets = magnet_response.get("data", {}).get("magnets", [])
            if magnets:
                torrent_id = magnets[0].get("id")
                logger.debug(
                    f"AllDebrid: Successfully added magnet, ID: {torrent_id}"
                )
                return str(torrent_id)
        except Exception as e:
            logger.error(f"AllDebrid: Exception extracting magnet ID: {str(e)}")

        return "Error: Could not extract torrent ID."

    async def start_background_caching(self, magnet, query=None):
        """Start caching a magnet/torrent in the background via AllDebrid."""
        logger.info("AllDebrid: Starting background caching for magnet")
        torrent_download = None
        if query and isinstance(query, dict):
            raw = query.get("torrent_download")
            if raw:
                from urllib.parse import unquote
                torrent_download = unquote(raw)
        try:
            torrent_id = await self.add_magnet_or_torrent(
                magnet, torrent_download=torrent_download
            )
            if torrent_id and not torrent_id.startswith("Error"):
                logger.info(f"AllDebrid: Background caching started, ID: {torrent_id}")
                return True
            logger.error(f"AllDebrid: Failed to start background caching: {torrent_id}")
            return False
        except Exception as e:
            logger.error(f"AllDebrid: start_background_caching error: {e}")
            return False