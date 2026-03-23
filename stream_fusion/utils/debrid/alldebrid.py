# alldebrid.py - AllDebrid v4.1 API (using magnet/status for files)
import asyncio
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
        # AllDebrid allows 12 req/s — increase global limit from default 250/60 to 600/60 (10 req/s)
        self.global_limit = 600
        self.global_period = 60

    def get_headers(self):
        if settings.ad_unique_account:
            if not settings.proxied_link:
                logger.warning("AllDebrid: Unique account enabled, but proxied link is disabled. This may lead to account ban.")
                logger.warning("AllDebrid: Please enable proxied link in the settings.")
                raise HTTPException(status_code=500, detail="Proxied link is disabled.")
            if settings.ad_token:
                return {"Authorization": f"Bearer {settings.ad_token}"}
            else:
                logger.warning("AllDebrid: Unique account enabled, but no token provided. Please provide a token in the env.")
                raise HTTPException(status_code=500, detail="AllDebrid token is not provided.")
        else:
            return {"Authorization": f"Bearer {self.config.get('ADToken')}"}

    async def add_magnet(self, magnet, ip=None):
        url = f"{self.base_url}magnet/upload?agent={self.agent}"
        data = {"magnets[]": magnet}
        return await self.json_response(url, method='post', headers=self.get_headers(), data=data)

    async def add_torrent(self, torrent_file, ip=None):
        url = f"{self.base_url}magnet/upload/file?agent={self.agent}"
        files = {"files[]": (str(uuid.uuid4()) + ".torrent", torrent_file, 'application/x-bittorrent')}
        return await self.json_response(url, method='post', headers=self.get_headers(), files=files)

    async def get_magnet_files(self, id, ip=None):
        """Get files from magnet using v4 API"""
        url = f"{settings.ad_base_url}/v4/magnet/files"
        data = {"id[]": id, "agent": self.agent}
        return await self.json_response(url, method='post', headers=self.get_headers(), data=data)

    async def unrestrict_link(self, link, ip=None):
        url = f"{self.base_url}link/unlock?agent={self.agent}&link={link}"
        return await self.json_response(url, method='get', headers=self.get_headers())

    async def get_stream_link(self, query, config=None, ip=None):
        magnet = query['magnet']
        stream_type = query['type']
        torrent_download = unquote(query["torrent_download"]) if query["torrent_download"] is not None else None
        torrent_file_content = query.get("torrent_file_content", None)

        # Add magnet or torrent to AllDebrid
        torrent_id = await self.add_magnet_or_torrent(magnet, torrent_download, torrent_file_content, ip)
        torrent_id = str(torrent_id) if torrent_id else ""
        logger.info(f"AllDebrid: Torrent ID: {torrent_id}")

        if not torrent_id or torrent_id.startswith("Error"):
            logger.error(f"AllDebrid: Failed to add torrent: {torrent_id}")
            return settings.no_cache_video_url

        # Get files from magnet using v4 API
        logger.info(f"AllDebrid: Retrieving files for torrent ID: {torrent_id}")
        try:
            files_response = await self.get_magnet_files(torrent_id, ip)
            logger.debug(f"AllDebrid: Files response: {files_response}")

            if not files_response:
                logger.error("AllDebrid: Null response from get_magnet_files")
                return settings.no_cache_video_url

            if files_response.get("status") != "success":
                logger.warning(f"AllDebrid: get_magnet_files returned error: {files_response.get('error')}")
                return settings.no_cache_video_url

            if "data" not in files_response:
                logger.error("AllDebrid: No data in files response")
                return settings.no_cache_video_url

            magnets = files_response["data"].get("magnets", [])
            if not magnets or len(magnets) == 0:
                logger.error("AllDebrid: No magnet data in files response")
                return settings.no_cache_video_url

            magnet_data = magnets[0]
            files = magnet_data.get("files", [])

            # Flatten nested file structure
            files = flatten_files(files)
            logger.info(f"AllDebrid: Retrieved {len(files)} files (after flattening)")

        except Exception as e:
            logger.error(f"AllDebrid: Error getting magnet files: {str(e)}")
            import traceback
            logger.debug(f"AllDebrid: Traceback: {traceback.format_exc()}")
            return settings.no_cache_video_url

        link = settings.no_cache_video_url

        if stream_type == "movie":
            logger.info("AllDebrid: Finding largest file for movie")
            try:
                largest_file = max(files, key=lambda x: x.get("s", 0)) if files else None

                if largest_file and "l" in largest_file:
                    link = largest_file["l"]
                    logger.info(f"AllDebrid: Found movie link")
                else:
                    logger.error("AllDebrid: No valid link found for movie")
            except Exception as e:
                logger.error(f"AllDebrid: Error processing movie: {str(e)}")

        elif stream_type == "series":
            numeric_season = int(query['season'].replace("S", ""))
            numeric_episode = int(query['episode'].replace("E", ""))
            logger.info(f"AllDebrid: Finding S{numeric_season:02d}E{numeric_episode:02d}")

            try:
                matching_files = []
                for file_info in files:
                    if isinstance(file_info, dict):
                        filename = file_info.get("n", "")

                        if season_episode_in_filename(filename, numeric_season, numeric_episode):
                            logger.debug(f"AllDebrid: ✓ Match: {filename}")
                            matching_files.append(file_info)
                        else:
                            import re
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
                    if "l" in target_file:
                        link = target_file["l"]
                        logger.info(f"AllDebrid: Found episode link")
                    else:
                        logger.error("AllDebrid: Matching file has no link")
                else:
                    logger.warning(f"AllDebrid: No files found for S{numeric_season:02d}E{numeric_episode:02d}")

            except Exception as e:
                logger.error(f"AllDebrid: Error processing series: {str(e)}")

        else:
            logger.error("AllDebrid: Unsupported stream type.")
            raise HTTPException(status_code=500, detail="Unsupported stream type.")

        if link == settings.no_cache_video_url:
            logger.info("AllDebrid: No link found, returning no-cache URL")
            return link

        logger.info(f"AllDebrid: Retrieved link successfully")

        # Try to unrestrict the link
        try:
            unlocked_response = await self.unrestrict_link(link, ip)
            if unlocked_response and unlocked_response.get("status") == "success" and "data" in unlocked_response:
                final_link = unlocked_response["data"].get("link", link)
                logger.info(f"AllDebrid: Link unrestricted")
                return final_link
        except Exception as e:
            logger.debug(f"AllDebrid: Could not unrestrict link: {str(e)}")

        return link

    # Redis TTLs for shared availability cache
    _CACHE_TTL_CACHED     = 7 * 24 * 3600   # 7 days  — cached torrents stay available a long time
    _CACHE_TTL_NOT_CACHED = 1 * 3600         # 1 hour  — may become available soon

    @staticmethod
    def _ad_cache_key(hash_value: str) -> str:
        return f"debrid:availability:ad:{hash_value}"

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        if not hashes_or_magnets:
            return {"status": "success", "data": {"magnets": []}}

        # Normalise inputs: accept both plain hashes and magnet URIs
        import re as _re
        def _extract_hash(value):
            if _re.match(r'^[a-fA-F0-9]{40}$', value):
                return value.lower()
            m = _re.search(r'urn:btih:([a-fA-F0-9]{40})', value, _re.IGNORECASE)
            return m.group(1).lower() if m else None

        hashes = [h for h in (_extract_hash(v) for v in hashes_or_magnets) if h]
        if not hashes:
            logger.warning("AllDebrid: No valid hashes found in input")
            return {"status": "success", "data": {"magnets": []}}

        results = {}   # hash → {"hash": h, "instant": bool, "files": []}

        # --- Step 0: Shared Redis cache lookup (one mget for all hashes) ---
        # Safety: only True/False values explicitly set by this method are trusted.
        # A missing key simply means "unknown" — never treated as False.
        to_check = list(hashes)
        try:
            from stream_fusion.utils.cache.local_redis import RedisCache
            import jsonpickle as _jp
            redis = RedisCache(self.config)
            redis_client = await redis.get_redis_client()
            if redis_client:
                keys = [self._ad_cache_key(h) for h in hashes]
                raw_values = await redis_client.mget(keys)
                for h, raw in zip(hashes, raw_values):
                    if raw is not None:
                        try:
                            cached_val = _jp.decode(raw)
                            results[h] = {"hash": h, "instant": bool(cached_val), "files": []}
                        except Exception:
                            pass  # corrupted entry — treat as unknown
                to_check = [h for h in hashes if h not in results]
                redis_hits = len(hashes) - len(to_check)
                if redis_hits:
                    logger.info(f"AllDebrid: Redis cache hit for {redis_hits}/{len(hashes)} hashes")
        except Exception as e:
            logger.warning(f"AllDebrid: Redis cache lookup failed ({e}), continuing without cache")
            redis_client = None

        unchecked_hashes = list(to_check)

        # --- Step 1: StremThru (community cache) if configured ---
        if unchecked_hashes and settings.stremthru_url:
            try:
                from stream_fusion.utils.debrid.stremthru import StremThru
                token = settings.ad_token if settings.ad_unique_account else self.config.get("ADToken")
                if token:
                    session = await self._get_session()
                    st = StremThru(self.config, session=session)
                    st.set_store_credentials("alldebrid", token)
                    st_results = await st.get_availability_bulk(unchecked_hashes, ip)
                    st_found = []
                    for item in st_results:
                        h = item.get("hash", "").lower()
                        if h:
                            results[h] = {"hash": h, "instant": True, "files": item.get("files", [])}
                            st_found.append(h)
                    unchecked_hashes = [h for h in unchecked_hashes if h not in results]
                    logger.info(
                        f"AllDebrid: StremThru found {len(st_found)} cached, "
                        f"{len(unchecked_hashes)} remaining for bulk check"
                    )
                    # StremThru results are NOT stored in Redis — StremThru manages its own
                    # community cache and keeps it up to date; we always call it fresh.
                else:
                    logger.warning("AllDebrid: no token for StremThru, falling back to bulk check for all hashes")
            except Exception as e:
                logger.warning(f"AllDebrid: StremThru failed ({e}), bulk-checking all hashes")

        # --- Step 2: Bulk upload/delete for unchecked hashes ---
        # Raw hashes accepted natively by the AD API — shorter and unambiguous vs magnet URIs
        # Batch of 20 stays safely under the 30-slot limit
        # SAFETY RULE: only cache False when the API returned status:success AND the hash
        # was present in the response — never cache False from errors or exceptions.
        batch_size = 20
        for i in range(0, len(unchecked_hashes), batch_size):
            batch = unchecked_hashes[i:i + batch_size]
            ids_to_delete = []
            api_confirmed = {}   # hash → bool, only filled from valid API responses
            try:
                upload_resp = await self.json_response(
                    f"{self.base_url}magnet/upload?agent={self.agent}",
                    method='post',
                    headers=self.get_headers(),
                    data={"magnets[]": batch},
                )
                if not upload_resp or upload_resp.get("status") != "success":
                    error_code = (upload_resp or {}).get("error", {}).get("code", "UNKNOWN")
                    if error_code == "MAGNET_TOO_MANY_ACTIVE":
                        logger.warning("AllDebrid: Too many active magnets (30 slot limit), skipping batch")
                    else:
                        logger.error(f"AllDebrid: Bulk upload error ({error_code}): {upload_resp}")
                    # API failed — do NOT cache anything, mark locally as False only
                    for h in batch:
                        results[h] = {"hash": h, "instant": False, "files": []}
                    continue

                seen_hashes = set()
                for m in upload_resp.get("data", {}).get("magnets", []):
                    mh = (m.get("hash") or "").lower()
                    if not mh:
                        continue
                    if m.get("id"):
                        ids_to_delete.append(m["id"])
                    if "error" in m:
                        # Per-magnet error (e.g. invalid hash) — confirmed False, safe to cache
                        results[mh] = {"hash": mh, "instant": False, "files": []}
                        api_confirmed[mh] = False
                    else:
                        # statusCode 4 = "Ready" — safety net when ready field is missing/False
                        is_ready = bool(m.get("ready", False)) or m.get("statusCode") == 4
                        results[mh] = {"hash": mh, "instant": is_ready, "files": []}
                        api_confirmed[mh] = is_ready
                    seen_hashes.add(mh)

                # Hashes absent from the response — unknown, do NOT cache
                for h in batch:
                    if h not in seen_hashes:
                        results[h] = {"hash": h, "instant": False, "files": []}

                # Store API-confirmed results in Redis
                if redis_client and api_confirmed:
                    try:
                        pipe = redis_client.pipeline()
                        for h, is_cached in api_confirmed.items():
                            ttl = self._CACHE_TTL_CACHED if is_cached else self._CACHE_TTL_NOT_CACHED
                            pipe.set(self._ad_cache_key(h), _jp.encode(is_cached), ex=ttl)
                        await pipe.execute()
                        logger.debug(
                            f"AllDebrid: Stored {len(api_confirmed)} results in Redis "
                            f"({sum(api_confirmed.values())} cached, "
                            f"{len(api_confirmed) - sum(api_confirmed.values())} not cached)"
                        )
                    except Exception as e:
                        logger.debug(f"AllDebrid: Redis store (bulk) failed: {e}")

            except Exception as e:
                logger.error(f"AllDebrid: Bulk check batch failed: {e}")
                # Exception — do NOT cache anything
                for h in batch:
                    results[h] = {"hash": h, "instant": False, "files": []}
            finally:
                # Always delete uploaded magnets — even if an exception occurred after upload
                if ids_to_delete:
                    await asyncio.gather(*[
                        self.json_response(
                            f"{self.base_url}magnet/delete?agent={self.agent}",
                            method='post',
                            headers=self.get_headers(),
                            data={"id": mid},
                        )
                        for mid in ids_to_delete
                    ], return_exceptions=True)
                    logger.debug(f"AllDebrid: Deleted {len(ids_to_delete)} temporary magnets")

        # Only return cached items — _update_availability_alldebrid marks every item in the
        # response as "AD" available without checking the `instant` field, so non-cached
        # hashes must be excluded (they stay with availability=False in TorrentSmartContainer)
        cached_magnets = [v for v in results.values() if v.get("instant")]
        logger.info(f"AllDebrid: Cache check complete — {len(cached_magnets)}/{len(hashes)} cached")
        return {"status": "success", "data": {"magnets": cached_magnets}}

    async def add_magnet_or_torrent(self, magnet, torrent_download=None, torrent_file_content=None, ip=None):
        logger.debug(f"AllDebrid: Adding magnet or torrent")
        torrent_id = ""

        # PRIORITE 1: Use cached .torrent file
        if torrent_file_content is not None:
            logger.info(f"AllDebrid: Attempting to add cached .torrent file")
            try:
                upload_response = await self.add_torrent(torrent_file_content, ip)
                if upload_response and upload_response.get("status") == "success":
                    files = upload_response.get("data", {}).get("files", [])
                    if files and len(files) > 0:
                        torrent_id = files[0].get("id")
                        if torrent_id:
                            logger.info(f"AllDebrid: Successfully added cached .torrent, ID: {torrent_id}")
                            return str(torrent_id)
                logger.warning(f"AllDebrid: Failed to add cached .torrent")
            except Exception as e:
                logger.warning(f"AllDebrid: Exception with cached .torrent: {str(e)}")

        # PRIORITE 2: Download and add .torrent file
        if torrent_download is not None:
            logger.info(f"AllDebrid: Downloading and adding .torrent file")
            try:
                torrent_file = await self.download_torrent_file(torrent_download)
                upload_response = await self.add_torrent(torrent_file, ip)

                if upload_response and upload_response.get("status") == "success":
                    files = upload_response.get("data", {}).get("files", [])
                    if files and len(files) > 0:
                        torrent_id = files[0].get("id")
                        if torrent_id:
                            logger.info(f"AllDebrid: Successfully added downloaded .torrent, ID: {torrent_id}")
                            return str(torrent_id)
                logger.warning(f"AllDebrid: Failed to add downloaded .torrent, falling back to magnet")
            except Exception as e:
                logger.warning(f"AllDebrid: Exception downloading .torrent: {str(e)}")

        # PRIORITE 3: Fall back to magnet
        logger.info(f"AllDebrid: Adding magnet link")
        magnet_response = await self.add_magnet(magnet, ip)

        if not magnet_response or magnet_response.get("status") != "success":
            logger.error(f"AllDebrid: Failed to add magnet: {magnet_response}")
            return "Error: Failed to add magnet."

        try:
            magnets = magnet_response.get("data", {}).get("magnets", [])
            if magnets and len(magnets) > 0:
                torrent_id = magnets[0].get("id")
                logger.info(f"AllDebrid: Successfully added magnet, ID: {torrent_id}")
                return str(torrent_id)
        except Exception as e:
            logger.error(f"AllDebrid: Exception extracting magnet ID: {str(e)}")

        return "Error: Could not extract torrent ID."
