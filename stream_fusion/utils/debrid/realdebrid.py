import re
import asyncio
import aiohttp
from urllib.parse import unquote

from fastapi import HTTPException

from stream_fusion.services.rd_conn.token_manager import RDTokenManager
from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.general import (
    get_info_hash_from_magnet,
    is_video_file,
    season_episode_in_filename,
)
from stream_fusion.settings import settings
from stream_fusion.logging_config import logger


class RealDebrid(BaseDebrid):
    def __init__(self, config, session: aiohttp.ClientSession = None):
        super().__init__(config, session)
        self.base_url = f"{settings.rd_base_url}/{settings.rd_api_version}/"
        if not settings.rd_unique_account:
            self.token_manager = RDTokenManager(config)

    async def get_headers(self):
        if settings.rd_unique_account:
            if not settings.proxied_link:
                logger.warning(
                    "Real-Debrid: Unique account is enabled, but proxied link is disabled. This may lead to account ban."
                )
                logger.warning(
                    "Real-Debrid: Please enable proxied link in the settings."
                )
                raise HTTPException(
                    status_code=500, detail="Real-Debrid: Proxied link is disabled."
                )
            if settings.rd_token:
                return {"Authorization": f"Bearer {settings.rd_token}"}
            else:
                logger.warning(
                    "Real-Debrid: Unique account is enabled, but no token is provided. Please provide a token in the env."
                )
                raise HTTPException(
                    status_code=500, detail="Real-Debrid: Token is not provided."
                )
        else:
            return {"Authorization": f"Bearer {await self.token_manager.get_access_token()}"}

    async def add_magnet(self, magnet, ip=None):
        url = f"{self.base_url}torrents/addMagnet"
        data = {"magnet": magnet}
        logger.debug(f"Real-Debrid: Adding magnet: {magnet}")
        try:
            headers = await self.get_headers()
            return await self.json_response(
                url, method="post", headers=headers, data=data
            )
        except HTTPException as e:
            if e.status_code == 451:
                logger.error(f"Real-Debrid: Torrent banned (451): {magnet}")
                raise
            raise

    async def add_torrent(self, torrent_file):
        url = f"{self.base_url}torrents/addTorrent"
        try:
            headers = await self.get_headers()
            return await self.json_response(
                url, method="put", headers=headers, data=torrent_file
            )
        except HTTPException as e:
            if e.status_code == 451:
                logger.error("Real-Debrid: Torrent banned (451)")
                raise
            raise

    async def delete_torrent(self, id):
        url = f"{self.base_url}torrents/delete/{id}"
        headers = await self.get_headers()
        return await self.json_response(url, method="delete", headers=headers)

    async def get_torrent_info(self, torrent_id):
        logger.trace(f"Real-Debrid: Getting torrent info for ID: {torrent_id}")
        url = f"{self.base_url}torrents/info/{torrent_id}"
        headers = await self.get_headers()
        torrent_info = await self.json_response(url, headers=headers)
        if not torrent_info or "files" not in torrent_info:
            return None
        return torrent_info

    async def select_files(self, torrent_id, file_id):
        logger.debug(
            f"Real-Debrid: Selecting file(s): {file_id} for torrent ID: {torrent_id}"
        )
        await self._torrent_rate_limit()
        url = f"{self.base_url}torrents/selectFiles/{torrent_id}"
        data = {"files": str(file_id)}
        session = await self._get_session()
        timeout = aiohttp.ClientTimeout(total=30)
        headers = await self.get_headers()
        async with session.post(url, headers=headers, data=data, timeout=timeout) as response:
            pass  # Just need to make the request

    async def unrestrict_link(self, link):
        url = f"{self.base_url}unrestrict/link"
        data = {"link": link}
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                headers = await self.get_headers()
                response = await self.json_response(url, method="post", headers=headers, data=data)
                if response and "download" in response:
                    return response
                else:
                    logger.warning(f"Real-Debrid: Unexpected response when unrestricting link: {response}")
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Real-Debrid: Error unrestricting link (attempt {attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Real-Debrid: Failed to unrestrict link after {max_retries} attempts: {str(e)}")
                    raise

        return None

    async def is_already_added(self, magnet):
        hash = magnet.split("urn:btih:")[1].split("&")[0].lower()
        url = f"{self.base_url}torrents"
        headers = await self.get_headers()
        torrents = await self.json_response(url, headers=headers)
        for torrent in torrents:
            if torrent["hash"].lower() == hash:
                return torrent["id"]
        return False

    async def wait_for_link(self, torrent_id, timeout=60, interval=5):
        import time
        start_time = time.time()
        while time.time() - start_time < timeout:
            torrent_info = await self.get_torrent_info(torrent_id)
            if (
                torrent_info
                and "links" in torrent_info
                and len(torrent_info["links"]) > 0
            ):
                return torrent_info["links"]
            await asyncio.sleep(interval)
        return None

    # --- Provider overrides for the generic BaseDebrid cache (issue #77) ---

    @property
    def service_name(self) -> str:
        return "realdebrid"

    def _index_results_by_hash(self, response) -> dict:
        # RD returns either a StremThru list (cached) or an AD-style dict (stub)
        if isinstance(response, list):
            return {item["hash"]: item for item in response if item.get("hash")}
        if isinstance(response, dict):
            return {
                m["hash"]: m
                for m in response.get("data", {}).get("magnets", [])
                if m.get("hash")
            }
        return {}

    def _reconstruct_response(self, items: list):
        # Return list format so TorrentSmartContainer routes via __using_stremthru
        return items

    def _sanitize_for_cache(self, item: dict) -> dict:
        # Strip tokenized private links from StremThru responses before shared storage
        safe = {k: v for k, v in item.items() if k not in ("link", "url", "stream_link")}
        if "files" in safe:
            safe["files"] = [
                {k2: v2 for k2, v2 in f.items() if k2 in ("name", "index", "size")}
                for f in (safe["files"] or [])
            ]
        return safe

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        if not hashes_or_magnets:
            return []

        # Normalise inputs: accept both plain hashes and magnet URIs
        def _extract_hash(value):
            if re.match(r'^[a-fA-F0-9]{40}$', value):
                return value.lower()
            m = re.search(r'urn:btih:([a-fA-F0-9]{40})', value, re.IGNORECASE)
            return m.group(1).lower() if m else None

        hashes = list(dict.fromkeys(h for h in (_extract_hash(v) for v in hashes_or_magnets) if h))
        if not hashes:
            return []

        result_items = []
        remaining_hashes = hashes

        # Step 1: StremThru community cache (native RD /torrents/instantAvailability is dead)
        if settings.stremthru_url:
            try:
                token = settings.rd_token if settings.rd_unique_account else await self.token_manager.get_access_token()
                if token:
                    stremthru_results, remaining_hashes = await self._get_stremthru_community_cache(
                        hashes, "realdebrid", token, ip=ip
                    )
                    result_items.extend(stremthru_results)
                    logger.debug(f"Real-Debrid: StremThru found {len(stremthru_results)} cached hashes, {len(remaining_hashes)} remaining")
                else:
                    logger.warning("Real-Debrid: no token available for StremThru check")
            except Exception as e:
                logger.warning(f"Real-Debrid: StremThru cache check failed ({e})")

        # Step 2: Direct RD check via anonymous magnet add + /torrents for remaining hashes
        _MAX_DIRECT = 20
        to_check = remaining_hashes[:_MAX_DIRECT]
        skipped = remaining_hashes[_MAX_DIRECT:]
        if skipped:
            logger.warning(
                f"Real-Debrid: Skipping {len(skipped)} hashes (RD direct check limit is {_MAX_DIRECT})"
            )

        if to_check:
            direct_cached = await self._check_availability_direct(to_check)
            result_items.extend(direct_cached)

        logger.debug(f"Real-Debrid: availability check complete — {len(result_items)} cached hashes found")
        return result_items

    async def _check_availability_direct(self, hashes):
        """
        Check RD cache by adding anonymous magnets (no trackers) and inspecting /torrents.

        Status semantics for no-tracker magnets:
          magnet_conversion / waiting_files_selection / downloaded → CACHED
          451 infringing_file                                      → NOT cached (and nothing added to account)
          magnet_error / queued / dead / …                        → NOT cached

        Strategy:
          A. GET /torrents → mark already-present hashes as cached, skip adding them
          B. POST /torrents/addMagnet for each remaining hash (0.5 s between calls)
          C. GET /torrents → single status check for all newly added IDs
          D. Fire-and-forget deletion of added IDs (451s were never added, no cleanup needed)
        """
        if not hashes:
            return []

        _CACHED_STATUSES = frozenset(("magnet_conversion", "waiting_files_selection", "downloaded"))

        try:
            headers = await self.get_headers()
        except Exception as e:
            logger.warning(f"Real-Debrid: Could not get headers for direct check: {e}")
            return []

        # A — GET /torrents: mark hashes already in account, build remaining list
        cached_results = []
        remaining = list(hashes)
        try:
            existing_torrents = await self.json_response(f"{self.base_url}torrents", headers=headers)
            if isinstance(existing_torrents, list):
                existing_by_hash = {
                    (t.get("hash") or "").lower(): t
                    for t in existing_torrents if t.get("hash")
                }
                still_remaining = []
                for h in hashes:
                    t = existing_by_hash.get(h)
                    if t and t.get("status") in _CACHED_STATUSES:
                        cached_results.append({
                            "hash": h, "status": "cached", "files": [],
                            "store_name": "realdebrid", "debrid": "RD",
                        })
                    else:
                        still_remaining.append(h)
                remaining = still_remaining
        except Exception as e:
            logger.warning(f"Real-Debrid: Could not fetch existing torrents: {e}")

        if not remaining:
            logger.debug(f"Real-Debrid: direct check — {len(cached_results)}/{len(hashes)} cached")
            return cached_results

        # B — Add anonymous magnets for hashes not in account (safe: no pre-existing ID risk)
        new_adds = {}  # {hash: torrent_id}
        for i, h in enumerate(remaining):
            if i > 0:
                await asyncio.sleep(0.5)
            tid = await self._add_magnet_probe(h, headers)
            if tid:
                new_adds[h] = tid

        if not new_adds:
            logger.debug(f"Real-Debrid: direct check — {len(cached_results)}/{len(hashes)} cached")
            return cached_results

        # C — Single GET /torrents to check status of all newly added IDs
        try:
            all_torrents = await self.json_response(f"{self.base_url}torrents", headers=headers)
            torrents_by_id = {t.get("id"): t for t in (all_torrents or []) if isinstance(t, dict)}
            for h, tid in new_adds.items():
                torrent = torrents_by_id.get(tid)
                status = torrent.get("status") if torrent else None
                if status in _CACHED_STATUSES:
                    cached_results.append({
                        "hash": h, "status": "cached", "files": [],
                        "store_name": "realdebrid", "debrid": "RD",
                    })
                    logger.trace(f"Real-Debrid: {h} cached (status={status})")
                else:
                    logger.trace(f"Real-Debrid: {h} not cached (status={status})")
        except Exception as e:
            logger.error(f"Real-Debrid: Could not fetch torrents for status check: {e}")

        logger.debug(f"Real-Debrid: direct check — {len(cached_results)}/{len(hashes)} cached")

        # D — Background deletion of all added IDs (451s were never added, nothing to clean up)
        asyncio.create_task(self._delete_torrents_background(list(new_adds.values())))

        return cached_results

    async def _add_magnet_probe(self, info_hash: str, headers: dict):
        """
        Low-level addMagnet for availability probing — bypasses json_response to silence
        expected 451 (infringing_file) at TRACE level.
        Retries once on 429 with a 2 s back-off.
        Returns the torrent ID string on success, None otherwise.
        """
        await self._global_rate_limit()
        url = f"{self.base_url}torrents/addMagnet"
        session = await self._get_session()
        timeout = aiohttp.ClientTimeout(total=30)
        max_attempts = 2  # 1 retry on 429
        for attempt in range(max_attempts):
            try:
                async with session.post(
                    url,
                    headers=headers,
                    data={"magnet": f"magnet:?xt=urn:btih:{info_hash}"},
                    timeout=timeout,
                ) as response:
                    if response.status == 451:
                        logger.trace(f"Real-Debrid: {info_hash} not cached (451 infringing_file)")
                        return None
                    if response.status == 429:
                        logger.debug(f"Real-Debrid: probe rate limited for {info_hash} (attempt {attempt + 1}/{max_attempts})")
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(2)
                            continue
                        return None
                    if response.status in (200, 201):
                        data = await response.json()
                        return data.get("id") if isinstance(data, dict) else None
                    body = await response.text()
                    logger.warning(f"Real-Debrid: addMagnet probe unexpected {response.status} for {info_hash}: {body[:200]}")
                    return None
            except Exception as e:
                logger.warning(f"Real-Debrid: addMagnet probe failed for {info_hash}: {e}")
                return None
        return None

    async def _delete_torrents_background(self, torrent_ids):
        """Progressively delete temp torrents added during availability check. Non-blocking."""
        logger.debug(f"Real-Debrid: background cleanup of {len(torrent_ids)} temp torrent(s)")
        for torrent_id in torrent_ids:
            try:
                await self.delete_torrent(torrent_id)
                logger.debug(f"Real-Debrid: Deleted temp torrent {torrent_id}")
            except Exception as e:
                logger.debug(f"Real-Debrid: Could not delete temp torrent {torrent_id}: {e}")
            await asyncio.sleep(1)  # gentle on the RD API (~1 s/delete, max ~20 s for 20)

    async def get_stream_link(self, query, config=None, ip=None):
        # Extract query parameters
        magnet = query["magnet"]
        stream_type = query["type"]
        file_index = int(query["file_index"]) if query["file_index"] is not None else None
        season = query["season"]
        episode = query["episode"]
        info_hash = get_info_hash_from_magnet(magnet)

        logger.debug(f"Real-Debrid: Getting stream link for {stream_type} with hash: {info_hash}")

        # Check for cached torrents
        cached_torrent_ids = await self._get_cached_torrent_ids(info_hash)
        logger.debug(f"Real-Debrid: Found {len(cached_torrent_ids)} cached torrents with hash: {info_hash}")

        torrent_id = None
        if cached_torrent_ids:
            torrent_info = await self._get_cached_torrent_info(cached_torrent_ids, file_index, season, episode, stream_type)
            if torrent_info:
                torrent_id = torrent_info["id"]
                logger.debug(f"Real-Debrid: Found cached torrent with ID: {torrent_id}")

        # If the torrent is not in cache, add it
        if torrent_id is None:
            torrent_id = await self.add_magnet_or_torrent_and_select(query, ip)
            if not torrent_id:
                logger.error("Real-Debrid: Failed to add or find torrent.")
                raise HTTPException(status_code=500, detail="Real-Debrid: Failed to add or find torrent.")

        logger.debug(f"Real-Debrid: Waiting for link(s) to be ready for torrent ID: {torrent_id}")
        links = await self.wait_for_link(torrent_id, timeout=20)
        if links is None:
            logger.warning("Real-Debrid: No links available after waiting. Returning NO_CACHE_VIDEO_URL.")
            return settings.no_cache_video_url

        # Refresh torrent info to ensure we have the latest data
        torrent_info = await self.get_torrent_info(torrent_id)

        # Select the appropriate link
        if len(links) > 1:
            logger.debug("Real-Debrid: Finding appropriate link")
            download_link = await self._find_appropriate_link(torrent_info, links, file_index, season, episode)
        else:
            download_link = links[0]

        # Unrestrict the link
        logger.debug(f"Real-Debrid: Unrestricting the download link: {download_link}")
        unrestrict_response = await self.unrestrict_link(download_link)
        if not unrestrict_response or "download" not in unrestrict_response:
            logger.error("Real-Debrid: Failed to unrestrict link.")
            return None

        logger.debug(f"Real-Debrid: Got download link: {unrestrict_response['download']}")
        return unrestrict_response["download"]

    async def _get_cached_torrent_ids(self, info_hash):
        await self._torrent_rate_limit()
        url = f"{self.base_url}torrents"
        headers = await self.get_headers()
        torrents = await self.json_response(url, headers=headers)

        logger.debug(f"Real-Debrid: Searching user's downloads for hash: {info_hash}")
        torrent_ids = [
            torrent["id"]
            for torrent in torrents
            if torrent["hash"].lower() == info_hash
        ]
        return torrent_ids

    async def _get_cached_torrent_info(
        self, cached_ids, file_index, season, episode, stream_type
    ):
        for cached_torrent_id in cached_ids:
            cached_torrent_info = await self.get_torrent_info(cached_torrent_id)
            if self._torrent_contains_file(
                cached_torrent_info, file_index, season, episode, stream_type
            ):
                return cached_torrent_info
        return None

    def _torrent_contains_file(
        self, torrent_info, file_index, season, episode, stream_type
    ):
        if not torrent_info or "files" not in torrent_info:
            return False

        if stream_type == "movie":
            return any(file["selected"] for file in torrent_info["files"])
        elif stream_type == "series":
            if file_index is not None:
                return any(
                    file["id"] == file_index and file["selected"]
                    for file in torrent_info["files"]
                )
            else:
                return any(
                    file["selected"]
                    and season_episode_in_filename(file["path"], season, episode)
                    for file in torrent_info["files"]
                )
        return False

    async def add_magnet_or_torrent(self, magnet, torrent_download=None, ip=None):
        if torrent_download is None:
            logger.debug("Real-Debrid: Adding magnet")
            magnet_response = await self.add_magnet(magnet)
            logger.debug(f"Real-Debrid: Add magnet response: {magnet_response}")

            if not magnet_response or "id" not in magnet_response:
                logger.error("Real-Debrid: Failed to add magnet.")
                raise HTTPException(
                    status_code=451, detail="Real-Debrid: Torrent banned or unavailable for legal reasons."
                )

            torrent_id = magnet_response["id"]
        else:
            logger.debug("Real-Debrid: Downloading and adding torrent file")
            torrent_file = await self.download_torrent_file(torrent_download)
            upload_response = await self.add_torrent(torrent_file)
            logger.debug(f"Real-Debrid: Add torrent file response: {upload_response}")

            if not upload_response or "id" not in upload_response:
                logger.error("Real-Debrid: Failed to add torrent file.")
                raise HTTPException(
                    status_code=451, detail="Real-Debrid: Torrent banned or unavailable for legal reasons."
                )

            torrent_id = upload_response["id"]

        logger.debug(f"Real-Debrid: New torrent added with ID: {torrent_id}")
        return await self.get_torrent_info(torrent_id)

    async def add_magnet_or_torrent_and_select(self, query, ip=None):
        magnet = query['magnet']
        torrent_download = unquote(query["torrent_download"]) if query["torrent_download"] is not None else None
        stream_type = query['type']
        file_index = int(query["file_index"]) if query["file_index"] is not None else None
        season = query["season"]
        episode = query["episode"]

        torrent_info = await self.add_magnet_or_torrent(magnet, torrent_download, ip)
        if not torrent_info or "files" not in torrent_info:
            logger.error("Real-Debrid: Failed to add or find torrent.")
            return None

        is_season_pack = stream_type == "series" and len(torrent_info["files"]) > 5

        if is_season_pack:
            logger.debug("Real-Debrid: Processing season pack")
            await self._process_season_pack(torrent_info)
        else:
            logger.debug("Real-Debrid: Selecting specific file")
            await self._select_file(
                torrent_info, stream_type, file_index, season, episode
            )

        logger.debug(f"Real-Debrid: Added magnet or torrent to download service: {magnet[:50]}")
        return torrent_info['id']

    async def _process_season_pack(self, torrent_info):
        logger.debug("Real-Debrid: Processing season pack files")
        video_file_indexes = [
            str(file["id"])
            for file in torrent_info["files"]
            if is_video_file(file["path"])
        ]

        if video_file_indexes:
            await self.select_files(torrent_info["id"], ",".join(video_file_indexes))
            logger.debug(
                f"Real-Debrid: Selected {len(video_file_indexes)} video files from season pack"
            )
            await asyncio.sleep(10)
        else:
            logger.warning("Real-Debrid: No video files found in the season pack")

    async def _select_file(self, torrent_info, stream_type, file_index, season, episode):
        torrent_id = torrent_info["id"]
        if file_index is not None:
            logger.debug(f"Real-Debrid: Selecting file_index: {file_index}")
            await self.select_files(torrent_id, file_index)
            return

        files = torrent_info["files"]
        if stream_type == "movie":
            largest_file_id = max(files, key=lambda x: x["bytes"])["id"]
            logger.debug(f"Real-Debrid: Selecting largest file_index: {largest_file_id}")
            await self.select_files(torrent_id, largest_file_id)
        elif stream_type == "series":
            matching_files = [
                file
                for file in files
                if season_episode_in_filename(file["path"], season, episode)
            ]
            if matching_files:
                largest_file_id = max(matching_files, key=lambda x: x["bytes"])["id"]
                logger.debug(
                    f"Real-Debrid: Selecting largest matching file_index: {largest_file_id}"
                )
                await self.select_files(torrent_id, largest_file_id)
            else:
                logger.warning(
                    "Real-Debrid: No matching files found for the specified episode"
                )

    async def _find_appropriate_link(self, torrent_info, links, file_index, season, episode):
        # Refresh torrent info to get the latest selected files
        torrent_info = await self.get_torrent_info(torrent_info["id"])
        selected_files = [file for file in torrent_info["files"] if file["selected"] == 1]

        logger.debug(f"Real-Debrid: Finding appropriate link. Selected files: {len(selected_files)}, Available links: {len(links)}")

        if not selected_files:
            logger.warning("Real-Debrid: No files were selected. Selecting the largest file.")
            largest_file = max(torrent_info["files"], key=lambda x: x['bytes'])
            selected_files = [largest_file]

        if file_index is not None:
            index = next((i for i, file in enumerate(selected_files) if file["id"] == file_index), None)
        else:
            matching_indexes = [
                {"index": i, "file": file}
                for i, file in enumerate(selected_files)
                if season_episode_in_filename(file["path"], season, episode)
            ]
            if matching_indexes:
                index = max(matching_indexes, key=lambda x: x["file"]["bytes"])["index"]
            else:
                logger.warning("Real-Debrid: No matching episode found. Selecting the largest file.")
                index = max(range(len(selected_files)), key=lambda i: selected_files[i]['bytes'])

        if index is None or index >= len(links):
            logger.warning("Real-Debrid: Appropriate link not found. Falling back to the first available link.")
            return links[0] if links else settings.no_cache_video_url

        logger.debug(f"Real-Debrid: Selected link index: {index}")
        return links[index]
