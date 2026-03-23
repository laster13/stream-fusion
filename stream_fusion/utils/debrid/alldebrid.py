import re
import uuid
import aiohttp
import asyncio
from urllib.parse import unquote

from fastapi import HTTPException

from stream_fusion.utils.debrid.base_debrid import BaseDebrid
from stream_fusion.utils.debrid.stremthru import StremThru
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
        # Aligné avec l'approche précédente: plus permissif sur le bulk check
        self.rate_limit_limit = 600
        self.rate_limit_interval = 60

        # Cache local léger de disponibilité
        self.instant_cache_ttl = getattr(settings, "ad_instant_cache_ttl", 6 * 3600)
        self.not_instant_cache_ttl = getattr(
            settings, "ad_not_instant_cache_ttl", 30 * 60
        )

        # Accélération optionnelle via StremThru
        self.enable_stremthru_prefetch = bool(
            self.config.get("debrid", False)
            and (
                self.config.get("stremthru")
                or self.config.get("ADToken")
            )
        )

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

    async def _cache_get_json(self, key):
        cache = getattr(self, "redis_cache", None) or getattr(settings, "redis_cache", None)
        if cache is None:
            return None
        try:
            return await cache.get(key)
        except Exception as e:
            logger.debug(f"AllDebrid: Cache get failed for {key}: {str(e)}")
            return None

    async def _cache_set_json(self, key, value, expiration):
        cache = getattr(self, "redis_cache", None) or getattr(settings, "redis_cache", None)
        if cache is None:
            return
        try:
            await cache.set(key, value, expiration=expiration)
        except Exception as e:
            logger.debug(f"AllDebrid: Cache set failed for {key}: {str(e)}")

    def _instant_cache_key(self, hash_value):
        return f"alldebrid:instant:{hash_value}"

    def _not_instant_cache_key(self, hash_value):
        return f"alldebrid:not_instant:{hash_value}"

    async def _lookup_local_cache(self, hashes):
        cached_results = {}
        remaining = []

        for hash_value in hashes:
            instant_key = self._instant_cache_key(hash_value)
            not_instant_key = self._not_instant_cache_key(hash_value)

            cached_instant = await self._cache_get_json(instant_key)
            if cached_instant:
                cached_results[hash_value] = self._build_result_item(
                    hash_value,
                    instant=True,
                    files=cached_instant.get("files", []),
                    source="local_cache",
                )
                continue

            cached_not_instant = await self._cache_get_json(not_instant_key)
            if cached_not_instant:
                cached_results[hash_value] = self._build_result_item(
                    hash_value,
                    instant=False,
                    files=[],
                    source="local_cache",
                )
                continue

            remaining.append(hash_value)

        if cached_results:
            logger.info(
                f"AllDebrid: Local cache found {sum(1 for x in cached_results.values() if x.get('instant'))} instant, "
                f"{sum(1 for x in cached_results.values() if not x.get('instant'))} non-instant"
            )

        return cached_results, remaining

    async def _write_local_cache(self, result_items):
        for item in result_items:
            hash_value = item.get("hash")
            if not hash_value:
                continue

            if item.get("instant") is True:
                await self._cache_set_json(
                    self._instant_cache_key(hash_value),
                    {"files": item.get("files", [])},
                    expiration=self.instant_cache_ttl,
                )
            else:
                await self._cache_set_json(
                    self._not_instant_cache_key(hash_value),
                    {"instant": False},
                    expiration=self.not_instant_cache_ttl,
                )

    async def _lookup_stremthru(self, hashes, ip=None):
        if not hashes:
            return {}, []

        try:
            stremthru = StremThru(self.config, self.session)
            response = await stremthru.get_availability_bulk(hashes, ip)
            if not isinstance(response, list):
                return {}, hashes

            cached = {}
            remaining = []

            for hash_value in hashes:
                remaining.append(hash_value)

            result_by_hash = {}
            for item in response:
                if not isinstance(item, dict):
                    continue
                hash_value = self._normalize_hash_value(item.get("hash"))
                if not hash_value:
                    continue
                result_by_hash[hash_value] = item

            still_remaining = []
            for hash_value in hashes:
                item = result_by_hash.get(hash_value)
                if not item:
                    still_remaining.append(hash_value)
                    continue

                status = str(item.get("status", "")).lower()
                files = item.get("files", [])
                debrid_code = str(item.get("debrid", "")).upper()

                # Accélération seulement sur le positif
                if status == "cached" and debrid_code in ("AD", "ALLDEBRID"):
                    cached[hash_value] = self._build_result_item(
                        hash_value,
                        instant=True,
                        files=files,
                        source="stremthru",
                    )
                else:
                    still_remaining.append(hash_value)

            if cached:
                logger.info(
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

        logger.info(
            f"AllDebrid: Deleted {success}/{len(magnet_ids)} uploaded magnets after availability check"
        )

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
            logger.info(
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
            logger.info(
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
                            logger.info(
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
            logger.info(
                f"AllDebrid: Direct batch returned {len(magnets_data)} magnets"
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
            try:
                await self.delete_magnets(uploaded_ids, ip)
            except Exception as e:
                logger.debug(
                    f"AllDebrid: Cleanup failed after availability check: {str(e)}"
                )

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """
        Philosophie gardée:
        - AllDebrid est la source de vérité finale
        - cache local = accélération
        - StremThru = accélération positive uniquement
        - fallback final = upload direct AllDebrid
        """
        if len(hashes_or_magnets) == 0:
            logger.info("AllDebrid: No hashes to check")
            return {"status": "success", "data": {"magnets": []}}

        cleaned_hashes = []
        for value in hashes_or_magnets:
            normalized = self._normalize_hash_value(value)
            if normalized:
                cleaned_hashes.append(normalized)

        # unique en gardant l'ordre
        cleaned_hashes = list(dict.fromkeys(cleaned_hashes))

        if not cleaned_hashes:
            logger.info("AllDebrid: No valid hashes to check")
            return {"status": "success", "data": {"magnets": []}}

        result_by_hash = {}

        # 1) Cache local
        local_cached, remaining_hashes = await self._lookup_local_cache(cleaned_hashes)
        result_by_hash.update(local_cached)

        # 2) StremThru positif seulement
        if remaining_hashes and self.enable_stremthru_prefetch:
            stremthru_cached, remaining_hashes = await self._lookup_stremthru(
                remaining_hashes, ip
            )
            result_by_hash.update(stremthru_cached)

            # écrire le positif StremThru dans le cache local
            await self._write_local_cache(list(stremthru_cached.values()))

        # 3) Check direct AllDebrid pour le reste
        batch_size = 20
        direct_results = []

        for i in range(0, len(remaining_hashes), batch_size):
            batch = remaining_hashes[i : i + batch_size]
            batch_results = await self._check_availability_batch_direct(batch, ip)
            direct_results.extend(batch_results)

            # mini pause défensive entre batches
            if i + batch_size < len(remaining_hashes):
                await asyncio.sleep(0.15)

        for item in direct_results:
            hash_value = item.get("hash")
            if hash_value:
                result_by_hash[hash_value] = item

        # 4) Compléter les manquants
        for hash_value in cleaned_hashes:
            if hash_value not in result_by_hash:
                result_by_hash[hash_value] = self._build_result_item(
                    hash_value, instant=False, files=[], source="fallback"
                )

        ordered_results = [result_by_hash[h] for h in cleaned_hashes]

        # 5) Écriture du cache local
        await self._write_local_cache(ordered_results)

        ready_count = sum(1 for item in ordered_results if item.get("instant") is True)
        logger.info(
            f"AllDebrid: Cache check complete — {ready_count}/{len(ordered_results)} cached"
        )

        # Format compatible avec le reste du pipeline
        final_magnets = [
            {
                "hash": item["hash"],
                "instant": item["instant"],
                "files": item.get("files", []),
            }
            for item in ordered_results
        ]

        return {"status": "success", "data": {"magnets": final_magnets}}

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

        logger.info("AllDebrid: Adding magnet link")
        magnet_response = await self.add_magnet(magnet, ip)

        if not magnet_response or magnet_response.get("status") != "success":
            logger.error(f"AllDebrid: Failed to add magnet: {magnet_response}")
            return "Error: Failed to add magnet."

        try:
            magnets = magnet_response.get("data", {}).get("magnets", [])
            if magnets:
                torrent_id = magnets[0].get("id")
                logger.info(
                    f"AllDebrid: Successfully added magnet, ID: {torrent_id}"
                )
                return str(torrent_id)
        except Exception as e:
            logger.error(f"AllDebrid: Exception extracting magnet ID: {str(e)}")

        return "Error: Could not extract torrent ID."