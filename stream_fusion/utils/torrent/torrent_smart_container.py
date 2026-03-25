import os

from typing import List, Dict
from RTN import parse

from stream_fusion.utils.debrid.alldebrid import AllDebrid
from stream_fusion.utils.debrid.premiumize import Premiumize
from stream_fusion.utils.debrid.realdebrid import RealDebrid
from stream_fusion.utils.debrid.torbox import Torbox
from stream_fusion.utils.stremthru.debrid import StremThruDebrid as StremThru
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.utils.cache.cache import cache_public
from stream_fusion.utils.general import season_episode_in_filename
from stream_fusion.logging_config import logger


class TorrentSmartContainer:
    def __init__(self, torrent_items: List[TorrentItem], media):
        self.logger = logger
        self.logger.info(
            f"Initializing TorrentSmartContainer with {len(torrent_items)} items"
        )
        self.__itemsDict: Dict[str, TorrentItem] = self._build_items_dict_by_infohash(
            torrent_items
        )
        self.__media = media
        self.__using_stremthru = False
        self.logger.info(
            "TorrentSmartContainer: Including all torrents regardless of seeders count"
        )

    def _normalize_hash(self, value):
        if not value:
            return None
        return str(value).strip().lower()

    def _is_video_filename(self, name: str) -> bool:
        if not name:
            return False
        return name.lower().endswith(
            (".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts")
        )

    def _build_file_info(self, file_index, title, size):
        return {
            "file_index": file_index if file_index is not None else 0,
            "title": title or "",
            "size": size or 0,
        }

    def _extract_local_movie_candidates(self, item: TorrentItem):
        candidates = []

        if getattr(item, "file_name", None):
            candidates.append(
                self._build_file_info(
                    getattr(item, "file_index", 0),
                    item.file_name,
                    getattr(item, "size", 0),
                )
            )

        if getattr(item, "full_index", None):
            for entry in item.full_index:
                if not isinstance(entry, dict):
                    continue
                file_name = entry.get("file_name") or entry.get("title") or ""
                if not file_name:
                    continue
                candidates.append(
                    self._build_file_info(
                        entry.get("file_index", 0),
                        file_name,
                        entry.get("size", 0),
                    )
                )

        return candidates

    def _extract_local_series_candidate(self, item: TorrentItem, media):
        if not getattr(item, "full_index", None):
            return None

        clean_season = media.season.replace("S", "")
        clean_episode = media.episode.replace("E", "")
        numeric_season = int(clean_season)
        numeric_episode = int(clean_episode)

        for entry in item.full_index:
            if not isinstance(entry, dict):
                continue
            seasons = entry.get("seasons", [])
            episodes = entry.get("episodes", [])
            if numeric_season in seasons and numeric_episode in episodes:
                return self._build_file_info(
                    entry.get("file_index", 0),
                    entry.get("file_name", ""),
                    entry.get("size", 0),
                )

        return None

    def get_unaviable_hashes(self):
        hashes = []
        for info_hash, item in self.__itemsDict.items():
            if item.availability is False:
                hashes.append(info_hash)
        self.logger.debug(
            f"TorrentSmartContainer: Retrieved {len(hashes)} hashes to process"
        )
        return hashes

    def get_items(self):
        items = list(self.__itemsDict.values())
        self.logger.debug(f"TorrentSmartContainer: Retrieved {len(items)} items")
        return items

    def get_direct_torrentable(self):
        self.logger.info("TorrentSmartContainer: Retrieving direct torrentable items")
        direct_torrentable_items = []
        for torrent_item in self.__itemsDict.values():
            if torrent_item.privacy == "public" and torrent_item.file_index is not None:
                direct_torrentable_items.append(torrent_item)
        self.logger.info(
            f"TorrentSmartContainer: Found {len(direct_torrentable_items)} direct torrentable items"
        )
        return direct_torrentable_items

    def get_best_matching(self):
        self.logger.info("TorrentSmartContainer: Finding best matching items")
        best_matching = []
        self.logger.debug(
            f"TorrentSmartContainer: Total items to process: {len(self.__itemsDict)}"
        )

        for torrent_item in self.__itemsDict.values():
            self.logger.trace(
                f"TorrentSmartContainer: Processing item: {torrent_item.raw_title} - Has torrent: {torrent_item.torrent_download is not None}"
            )

            if torrent_item.torrent_download is not None:
                self.logger.trace(
                    f"TorrentSmartContainer: Has file index: {torrent_item.file_index is not None}"
                )

                if torrent_item.file_index is not None:
                    best_matching.append(torrent_item)
                    self.logger.trace(
                        "TorrentSmartContainer: Item added to best matching (has file index)"
                    )

                elif self.__media.type == "series":
                    if torrent_item.full_index:
                        matching_file = self._find_matching_file(
                            torrent_item.full_index,
                            self.__media.season,
                            self.__media.episode,
                        )
                        if matching_file:
                            torrent_item.file_index = matching_file["file_index"]
                            torrent_item.file_name = matching_file["file_name"]
                            torrent_item.size = matching_file["size"]
                            best_matching.append(torrent_item)
                            self.logger.trace(
                                f"TorrentSmartContainer: Item added to best matching (found matching file: {matching_file['file_name']})"
                            )
                        else:
                            self.logger.trace(
                                "TorrentSmartContainer: No matching file found in full_index, item not added to best matching"
                            )
                    else:
                        best_matching.append(torrent_item)
                        self.logger.trace(
                            "TorrentSmartContainer: Item added to best matching (series without full_index, will be extracted by debrid)"
                        )
                else:
                    best_matching.append(torrent_item)
                    self.logger.trace(
                        "TorrentSmartContainer: Item added to best matching (movie without file_index)"
                    )

            else:
                best_matching.append(torrent_item)
                seeders_info = (
                    f"with {torrent_item.seeders} seeders"
                    if torrent_item.seeders is not None
                    else "with unknown seeders"
                )
                self.logger.trace(
                    f"TorrentSmartContainer: Item added to best matching (magnet link) - {seeders_info}"
                )

        for item in best_matching:
            if item.parsed_data is None:
                self.logger.debug(
                    f"TorrentSmartContainer.get_best_matching: Item '{item.raw_title[:60]}' missing parsed_data, parsing now"
                )
                item.parsed_data = parse(item.raw_title)

        self.logger.success(
            f"TorrentSmartContainer: Found {len(best_matching)} best matching items"
        )
        return best_matching

    def _find_matching_file(self, full_index, season, episode):
        self.logger.trace(
            f"TorrentSmartContainer: Searching for matching file: Season {season}, Episode {episode}"
        )

        if not full_index:
            self.logger.trace(
                "TorrentSmartContainer: Full index is empty, cannot find matching file"
            )
            return None

        try:
            target_season = int(season.replace("S", ""))
            target_episode = int(episode.replace("E", ""))
        except ValueError:
            self.logger.error(
                f"TorrentSmartContainer: Invalid season or episode format: {season}, {episode}"
            )
            return None

        best_match = None
        for file_entry in full_index:
            if (
                target_season in file_entry["seasons"]
                and target_episode in file_entry["episodes"]
            ):
                if best_match is None or file_entry["size"] > best_match["size"]:
                    best_match = file_entry
                    self.logger.trace(
                        f"TorrentSmartContainer: Found potential match: {file_entry['file_name']}"
                    )

        if best_match:
            self.logger.trace(
                f"TorrentSmartContainer: Best matching file found: {best_match['file_name']}"
            )
            return best_match

        self.logger.warning(
            f"TorrentSmartContainer: No matching file found for Season {season}, Episode {episode}"
        )
        return None

    def cache_container_items(self):
        self.logger.info(
            "TorrentSmartContainer: Starting cache process for container items"
        )
        self._save_to_cache()

    def _save_to_cache(self):
        self.logger.info("TorrentSmartContainer: Saving public items to cache")
        public_torrents = list(
            filter(lambda x: x.privacy == "public", self.get_items())
        )
        self.logger.debug(
            f"TorrentSmartContainer: Found {len(public_torrents)} public torrents to cache"
        )
        cache_public(public_torrents, self.__media)
        self.logger.info("TorrentSmartContainer: Caching process completed")

    def update_availability(self, debrid_response, debrid_type, media):
        if not debrid_response or debrid_response == {} or debrid_response == []:
            self.logger.debug(
                "TorrentSmartContainer: Debrid response is empty : "
                + str(debrid_response)
            )
            return

        self.logger.info(
            f"TorrentSmartContainer: Updating availability for {debrid_type.__name__}"
        )

        if (
            isinstance(debrid_response, list)
            and debrid_response
            and isinstance(debrid_response[0], dict)
            and "hash" in debrid_response[0]
        ):
            self.__using_stremthru = True
            store_name = debrid_response[0].get("store_name", "")
            underlying_debrid = (
                StremThru.get_underlying_debrid_code(store_name)
                or debrid_response[0].get("debrid", "RD")
            )
            self.logger.debug(
                f"TorrentSmartContainer: StremThru list response detected (store={store_name}, debrid={underlying_debrid})"
            )
            self._update_availability_stremthru(
                debrid_response, media, underlying_debrid
            )
            return

        if debrid_type is RealDebrid:
            self._update_availability_realdebrid(debrid_response, media)
        elif debrid_type is AllDebrid:
            self._update_availability_alldebrid(debrid_response, media)
        elif debrid_type is Torbox:
            self._update_availability_torbox(debrid_response, media)
        elif debrid_type is Premiumize:
            self._update_availability_premiumize(debrid_response)
        elif debrid_type is StremThru or debrid_type.__name__ == "StremThru":
            self.__using_stremthru = True

            if (
                debrid_response
                and isinstance(debrid_response[0], dict)
                and "store_name" in debrid_response[0]
            ):
                store_name = debrid_response[0]["store_name"]
            else:
                try:
                    log_entries = [
                        line
                        for line in self.logger.get_entries()
                        if "StremThru: Vérification de" in line
                        and "magnets sur StremThru-" in line
                    ]
                    if log_entries:
                        latest_log = log_entries[-1]
                        store_name = latest_log.split("StremThru-")[-1].strip()
                    else:
                        store_name = (
                            "torbox"
                            if "TBToken" in str(debrid_response)
                            else "alldebrid"
                        )
                except Exception:
                    store_name = (
                        "torbox" if "TBToken" in str(debrid_response) else "alldebrid"
                    )

            underlying_debrid = StremThru.get_underlying_debrid_code(store_name)
            self.logger.debug(
                f"TorrentSmartContainer: StremThru utilise le store: {store_name}, code: {underlying_debrid}"
            )
            self._update_availability_stremthru(
                debrid_response, media, underlying_debrid
            )
        else:
            self.logger.error(
                f"TorrentSmartContainer: Unsupported debrid type: {debrid_type.__name__}"
            )
            raise NotImplementedError(
                f"TorrentSmartContainer: Debrid type {debrid_type.__name__} not implemented"
            )

    def _update_availability_realdebrid(self, response, media):
        self.logger.info("TorrentSmartContainer: Updating availability for RealDebrid")
        for info_hash, details in response.items():
            normalized_hash = self._normalize_hash(info_hash)
            if "rd" not in details:
                self.logger.debug(
                    f"TorrentSmartContainer: Skipping hash {normalized_hash}: no RealDebrid data"
                )
                continue

            torrent_item = self.__itemsDict.get(normalized_hash)
            if torrent_item is None:
                self.logger.debug(
                    f"TorrentSmartContainer: Unknown RealDebrid hash returned: {normalized_hash}"
                )
                continue

            self.logger.debug(
                f"Processing {torrent_item.type}: {torrent_item.raw_title}"
            )
            files = []
            if torrent_item.type == "series":
                self._process_series_files(
                    details, media, torrent_item, files, debrid="RD"
                )
            else:
                self._process_movie_files(details, files)
            self._update_file_details(torrent_item, files, debrid="RD")

        self.logger.info(
            "TorrentSmartContainer: RealDebrid availability update completed"
        )

    def _process_series_files(
        self, details, media, torrent_item, files, debrid: str = "??"
    ):
        for variants in details["rd"]:
            file_found = False
            for file_index, file in variants.items():
                clean_season = media.season.replace("S", "")
                clean_episode = media.episode.replace("E", "")
                numeric_season = int(clean_season)
                numeric_episode = int(clean_episode)

                if season_episode_in_filename(
                    file["filename"], numeric_season, numeric_episode
                ):
                    self.logger.debug(f"Matching file found: {file['filename']}")
                    torrent_item.file_index = file_index
                    torrent_item.file_name = file["filename"]
                    torrent_item.size = file["filesize"]
                    torrent_item.availability = debrid
                    file_found = True
                    files.append(
                        {
                            "file_index": file_index,
                            "title": file["filename"],
                            "size": file["filesize"],
                        }
                    )
                    break
            if file_found:
                break

    def _process_movie_files(self, details, files):
        for variants in details["rd"]:
            for file_index, file in variants.items():
                self.logger.debug(
                    f"TorrentSmartContainer: Adding movie file: {file['filename']}"
                )
                files.append(
                    {
                        "file_index": file_index,
                        "title": file["filename"],
                        "size": file["filesize"],
                    }
                )

    def _update_availability_alldebrid(self, response, media):
        self.logger.info("TorrentSmartContainer: Updating availability for AllDebrid")

        if not isinstance(response, dict) or response.get("status") != "success":
            self.logger.error(
                f"TorrentSmartContainer: AllDebrid API error: {response}"
            )
            return

        magnets = response.get("data", {}).get("magnets", [])
        if not isinstance(magnets, list):
            self.logger.error(
                f"TorrentSmartContainer: Invalid AllDebrid magnets payload: {response}"
            )
            return

        for data in magnets:
            if not isinstance(data, dict):
                self.logger.warning(
                    f"TorrentSmartContainer: Invalid AllDebrid magnet entry ignored: {data}"
                )
                continue

            hash_value = self._normalize_hash(data.get("hash"))
            if not hash_value:
                self.logger.warning(
                    f"TorrentSmartContainer: AllDebrid entry without hash ignored: {data}"
                )
                continue

            torrent_item = self.__itemsDict.get(hash_value)
            if torrent_item is None:
                self.logger.debug(
                    f"TorrentSmartContainer: Unknown AllDebrid hash returned: {hash_value}"
                )
                continue

            if not data.get("instant", False):
                torrent_item.availability = False
                continue

            files_payload = data.get("files", [])
            if files_payload:
                files = []
                self._explore_folders_alldebrid(
                    files_payload, files, 1, torrent_item.type, media
                )

                if files:
                    self._update_file_details(torrent_item, files, debrid="AD")
                else:
                    torrent_item.availability = "AD"
            else:
                torrent_item.availability = "AD"

        self.logger.info(
            "TorrentSmartContainer: AllDebrid availability update completed"
        )

    def _update_availability_torbox(self, response, media):
        self.logger.info("TorrentSmartContainer: Updating availability for Torbox")
        if response["success"] is False:
            self.logger.error(f"TorrentSmartContainer: Torbox API error: {response}")
            return

        for data in response["data"]:
            hash_value = self._normalize_hash(data.get("hash"))
            torrent_item = self.__itemsDict.get(hash_value)
            if torrent_item is None:
                self.logger.debug(
                    f"TorrentSmartContainer: Unknown Torbox hash returned: {hash_value}"
                )
                continue

            files = self._process_torbox_files(data["files"], torrent_item.type, media)
            self._update_file_details(torrent_item, files, debrid="TB")

        self.logger.info("TorrentSmartContainer: Torbox availability update completed")

    def _process_torbox_files(self, files, type, media):
        processed_files = []
        for index, file in enumerate(files):
            if type == "series":
                if self._is_matching_episode_torbox(file["name"], media):
                    processed_files.append(
                        {
                            "file_index": index,
                            "title": os.path.basename(file["name"]),
                            "size": file["size"],
                        }
                    )
            elif type == "movie":
                processed_files.append(
                    {
                        "file_index": index,
                        "title": os.path.basename(file["name"]),
                        "size": file["size"],
                    }
                )
        return processed_files

    def _is_matching_episode_torbox(self, filepath, media):
        filename = os.path.basename(filepath)

        clean_season = media.season.replace("S", "")
        clean_episode = media.episode.replace("E", "")
        numeric_season = int(clean_season)
        numeric_episode = int(clean_episode)

        return season_episode_in_filename(filename, numeric_season, numeric_episode)

    def _update_availability_premiumize(self, response):
        self.logger.info("TorrentSmartContainer: Updating availability for Premiumize")
        if not response:
            self.logger.error(
                "TorrentSmartContainer: Empty response from Premiumize API"
            )
            return

        torrent_items = self.get_items()
        for hash_value, status in response.items():
            normalized_hash = self._normalize_hash(hash_value)
            for item in torrent_items:
                if item.info_hash and item.info_hash.lower() == normalized_hash:
                    is_available = status.get("transcoded", False)
                    item.availability = "PM" if is_available else None

                    if is_available:
                        if item.type == "series":
                            if "full_index" in item.__dict__ and item.full_index:
                                for file_info in item.full_index:
                                    clean_season = self.__media.season.replace("S", "")
                                    clean_episode = self.__media.episode.replace("E", "")
                                    numeric_season = int(clean_season)
                                    numeric_episode = int(clean_episode)

                                    if (
                                        numeric_season in file_info.get("seasons", [])
                                        and numeric_episode in file_info.get(
                                            "episodes", []
                                        )
                                    ):
                                        matched_file_info = {
                                            "file_index": file_info.get(
                                                "file_index", 0
                                            ),
                                            "title": file_info.get("file_name", ""),
                                            "size": file_info.get("size", 0),
                                        }
                                        self._update_file_details(
                                            item, [matched_file_info], debrid="PM"
                                        )
                                        self.logger.debug(
                                            f"TorrentSmartContainer: Updated series file details for {item.raw_title}: {matched_file_info}"
                                        )
                                        break

                        elif item.type == "movie":
                            self.logger.debug(
                                f"TorrentSmartContainer: Processing movie files for {item.raw_title}"
                            )

                            file_info = None

                            if "files" in status:
                                cached_files = [
                                    f
                                    for f in status["files"]
                                    if f.get("cached", False) is True
                                ]
                                if cached_files:
                                    largest_file = max(
                                        cached_files, key=lambda f: f.get("size", 0)
                                    )
                                    file_info = {
                                        "file_index": largest_file.get(
                                            "file_index", 0
                                        ),
                                        "title": largest_file.get("title", ""),
                                        "size": largest_file.get("size", 0),
                                    }

                            if (
                                not file_info
                                and "filename" in status
                                and "filesize" in status
                            ):
                                file_info = {
                                    "file_index": 0,
                                    "title": status.get("filename", ""),
                                    "size": int(status.get("filesize", 0)),
                                }

                            if file_info:
                                self._update_file_details(
                                    item, [file_info], debrid="PM"
                                )
                                self.logger.debug(
                                    f"TorrentSmartContainer: Updated movie file details for {item.raw_title}: {file_info}"
                                )

        self.logger.info(
            "TorrentSmartContainer: Premiumize availability update completed"
        )

    def _update_availability_stremthru(self, response, media, underlying_debrid="AD"):
        self.logger.info(
            f"TorrentSmartContainer: Updating StremThru availability (via {underlying_debrid})"
        )

        for result in response:
            if not isinstance(result, dict):
                continue

            hash_value = self._normalize_hash(result.get("hash"))
            if not hash_value:
                continue

            item = self.__itemsDict.get(hash_value)
            if item is None:
                self.logger.debug(
                    f"TorrentSmartContainer: Unknown StremThru hash returned: {hash_value}"
                )
                continue

            result_debrid = result.get("debrid")
            if result_debrid:
                debrid_code = result_debrid
                self.logger.debug(
                    f"TorrentSmartContainer: Utilisation du code debrid spécifique: {debrid_code} pour {hash_value}"
                )
            else:
                debrid_code = underlying_debrid

            item.availability = debrid_code

            files = result.get("files", [])
            if not isinstance(files, list):
                files = []

            normalized_files = []
            for file in files:
                if not isinstance(file, dict):
                    continue
                file_name = file.get("name", "") or file.get("title", "")
                normalized_files.append(
                    {
                        "index": file.get("index", 0),
                        "name": file_name,
                        "size": file.get("size", 0),
                    }
                )

            if item.type == "series":
                self.logger.debug(
                    f"TorrentSmartContainer: Processing series files for {item.raw_title}"
                )
                clean_season = media.season.replace("S", "")
                clean_episode = media.episode.replace("E", "")
                numeric_season = int(clean_season)
                numeric_episode = int(clean_episode)

                matching_files = []
                for file in normalized_files:
                    file_name = file.get("name", "")
                    if file_name and season_episode_in_filename(
                        file_name, numeric_season, numeric_episode
                    ):
                        matching_files.append(
                            self._build_file_info(
                                file.get("index", 0),
                                file_name,
                                file.get("size", 0),
                            )
                        )
                        self.logger.debug(
                            f"TorrentSmartContainer: Match found: {file_name}"
                        )

                if matching_files:
                    self._update_file_details(
                        item,
                        matching_files,
                        debrid=debrid_code,
                        skip_file_name_for_series=False,
                    )
                    self.logger.debug(
                        f"TorrentSmartContainer: StremThru updated matching series files for {item.raw_title}"
                    )
                    continue

                self.logger.debug(
                    f"TorrentSmartContainer: No direct episode match found for {item.raw_title}"
                )

                try:
                    from stream_fusion.utils.general import smart_episode_fallback

                    fallback_file = smart_episode_fallback(
                        normalized_files, numeric_season, numeric_episode
                    )
                except Exception:
                    fallback_file = None

                if fallback_file:
                    file_info = self._build_file_info(
                        fallback_file.get("index", 0),
                        fallback_file.get("name", ""),
                        fallback_file.get("size", 0),
                    )
                    self._update_file_details(
                        item,
                        [file_info],
                        debrid=debrid_code,
                        skip_file_name_for_series=False,
                    )
                    self.logger.info(
                        f"TorrentSmartContainer: Fallback intelligent utilisé pour {item.raw_title}: {fallback_file.get('name')}"
                    )
                    continue

                local_series_candidate = self._extract_local_series_candidate(item, media)
                if local_series_candidate:
                    self._update_file_details(
                        item,
                        [local_series_candidate],
                        debrid=debrid_code,
                        skip_file_name_for_series=False,
                    )
                    self.logger.info(
                        f"TorrentSmartContainer: Local full_index fallback utilisé pour {item.raw_title}: {local_series_candidate.get('title')}"
                    )
                    continue

                if normalized_files:
                    video_files = [
                        f for f in normalized_files if self._is_video_filename(f.get("name", ""))
                    ]
                    if video_files:
                        best_video = max(video_files, key=lambda f: f.get("size", 0))
                        file_info = self._build_file_info(
                            best_video.get("index", 0),
                            best_video.get("name", ""),
                            best_video.get("size", 0),
                        )
                        self._update_file_details(
                            item,
                            [file_info],
                            debrid=debrid_code,
                            skip_file_name_for_series=False,
                        )
                        self.logger.info(
                            f"TorrentSmartContainer: Generic video fallback utilisé pour {item.raw_title}: {best_video.get('name')}"
                        )
                        continue

                item.availability = debrid_code

            elif item.type == "movie":
                self.logger.debug(
                    f"TorrentSmartContainer: Processing movie files for {item.raw_title}"
                )

                file_infos = [
                    self._build_file_info(
                        file.get("index", 0),
                        file.get("name", ""),
                        file.get("size", 0),
                    )
                    for file in normalized_files
                    if file.get("name")
                ]

                if file_infos:
                    self._update_file_details(item, file_infos, debrid=debrid_code)
                    self.logger.debug(
                        f"TorrentSmartContainer: Updated movie file details for {item.raw_title}"
                    )
                    continue

                local_movie_candidates = self._extract_local_movie_candidates(item)
                if local_movie_candidates:
                    self._update_file_details(
                        item, local_movie_candidates, debrid=debrid_code
                    )
                    self.logger.info(
                        f"TorrentSmartContainer: Local movie fallback utilisé pour {item.raw_title}"
                    )
                    continue

                item.availability = debrid_code

            self.logger.debug(
                f"TorrentSmartContainer: Updated availability for {item.raw_title}: {item.availability}"
            )

        self.logger.info(
            "TorrentSmartContainer: StremThru availability update completed"
        )

    def _update_file_details(
        self, torrent_item, files, debrid: str = "??", skip_file_name_for_series=False
    ):
        if not files:
            self.logger.debug(
                f"TorrentSmartContainer: No files to update for {torrent_item.raw_title}"
            )
            return

        file = max(files, key=lambda file: file["size"])
        torrent_item.availability = debrid
        torrent_item.file_index = file["file_index"]

        if torrent_item.type == "series":
            if not skip_file_name_for_series:
                torrent_item.raw_title = file["title"]
            torrent_item.file_name = file["title"]
        else:
            torrent_item.file_name = file["title"]

        torrent_item.size = file["size"]
        self.logger.debug(
            f"TorrentSmartContainer: Updated file details for {torrent_item.raw_title}: {file['title']}"
        )

    def _build_items_dict_by_infohash(self, items: List[TorrentItem]):
        self.logger.info(
            f"TorrentSmartContainer: Building items dictionary by infohash ({len(items)} items)"
        )
        items_dict = {}
        for item in items:
            if item.info_hash is not None:
                normalized_hash = self._normalize_hash(item.info_hash)
                if normalized_hash not in items_dict:
                    self.logger.debug(f"Adding {normalized_hash} to items dict")
                    items_dict[normalized_hash] = item
                else:
                    existing_item = items_dict[normalized_hash]
                    if (
                        item.indexer == "Yggtorrent - API"
                        and existing_item.indexer != "Yggtorrent - API"
                    ):
                        self.logger.debug(
                            f"TorrentSmartContainer: Replacing {existing_item.indexer} with Yggtorrent for hash: {normalized_hash}"
                        )
                        items_dict[normalized_hash] = item
                    else:
                        self.logger.debug(
                            f"TorrentSmartContainer: Skipping duplicate info hash: {normalized_hash} (keeping {existing_item.indexer})"
                        )
        self.logger.info(
            f"TorrentSmartContainer: Built dictionary with {len(items_dict)} unique items"
        )
        return items_dict

    def _explore_folders_alldebrid(self, folder, files, file_index, type, media):
        if not isinstance(folder, list):
            self.logger.warning(
                f"TorrentSmartContainer: Invalid AllDebrid folder payload ignored: {folder}"
            )
            return file_index

        if type == "series":
            for file in folder:
                if not isinstance(file, dict):
                    self.logger.warning(
                        f"TorrentSmartContainer: Invalid AllDebrid entry ignored (not a dict): {file}"
                    )
                    continue

                if "e" in file and isinstance(file["e"], list):
                    file_index = self._explore_folders_alldebrid(
                        file["e"], files, file_index, type, media
                    )
                    continue

                # Handle both AllDebrid upload API format (n, s) and status/files API format (name, size, index)
                file_name = file.get("n") or file.get("name")
                if not file_name:
                    self.logger.warning(
                        f"TorrentSmartContainer: AllDebrid entry without filename ignored: {file}"
                    )
                    continue

                try:
                    parsed_file = parse(file_name)
                except Exception as exc:
                    self.logger.warning(
                        f"TorrentSmartContainer: Failed to parse AllDebrid filename '{file_name}': {exc}"
                    )
                    file_index += 1
                    continue

                clean_season = media.season.replace("S", "")
                clean_episode = media.episode.replace("E", "")
                numeric_season = int(clean_season)
                numeric_episode = int(clean_episode)

                if (
                    numeric_season in parsed_file.seasons
                    and numeric_episode in parsed_file.episodes
                ):
                    self.logger.debug(
                        f"TorrentSmartContainer: Matching series file found: {file_name}"
                    )
                    explicit_index = file.get("index")
                    effective_index = explicit_index if isinstance(explicit_index, int) and explicit_index >= 0 else file_index
                    files.append(
                        {
                            "file_index": effective_index,
                            "title": file_name,
                            "size": file.get("s") or file.get("size", 0),
                        }
                    )
                file_index += 1

        elif type == "movie":
            for file in folder:
                if not isinstance(file, dict):
                    self.logger.warning(
                        f"TorrentSmartContainer: Invalid AllDebrid entry ignored (not a dict): {file}"
                    )
                    continue

                if "e" in file and isinstance(file["e"], list):
                    file_index = self._explore_folders_alldebrid(
                        file["e"], files, file_index, type, media
                    )
                    continue

                # Handle both AllDebrid upload API format (n, s) and status/files API format (name, size, index)
                file_name = file.get("n") or file.get("name")
                if not file_name:
                    self.logger.warning(
                        f"TorrentSmartContainer: AllDebrid entry without filename ignored: {file}"
                    )
                    continue

                self.logger.debug(
                    f"TorrentSmartContainer: Adding movie file: {file_name}"
                )
                explicit_index = file.get("index")
                effective_index = explicit_index if isinstance(explicit_index, int) and explicit_index >= 0 else file_index
                files.append(
                    {
                        "file_index": effective_index,
                        "title": file_name,
                        "size": file.get("s") or file.get("size", 0),
                    }
                )
                file_index += 1

        return file_index