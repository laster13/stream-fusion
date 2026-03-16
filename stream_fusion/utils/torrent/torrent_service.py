import hashlib
import os
import time
import urllib.parse
from typing import List
import pathlib
import json

import bencodepy
import requests
from RTN import parse
from RTN.models import ParsedData

from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
from stream_fusion.utils.jackett.jackett_result import JackettResult
from stream_fusion.utils.sharewood.sharewood_result import SharewoodResult
from stream_fusion.utils.zilean.zilean_result import ZileanResult
from stream_fusion.utils.yggfilx.yggflix_result import YggflixResult
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.utils.general import get_info_hash_from_magnet
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings

class TorrentService:
    TORRENT_CACHE_DIR = pathlib.Path("/var/cache/torrents")

    def __init__(self, config, torrent_dao: TorrentItemDAO):
        self.config = config
        self.torrent_dao = torrent_dao
        self.logger = logger
        self.__session = requests.Session()
        # Ensure cache directory exists
        self.TORRENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def __generate_unique_id(raw_title: str, indexer: str = "cached") -> str:
        unique_string = f"{raw_title}_{indexer}"
        full_hash = hashlib.sha256(unique_string.encode()).hexdigest()
        return full_hash[:16]

    async def get_cached_torrent(self, raw_title: str, indexer: str) -> TorrentItem | None:
        unique_id = self.__generate_unique_id(raw_title, indexer)
        try:
            cached_item = await self.torrent_dao.get_torrent_item_by_id(unique_id)
            if cached_item:
                # to_torrent_item() automatically reprout parsed_data from raw_title
                return cached_item.to_torrent_item()
            return None
        except Exception as e:
            self.logger.error(f"Error getting cached torrent: {e}")
            return None

    @staticmethod
    def _strip_private_credentials(item: TorrentItem) -> TorrentItem:
        """Remove credential-bearing URLs from private-indexer items before caching.

        After stripping, credentials are reconstructed at serve time inside
        TorrentItem.to_debrid_stream_query() using the current user/server config.
        This prevents passkeys and API keys from leaking into shared Redis/PostgreSQL.
        """
        if item.indexer == "Sharewood - API":
            # torrent_download is the passkey-bearing download URL
            item.torrent_download = None
            # Rebuild a clean magnet (no private announce URL) from info_hash
            if item.info_hash:
                raw_name = item.file_name or item.raw_title or ""
                item.magnet = f"magnet:?xt=urn:btih:{item.info_hash}&dn={urllib.parse.quote(raw_name)}"
            # Strip the Sharewood announce URL from the trackers list
            item.trackers = [
                t for t in (item.trackers or [])
                if settings.sharewood_url not in str(t)
            ]
            # link is no longer useful after processing; point to clean magnet
            item.link = item.magnet or ""
        return item

    async def cache_torrent(self, torrent_item: TorrentItem, id: str = None):
        unique_id = self.__generate_unique_id(torrent_item.raw_title, torrent_item.indexer)
        # C411/Torr9 sans tmdb_id → orphelins, jamais retrouvés → on skip
        if torrent_item.indexer in ['C411 - API', 'Torr9 - API'] and not torrent_item.tmdb_id:
            self.logger.debug(f"TorrentService: Skipping {torrent_item.indexer} torrent without tmdb_id: {torrent_item.raw_title}")
            return
        try:
            existing = await self.torrent_dao.get_torrent_item_by_id(unique_id)
            if existing:
                # Already in DB, skip — don't overwrite tmdb_id or other fields
                self.logger.debug(f"TorrentService: Torrent already cached, skipping: {unique_id}")
            else:
                await self.torrent_dao.create_torrent_item(torrent_item, unique_id)
                self.logger.debug(f"TorrentService: Created new cached torrent: {unique_id}")
        except Exception as e:
            if "duplicate key value violates unique constraint" in str(e):
                self.logger.debug(f"TorrentService: Race condition, torrent already exists: {unique_id}")
            else:
                self.logger.error(f"TorrentService: Error caching torrent {unique_id}: {str(e)}")

    async def _update_cached_item(self, cached_item: TorrentItem, new_item: TorrentItem):
        """Update cached item with new data (tmdb_id, torrent_file_path) if available"""
        try:
            unique_id = self.__generate_unique_id(cached_item.raw_title, cached_item.indexer)
            needs_update = False

            # Update tmdb_id if the new item has one and cached doesn't
            if new_item.tmdb_id and not cached_item.tmdb_id:
                cached_item.tmdb_id = new_item.tmdb_id
                needs_update = True
                self.logger.debug(f"Updated tmdb_id for {unique_id}: {new_item.tmdb_id}")

            # Update torrent_file_path if new item has one and cached doesn't (or is different)
            if new_item.torrent_file_path and not cached_item.torrent_file_path:
                cached_item.torrent_file_path = new_item.torrent_file_path
                needs_update = True
                self.logger.debug(f"Updated torrent_file_path for {unique_id}: {new_item.torrent_file_path}")

            if needs_update:
                await self.torrent_dao.update_torrent_item(unique_id, cached_item)
                self.logger.info(f"Cached torrent updated: {unique_id}")
        except Exception as e:
            self.logger.error(f"Error updating cached item: {e}")

    async def convert_and_process(self, results: List[JackettResult | ZileanResult | YggflixResult | SharewoodResult], skip_yggflix_download: bool = False):
        """
        Convert and process torrent results.

        Args:
            results: List of torrent results to process
            skip_yggflix_download: If True, don't download .torrent files from Yggflix (just update metadata)
                                  Used during search to avoid heavy downloads. Set to False for actual playback.
        """
        torrent_items_result = []

        for result in results:
            torrent_item = result.convert_to_torrent_item()

            cached_item = await self.get_cached_torrent(torrent_item.raw_title, torrent_item.indexer)
            if cached_item:
                # Pour Yggflix: mettre à jour les seeders frais en mémoire (sans écriture DB)
                if torrent_item.indexer == "Yggtorrent - API":
                    cached_item.seeders = torrent_item.seeders
                    self.logger.debug(f"Updated seeders in memory for {torrent_item.raw_title}: {torrent_item.seeders}")

                # Don't update - causes DB contention with concurrent requests
                # tmdb_id will only be set for NEW torrents, not existing ones
                # await self._update_cached_item(cached_item, torrent_item)
                torrent_items_result.append(cached_item)
                continue

            # If skip_yggflix_download is True and this is a Yggflix URL, just cache without downloading
            if skip_yggflix_download and settings.yggflix_url and torrent_item.link.startswith(settings.yggflix_url):
                # Don't process, just cache the raw item (without .torrent file and info_hash)
                # The info_hash will be set to None, magnet will be empty
                await self.cache_torrent(torrent_item)
                torrent_items_result.append(torrent_item)
                continue

            if torrent_item.link.startswith("magnet:"):
                processed_torrent_item = self.__process_magnet(torrent_item)
            elif settings.sharewood_url and torrent_item.link.startswith(settings.sharewood_url):
                processed_torrent_item = self.__process_sharewood_web_url(torrent_item)
            elif settings.yggflix_url and torrent_item.link.startswith(settings.yggflix_url):
                processed_torrent_item = self.__process_ygg_api_url(torrent_item)
            else:
                processed_torrent_item = self.__process_web_url(torrent_item)

            self._strip_private_credentials(processed_torrent_item)
            await self.cache_torrent(processed_torrent_item)
            torrent_items_result.append(processed_torrent_item)

        return torrent_items_result
        
    def __process_sharewood_web_url(self, result: TorrentItem):
        if not self.config["sharewood"]:
            logger.error("Sharewood is not enabled in the config. Skipping processing of Sharewood URL.")
        
        try:
            time.sleep(1) # API limit 1 request per second
            response = self.__session.get(result.link, allow_redirects=True, timeout=5)
        except requests.exceptions.RequestException:
            self.logger.error(f"Error while processing url: {result.link}")
            return result
        except requests.exceptions.ReadTimeout:
            self.logger.error(f"Timeout while processing url: {result.link}")
            return result
        
        if response.status_code == 200:
            return self.__process_torrent(result, response.content)
        else:
            self.logger.error(f"Error code {response.status_code} while processing sharewood url: {result.link}")

        return result


    def __process_ygg_api_url(self, result: TorrentItem): 
        if not self.config["yggflix"]:
            logger.error("Yggflix is not enabled in the config. Skipping processing of Yggflix URL.")
        try:
            response = self.__session.get(result.link, timeout=10)
            time.sleep(0.1) # Add a delay of 0.1 seconds between requests faire usage for small VPS
        except requests.exceptions.RequestException:
            self.logger.error(f"Error while processing url: {result.link}")
            return result
        except requests.exceptions.ReadTimeout:
            self.logger.error(f"Timeout while processing url: {result.link}")
            return result
        
        if response.status_code == 200:
            return self.__process_torrent(result, response.content)
        elif response.status_code == 422:
            self.logger.info(f"Not aviable torrent on yggflix: {result.file_name}")
        else:
            self.logger.error(f"Error code {response.status_code} while processing ygg url: {result.link}")

        return result

    def __process_web_url(self, result: TorrentItem):
        try:
            time.sleep(0.2)
            response = self.__session.get(result.link, allow_redirects=False, timeout=40) # flaresolverr and Jackett timeouts
        except requests.exceptions.RequestException:
            self.logger.error(f"Error while processing url: {result.link}")
            return result
        except requests.exceptions.ReadTimeout:
            self.logger.error(f"Timeout while processing url: {result.link}")
            return result

        if response.status_code == 200:
            return self.__process_torrent(result, response.content)
        elif response.status_code == 302:
            result.magnet = response.headers['Location']
            return self.__process_magnet(result)
        else:
            self.logger.error(f"Error code {response.status_code} while processing url: {result.link}")

        return result

    def __process_torrent(self, result: TorrentItem, torrent_file):
        try:
            metadata = bencodepy.decode(torrent_file)
        except Exception as e:
            try:
                from bencodepy import Decoder
                decoder = Decoder(encoding='latin-1')
                metadata = decoder.decode(torrent_file)
            except Exception as inner_e:
                logger.error(f"Impossible de décoder le fichier torrent: {str(e)} puis {str(inner_e)}")
                result.torrent_download = result.link
                result.trackers = []
                result.info_hash = ""
                result.magnet = ""
                return result

        result.torrent_download = result.link
        # Save .torrent file to disk instead of PostgreSQL
        try:
            # Use info_hash as filename if available, otherwise generate one
            filename = f"{result.info_hash if result.info_hash else hashlib.sha256(torrent_file).hexdigest()}.torrent"
            filepath = self.TORRENT_CACHE_DIR / filename
            with open(filepath, 'wb') as f:
                f.write(torrent_file)
            result.torrent_file_path = str(filepath)
            self.logger.debug(f"Saved .torrent to disk: {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving .torrent to disk: {e}")
            result.torrent_file_path = None

        try:
            result.trackers = self.__get_trackers_from_torrent(metadata)
            result.info_hash = self.__convert_torrent_to_hash(metadata["info"])
            result.magnet = self.__build_magnet(result.info_hash, metadata["info"]["name"], result.trackers)
        except Exception as e:
            logger.error(f"Erreur lors du traitement des métadonnées du torrent: {str(e)}")
            result.trackers = []
            result.info_hash = ""
            result.magnet = ""

        if "files" not in metadata["info"]:
            result.file_index = 1
            return result

        result.files = metadata["info"]["files"]

        if result.type == "series":
            # Ensure we have parsed_data from raw_title
            if not result.parsed_data:
                result.parsed_data = parse(result.raw_title)

            # Only try to find episode file if we have valid parsed_data
            if result.parsed_data and isinstance(result.parsed_data, ParsedData):
                file_details = self.__find_single_episode_file(result.files, result.parsed_data.seasons, result.parsed_data.episodes)
            else:
                file_details = None
                self.logger.warning(f"No valid parsed_data for series torrent: {result.raw_title}")

            if file_details is not None:
                self.logger.debug("File details")
                self.logger.debug(file_details)
                result.file_index = file_details["file_index"]
                result.file_name = file_details["title"]
                result.size = file_details["size"]

            # Always create full_index for series - AllDebrid needs all files to search for matching episode
            result.full_index = self.__find_full_index(result.files)

        if result.type == "movie":
            result.file_index = self.__find_movie_file(result.files)

        return result

    def __process_magnet(self, result: TorrentItem):
        if result.magnet is None:
            result.magnet = result.link

        if result.info_hash is None:
            result.info_hash = get_info_hash_from_magnet(result.magnet)

        result.trackers = self.__get_trackers_from_magnet(result.magnet)

        return result

    def __convert_torrent_to_hash(self, torrent_contents):
        hashcontents = bencodepy.encode(torrent_contents)
        hexHash = hashlib.sha1(hashcontents).hexdigest()
        return hexHash.lower()

    def __build_magnet(self, hash, display_name, trackers):
        magnet_base = "magnet:?xt=urn:btih:"
        magnet = f"{magnet_base}{hash}&dn={display_name}"

        if len(trackers) > 0:
            magnet = f"{magnet}&tr={'&tr='.join(trackers)}"

        return magnet

    def __get_trackers_from_torrent(self, torrent_metadata):
        # Sometimes list, sometimes string
        announce = torrent_metadata["announce"] if "announce" in torrent_metadata else []
        # Sometimes 2D array, sometimes 1D array
        announce_list = torrent_metadata["announce-list"] if "announce-list" in torrent_metadata else []

        trackers = set()
        if isinstance(announce, str):
            trackers.add(announce)
        elif isinstance(announce, list):
            for tracker in announce:
                trackers.add(tracker)

        for announce_list_item in announce_list:
            if isinstance(announce_list_item, list):
                for tracker in announce_list_item:
                    trackers.add(tracker)
            if isinstance(announce_list_item, str):
                trackers.add(announce_list_item)

        return list(trackers)

    def __get_trackers_from_magnet(self, magnet: str):
        url_parts = urllib.parse.urlparse(magnet)
        query_parts = urllib.parse.parse_qs(url_parts.query)

        trackers = []
        if "tr" in query_parts:
            trackers = query_parts["tr"]

        return trackers

    def __find_single_episode_file(self, file_structure, season, episode):

        if len(season) == 0 or len(episode) == 0:
            return None

        file_index = 1
        strict_episode_files = []
        episode_files = []
        for files in file_structure:
            for file in files["path"]:

                parsed_file = parse(file)

                if season[0] in parsed_file.seasons and episode[0] in parsed_file.episodes:
                    episode_files.append({
                        "file_index": None,  # Let debrid service handle file selection
                        "title": file,
                        "size": files["length"]
                    })

            # Doesn't that need to be indented?
            file_index += 1

        if episode_files:
            return max(episode_files, key=lambda file: file["size"])
        return None
    
    def __find_full_index(self, file_structure):
        self.logger.debug("Starting to build full index of video files")
        video_formats = {".mkv", ".mp4", ".avi", ".mov", ".flv", ".wmv", ".webm", ".mpg", ".mpeg", ".m4v", ".3gp", ".3g2",
                        ".ogv", ".ogg", ".drc", ".gif", ".gifv", ".mng", ".avi", ".mov", ".qt", ".wmv", ".yuv", ".rm",
                        ".rmvb", ".asf", ".amv", ".m4p", ".m4v", ".mpg", ".mp2", ".mpeg", ".mpe", ".mpv", ".mpg",
                        ".mpeg", ".m2v", ".m4v", ".svi", ".3gp", ".3g2", ".mxf", ".roq", ".nsv", ".flv", ".f4v",
                        ".f4p", ".f4a", ".f4b"}
        
        full_index = []
        file_index = 1

        for file_entry in file_structure:
            file_path = file_entry.get("path", [])
            if isinstance(file_path, list):
                file_name = file_path[-1] if file_path else ""
            else:
                file_name = file_path

            _, file_extension = os.path.splitext(file_name.lower())
            
            if file_extension in video_formats:
                parsed_file = parse(file_name)
                if len(parsed_file.seasons) == 0 or len(parsed_file.episodes) == 0:
                    self.logger.debug(f"Skipping file without season or episode parsed: {file_name}")
                    continue
                full_index.append({
                    "file_index": file_index,
                    "file_name": file_name,
                    "full_path": os.path.join(*file_path) if isinstance(file_path, list) else file_path,
                    "size": file_entry.get("length", 0),
                    "seasons": parsed_file.seasons,
                    "episodes": parsed_file.episodes
                })
                self.logger.trace(f"Added file to index: {file_name}")
            
            file_index += 1
        
        self.logger.debug(f"Full index built with {len(full_index)} video files")
        return full_index

    def __find_movie_file(self, file_structure):
        max_size = 0
        max_file_index = 1
        current_file_index = 1
        for files in file_structure:
            if files["length"] > max_size:
                max_file_index = current_file_index
                max_size = files["length"]
            current_file_index += 1

        return max_file_index
