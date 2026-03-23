import json
import asyncio
from typing import List, Dict

from RTN import ParsedData, parse
from stream_fusion.settings import settings
from stream_fusion.utils.models.media import Media
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.utils.string_encoding import encodeb64

from stream_fusion.utils.parser.parser_utils import (
    detect_french_language,
    extract_release_group,
    get_emoji,
    INSTANTLY_AVAILABLE,
    DOWNLOAD_REQUIRED,
    DIRECT_TORRENT,
)


class StreamParser:
    def __init__(self, config: Dict):
        self.config = config
        self.configb64 = encodeb64(json.dumps(config).replace("=", "%3D"))

    async def parse_to_stremio_streams(
        self, torrent_items: List[TorrentItem], media: Media
    ) -> List[Dict]:
        try:
            max_results = int(self.config.get("maxResults", len(torrent_items)))
        except (TypeError, ValueError):
            max_results = len(torrent_items)

        limited_items = torrent_items[:max_results]

        tasks = [
            self._parse_to_debrid_stream_async(torrent_item, media)
            for torrent_item in limited_items
        ]

        stream_results = await asyncio.gather(*tasks, return_exceptions=True)

        stream_list = []
        for result in stream_results:
            if isinstance(result, Exception):
                continue
            if isinstance(result, list):
                stream_list.extend([item for item in result if item])
            elif result:
                stream_list.append(result)

        if self.config.get("debrid"):
            if self.config.get("torrenting"):
                stream_list = [
                    item
                    for item in stream_list
                    if item.get("name", "").startswith(DIRECT_TORRENT)
                    or INSTANTLY_AVAILABLE in item.get("name", "")
                ]
            else:
                stream_list = [
                    item
                    for item in stream_list
                    if INSTANTLY_AVAILABLE in item.get("name", "")
                ]

            is_torbox = (
                bool(self.config.get("TBToken"))
                or self.config.get("debridDownloader") == "TorBox"
            )

            SPECIAL_INDEXERS = ("C411", "Torr9", "LaCale", "GenerationFree")

            def combined_sort_key(item):
                name = item.get("name", "")
                desc = item.get("description", "")
                is_special = any(indexer in desc for indexer in SPECIAL_INDEXERS)

                if name.startswith(DIRECT_TORRENT):
                    return 0
                if INSTANTLY_AVAILABLE in name:
                    return 1
                if is_torbox and is_special:
                    return 2
                if is_special:
                    return 4
                return 3

            stream_list = sorted(stream_list, key=combined_sort_key)

        return stream_list

    async def _parse_to_debrid_stream_async(
        self, torrent_item: TorrentItem, media: Media
    ) -> List[Dict]:
        return await asyncio.to_thread(
            self._parse_to_debrid_stream_sync, torrent_item, media
        )

    def _parse_to_debrid_stream_sync(
        self, torrent_item: TorrentItem, media: Media
    ) -> List[Dict]:
        if torrent_item.parsed_data is None or not isinstance(
            torrent_item.parsed_data, ParsedData
        ):
            try:
                torrent_item.parsed_data = parse(torrent_item.raw_title or "")
            except Exception:
                torrent_item.parsed_data = None

        parsed_data: ParsedData = torrent_item.parsed_data
        name = self._create_stream_name(torrent_item, parsed_data)
        title = self._create_stream_title(torrent_item, parsed_data, media)

        try:
            query_payload = torrent_item.to_debrid_stream_query(media, self.config)
        except Exception:
            return []

        queryb64 = encodeb64(json.dumps(query_payload)).replace("=", "%3D")

        info_hash = getattr(torrent_item, "info_hash", None)
        file_name = (
            getattr(torrent_item, "file_name", None)
            or getattr(torrent_item, "raw_title", None)
            or "unknown"
        )

        results = []

        playback_stream = {
            "name": name,
            "description": title,
            "url": f"{self.config['addonHost']}/playback/{self.configb64}/{queryb64}",
            "behaviorHints": {
                "bingeGroup": self._generate_binge_group(torrent_item, media),
                "filename": file_name,
            },
        }

        if info_hash:
            playback_stream["infoHash"] = info_hash

        results.append(playback_stream)

        if self.config.get("torrenting") and torrent_item.privacy == "public":
            direct_stream = self._create_direct_torrent_stream(
                torrent_item, parsed_data, title, media
            )
            if direct_stream:
                results.append(direct_stream)

        return results

    def _generate_binge_group(self, torrent_item: TorrentItem, media: Media) -> str:
        info_hash = getattr(torrent_item, "info_hash", None) or "unknown"

        if media.type == "movie":
            return f"stream-fusion-{info_hash}"

        if media.type == "series":
            series_id = media.id.split(":")[0] if ":" in media.id else media.id

            parsed_data = torrent_item.parsed_data
            resolution = (
                parsed_data.resolution
                if parsed_data and getattr(parsed_data, "resolution", None)
                else "Unknown"
            )

            team = extract_release_group(torrent_item.raw_title) or (
                parsed_data.group if parsed_data and getattr(parsed_data, "group", None) else None
            )

            if team:
                return f"stream-fusion-{series_id}-{resolution}-{team}"
            return f"stream-fusion-{series_id}-{resolution}"

        return f"stream-fusion-{info_hash}"

    def _create_stream_name(
        self, torrent_item: TorrentItem, parsed_data: ParsedData
    ) -> str:
        resolution = (
            parsed_data.resolution
            if parsed_data and getattr(parsed_data, "resolution", None)
            else "Unknown"
        )

        if torrent_item.availability == "RD":
            name = f"{INSTANTLY_AVAILABLE}instant\nReal-Debrid\n({resolution})"
        elif torrent_item.availability == "AD":
            name = f"{INSTANTLY_AVAILABLE}instant\nAllDebrid\n({resolution})"
        elif torrent_item.availability == "TB":
            name = f"{INSTANTLY_AVAILABLE}instant\nTorBox\n({resolution})"
        elif torrent_item.availability == "PM":
            name = f"{INSTANTLY_AVAILABLE}instant\nPremiumize\n({resolution})"
        elif torrent_item.availability == "OC":
            name = f"{INSTANTLY_AVAILABLE}instant\nOffcloud\n({resolution})"
        elif torrent_item.availability == "DL":
            name = f"{INSTANTLY_AVAILABLE}instant\nDebridLink\n({resolution})"
        elif torrent_item.availability == "ED":
            name = f"{INSTANTLY_AVAILABLE}instant\nEasyDebrid\n({resolution})"
        elif torrent_item.availability == "PK":
            name = f"{INSTANTLY_AVAILABLE}instant\nPikPak\n({resolution})"
        else:
            name = (
                f"{DOWNLOAD_REQUIRED}download\n"
                f"{self.config.get('debridDownloader', settings.download_service)}\n"
                f"({resolution})"
            )
        return name

    def _create_stream_title(
        self, torrent_item: TorrentItem, parsed_data: ParsedData, media: Media
    ) -> str:
        display_name = torrent_item.file_name or torrent_item.raw_title or "unknown"
        title = f"{display_name}\n"
        title += self._add_language_info(torrent_item, parsed_data)
        title += self._add_torrent_info(torrent_item)
        title += self._add_media_info(parsed_data)
        return title.strip()

    def _add_language_info(
        self, torrent_item: TorrentItem, parsed_data: ParsedData
    ) -> str:
        info = (
            "/".join(get_emoji(lang) for lang in torrent_item.languages)
            if getattr(torrent_item, "languages", None)
            else "🌐"
        )

        raw_title = getattr(torrent_item, "raw_title", "") or ""
        lang_type = detect_french_language(raw_title)
        if lang_type:
            info += f"  ✔ {lang_type} "

        group = extract_release_group(raw_title) or (
            parsed_data.group if parsed_data and getattr(parsed_data, "group", None) else None
        )
        if group:
            info += f"  ☠️ {group}"

        return f"{info}\n"

    def _add_torrent_info(self, torrent_item: TorrentItem) -> str:
        try:
            size_in_gb = round(int(torrent_item.size) / 1024 / 1024 / 1024, 2)
            size_display = f"{size_in_gb}GB"
        except (TypeError, ValueError):
            size_display = "Unknown"

        indexer = torrent_item.indexer or "Unknown"
        seeders = torrent_item.seeders if torrent_item.seeders is not None else "?"
        return f"🔍 {indexer} 💾 {size_display} 👥 {seeders} \n"

    def _add_media_info(self, parsed_data: ParsedData) -> str:
        if not parsed_data:
            return ""

        info = []
        if getattr(parsed_data, "codec", None):
            info.append(f"🎥 {parsed_data.codec}")
        if getattr(parsed_data, "quality", None):
            info.append(f"📺 {parsed_data.quality}")
        if getattr(parsed_data, "hdr", None):
            hdr_info = " ".join(parsed_data.hdr)
            info.append(f"🌈 {hdr_info}")
        elif getattr(parsed_data, "resolution", None) == "2160p":
            info.append("🌈 SDR")
        if getattr(parsed_data, "audio", None):
            info.append(f"🎧 {' '.join(parsed_data.audio)}")
        return " ".join(info) + "\n" if info else ""

    def _create_direct_torrent_stream(
        self,
        torrent_item: TorrentItem,
        parsed_data: ParsedData,
        title: str,
        media: Media,
    ) -> Dict:
        if not getattr(torrent_item, "info_hash", None):
            return None

        direct_torrent_name = f"{DIRECT_TORRENT}\n"

        if (
            parsed_data
            and getattr(parsed_data, "quality", None)
            and parsed_data.quality[0] not in ["Unknown", ""]
        ):
            direct_torrent_name += f"{'|'.join(parsed_data.quality)}\n"

        stream = {
            "name": direct_torrent_name,
            "description": title,
            "infoHash": torrent_item.info_hash,
            "behaviorHints": {
                "bingeGroup": self._generate_binge_group(torrent_item, media),
                "filename": torrent_item.file_name or torrent_item.raw_title or "unknown",
            },
        }

        if torrent_item.file_index is not None:
            try:
                stream["fileIdx"] = int(torrent_item.file_index)
            except (TypeError, ValueError):
                pass

        return stream