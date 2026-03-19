import json
import asyncio
from typing import List, Dict

from RTN import ParsedData
from stream_fusion.settings import settings
from stream_fusion.utils.models.media import Media
from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.utils.string_encoding import encodeb64

from stream_fusion.utils.parser.parser_utils import (
    detect_french_language,
    extract_release_group,
    filter_by_availability,
    filter_by_direct_torrent,
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
        """Parse async avec asyncio.gather() au lieu de threading"""

        # Limite le nombre de résultats
        limited_items = torrent_items[: int(self.config["maxResults"])]

        # Créer des tâches async pour chaque torrent_item
        tasks = [
            self._parse_to_debrid_stream_async(torrent_item, media)
            for torrent_item in limited_items
        ]

        # Exécuter en parallèle
        stream_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filtrer les exceptions et aplatir les résultats
        stream_list = []
        for result in stream_results:
            if isinstance(result, Exception):
                # Log mais continue
                continue
            if isinstance(result, list):
                stream_list.extend(result)
            elif result:
                stream_list.append(result)

        if self.config["debrid"]:
            is_torbox = (bool(self.config.get("TBToken")) or self.config.get("debridDownloader") == "TorBox")

            def combined_sort_key(item):
                name = item["name"]
                desc = item.get("description", "")
                if name.startswith(DIRECT_TORRENT):
                    return 0
                if INSTANTLY_AVAILABLE in name:
                    return 1
                if is_torbox and ("C411" in desc or "Torr9" in desc):
                    return 2
                if "C411" in desc or "Torr9" in desc:
                    return 4
                return 3

            stream_list = sorted(stream_list, key=combined_sort_key)

        return stream_list

    async def _parse_to_debrid_stream_async(
        self, torrent_item: TorrentItem, media: Media
    ) -> List[Dict]:
        """Version async qui retourne une liste de streams"""
        return await asyncio.to_thread(
            self._parse_to_debrid_stream_sync, torrent_item, media
        )

    def _parse_to_debrid_stream_sync(
        self, torrent_item: TorrentItem, media: Media
    ) -> List[Dict]:
        """Version synchrone du parsing (CPU-bound)"""
        # Ensure parsed_data is valid ParsedData object, not string or dict
        from RTN import parse

        # Force reparsing if not ParsedData
        if torrent_item.parsed_data is None or not isinstance(torrent_item.parsed_data, ParsedData):
            torrent_item.parsed_data = parse(torrent_item.raw_title)

        parsed_data: ParsedData = torrent_item.parsed_data
        name = self._create_stream_name(torrent_item, parsed_data)
        title = self._create_stream_title(torrent_item, parsed_data, media)

        queryb64 = encodeb64(
            json.dumps(torrent_item.to_debrid_stream_query(media, self.config))
        ).replace("=", "%3D")

        results = []

        # Stream principal debrid
        results.append({
            "name": name,
            "description": title,
            "url": f"{self.config['addonHost']}/playback/{self.configb64}/{queryb64}",
            "infoHash": torrent_item.info_hash,
            "behaviorHints": {
                "bingeGroup": self._generate_binge_group(torrent_item, media),
                "filename": torrent_item.file_name or torrent_item.raw_title,
            },
        })

        # Ajouter le stream direct torrent si applicable
        if self.config["torrenting"] and torrent_item.privacy == "public":
            direct_stream = self._create_direct_torrent_stream(torrent_item, parsed_data, title, media)
            if direct_stream:
                results.append(direct_stream)

        return results

    def _generate_binge_group(self, torrent_item: TorrentItem, media: Media) -> str:
        """Génère un bingeGroup intelligent selon le type de média"""

        if media.type == "movie":
            return f"stream-fusion-{torrent_item.info_hash}"

        if media.type == "series":
            # Pour les séries, utiliser l'ID de la série + résolution + team pour permettre
            # la lecture automatique même avec des torrents d'épisodes individuels
            series_id = media.id.split(":")[0] if ":" in media.id else media.id
            resolution = torrent_item.parsed_data.resolution if torrent_item.parsed_data.resolution else "Unknown"

            # Ajouter la team si disponible pour une meilleure granularité
            team = extract_release_group(torrent_item.raw_title) or torrent_item.parsed_data.group
            if team:
                return f"stream-fusion-{series_id}-{resolution}-{team}"
            else:
                return f"stream-fusion-{series_id}-{resolution}"

        # Fallback
        return f"stream-fusion-{torrent_item.info_hash}"

    def _create_stream_name(
        self, torrent_item: TorrentItem, parsed_data: ParsedData
    ) -> str:
        resolution = parsed_data.resolution or "Unknown"
        # Services de debrid principaux
        if torrent_item.availability == "RD":
            name = f"{INSTANTLY_AVAILABLE}instant\nReal-Debrid\n({resolution})"
        elif torrent_item.availability == "AD":
            name = f"{INSTANTLY_AVAILABLE}instant\nAllDebrid\n({resolution})"
        elif torrent_item.availability == "TB":
            name = f"{INSTANTLY_AVAILABLE}instant\nTorBox\n({resolution})"
        elif torrent_item.availability == "PM":
            name = f"{INSTANTLY_AVAILABLE}instant\nPremiumize\n({resolution})"
        # Services de debrid additionnels
        elif torrent_item.availability == "OC":
            name = f"{INSTANTLY_AVAILABLE}instant\nOffcloud\n({resolution})"
        elif torrent_item.availability == "DL":
            name = f"{INSTANTLY_AVAILABLE}instant\nDebridLink\n({resolution})"
        elif torrent_item.availability == "ED":
            name = f"{INSTANTLY_AVAILABLE}instant\nEasyDebrid\n({resolution})"
        elif torrent_item.availability == "PK":
            name = f"{INSTANTLY_AVAILABLE}instant\nPikPak\n({resolution})"
        else:
            name = f"{DOWNLOAD_REQUIRED}download\n{self.config.get("debridDownloader", settings.download_service)}\n({resolution})"
        return name

    def _create_stream_title(
        self, torrent_item: TorrentItem, parsed_data: ParsedData, media: Media
    ) -> str:
        title = f"{torrent_item.file_name}\n" if torrent_item.file_name else f"{torrent_item.raw_title}\n"


        title += self._add_language_info(torrent_item, parsed_data)
        title += self._add_torrent_info(torrent_item)
        title += self._add_media_info(parsed_data)

        return title.strip()

    def _add_language_info(
        self, torrent_item: TorrentItem, parsed_data: ParsedData
    ) -> str:
        info = (
            "/".join(get_emoji(lang) for lang in torrent_item.languages)
            if torrent_item.languages
            else "🌐"
        )

        lang_type = detect_french_language(torrent_item.raw_title)
        if lang_type:
            info += f"  ✔ {lang_type} "

        group = extract_release_group(torrent_item.raw_title) or parsed_data.group
        if group:
            info += f"  ☠️ {group}"

        return f"{info}\n"

    def _add_torrent_info(self, torrent_item: TorrentItem) -> str:
        size_in_gb = round(int(torrent_item.size) / 1024 / 1024 / 1024, 2)
        return f"🔍 {torrent_item.indexer} 💾 {size_in_gb}GB 👥 {torrent_item.seeders} \n"

    def _add_media_info(self, parsed_data: ParsedData) -> str:
        info = []
        if parsed_data.codec:
            info.append(f"🎥 {parsed_data.codec}")
        if parsed_data.quality:
            info.append(f"📺 {parsed_data.quality}")
        # Ajouter les informations HDR/Dolby Vision/SDR
        if parsed_data.hdr:
            hdr_info = ' '.join(parsed_data.hdr)
            info.append(f"🌈 {hdr_info}")
        elif parsed_data.resolution == "2160p":
            # Si c'est du 4K sans HDR, c'est du SDR
            info.append("🌈 SDR")
        if parsed_data.audio:
            info.append(f"🎧 {' '.join(parsed_data.audio)}")
        return " ".join(info) + "\n" if info else ""

    def _create_direct_torrent_stream(
        self,
        torrent_item: TorrentItem,
        parsed_data: ParsedData,
        title: str,
        media: Media,
    ) -> Dict:
        """Crée un stream direct torrent"""
        direct_torrent_name = f"{DIRECT_TORRENT}\n{parsed_data.quality}\n"
        if parsed_data.quality and parsed_data.quality[0] not in ["Unknown", ""]:
            direct_torrent_name += f"({'|'.join(parsed_data.quality)})"

        return {
            "name": direct_torrent_name,
            "description": title,
            "infoHash": torrent_item.info_hash,
            "fileIdx": (
                int(torrent_item.file_index) if torrent_item.file_index else None
            ),
            "behaviorHints": {
                "bingeGroup": self._generate_binge_group(torrent_item, media),
                "filename": torrent_item.file_name or torrent_item.raw_title,
            },
        }
