
from RTN import parse

from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.utils.detection import detect_languages
from stream_fusion.logging_config import logger


class C411Result:
    def __init__(self):
        self.raw_title = None
        self.size = None
        self.link = None
        self.indexer = "C411 - API"
        self.seeders = 0
        self.magnet = None
        self.info_hash = None
        self.privacy = "public"
        self.languages = None
        self.type = None
        self.parsed_data = None
        self.torrent_download = None

    def convert_to_torrent_item(self):
        parsed_data = self.parsed_data or parse(self.raw_title)
        return TorrentItem(
            raw_title=self.raw_title,
            size=self.size,
            magnet=self.magnet,
            info_hash=self.info_hash.lower() if self.info_hash else None,
            link=self.link or self.magnet,
            seeders=self.seeders,
            languages=self.languages,
            indexer=self.indexer,
            privacy=self.privacy,
            type=self.type,
            parsed_data=parsed_data,
            torrent_download=self.torrent_download,
            tmdb_id=self.tmdb_id,
        )

    def from_api_item(self, api_item, media):
        self.info_hash = api_item.info_hash.lower() if api_item.info_hash else None
        if not self.info_hash or len(self.info_hash) != 40:
            raise ValueError(f"Invalid info_hash: {self.info_hash}")

        parsed = parse(api_item.raw_title)
        self.raw_title = parsed.raw_title
        self.parsed_data = parsed
        self.size = api_item.size or "0"
        self.magnet = f"magnet:?xt=urn:btih:{self.info_hash}&dn={self.raw_title}"
        self.link = self.magnet
        self.seeders = api_item.seeders or 0
        self.privacy = api_item.privacy or "public"
        self.languages = detect_languages(self.raw_title, default_language="fr")
        self.type = media.type
        self.tmdb_id = None  # C411 merges TMDB-ID and text results: assigned retroactively after title-match filtering
        # torrent_download and passkey tracker are reconstructed at serve time from settings
        self.torrent_download = None
        return self
