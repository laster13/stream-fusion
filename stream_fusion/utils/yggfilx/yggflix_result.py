from RTN import parse

from stream_fusion.utils.torrent.torrent_item import TorrentItem
from stream_fusion.logging_config import logger
from stream_fusion.utils.detection import detect_languages


class YggflixResult:
    def __init__(self):
        self.raw_title = None
        self.size = None
        self.link = None
        self.indexer = None
        self.seeders = None
        self.magnet = None
        self.info_hash = None
        self.privacy = None
        self.from_cache = None
        self.languages = None
        self.type = None
        self.tmdb_id = None
        self.parsed_data = None

    def convert_to_torrent_item(self):
        parsed_data = self.parsed_data or parse(self.raw_title)
        logger.debug(f"YggflixResult.convert_to_torrent_item(): '{self.raw_title[:60]}' → resolution='{getattr(parsed_data, 'resolution', 'UNKNOWN')}'")
        return TorrentItem(
            raw_title=self.raw_title,
            size=self.size,
            magnet=self.magnet,
            info_hash=self.info_hash.lower() if self.info_hash is not None else None,
            link=self.link,
            seeders=self.seeders,
            languages=self.languages,
            indexer=self.indexer,
            privacy=self.privacy,
            type=self.type,
            parsed_data=parsed_data,
            torrent_download=None,
            tmdb_id=self.tmdb_id
        )
