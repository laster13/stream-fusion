from urllib.parse import quote

from RTN import parse

from stream_fusion.utils.detection import detect_languages
from stream_fusion.utils.torrent.torrent_item import TorrentItem


class AbnResult:
    def __init__(self):
        self.raw_title = None
        self.size = None
        self.link = None
        self.indexer = "ABN - API"
        self.seeders = 0
        self.magnet = None
        self.info_hash = None
        self.privacy = "private"
        self.languages = None
        self.type = None
        self.parsed_data = None
        self.torrent_download = None
        # tmdb_id intentionally left None for keyword-based tracker
        # It will be assigned retroactively after title-match filtering
        self.tmdb_id = None

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

    def _normalize_info_hash(self, value) -> str:
        if not value:
            raise ValueError("Missing info_hash")
        info_hash = str(value).strip().lower()
        if len(info_hash) == 40 and all(c in "0123456789abcdef" for c in info_hash):
            return info_hash
        raise ValueError(f"Invalid info_hash: {info_hash}")

    def from_api_item(self, api_item: dict, media) -> "AbnResult":
        raw_hash = (
            api_item.get("infoHash")
            or api_item.get("info_hash")
            or api_item.get("hash")
        )
        self.info_hash = self._normalize_info_hash(raw_hash)

        raw_title = (
            api_item.get("name")
            or api_item.get("title")
            or api_item.get("release_name")
        )
        if not raw_title:
            raise ValueError("Missing title")

        parsed = parse(raw_title)
        self.raw_title = parsed.raw_title
        self.parsed_data = parsed

        size = api_item.get("size") or 0
        self.size = str(size)

        seeders = api_item.get("seeders") or 0
        self.seeders = int(seeders)

        self.magnet = (
            f"magnet:?xt=urn:btih:{self.info_hash}"
            f"&dn={quote(self.raw_title)}"
        )
        self.link = self.magnet

        # Store download URL without credentials — credentials added at serve time
        self.torrent_download = api_item.get("downloadUrl") or api_item.get("download_url") or None

        self.privacy = "private"
        self.languages = detect_languages(self.raw_title, default_language="fr")
        self.type = media.type
        # tmdb_id stays None — assigned retroactively after filtering confirms a match
        self.tmdb_id = None

        return self
