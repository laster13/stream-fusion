from RTN import parse
from RTN.models import ParsedData
from urllib.parse import quote

from stream_fusion.utils.models.media import Media
from stream_fusion.utils.models.series import Series
from stream_fusion.logging_config import logger

# Private indexer names that strip credentials before caching
_PRIVATE_CREDENTIAL_INDEXERS = {
    "Sharewood - API",
    "C411 - API",
    "Torr9 - API",
    "LaCale - API",
    "GenerationFree - API",
}

# Indexers that TorBox handles better than other debrid services.
# When TorBox is configured and a torrent comes from one of these, it is
# preferred as the download service regardless of debridDownloader.
_TORBOX_PREFERRED_INDEXERS = {
    "C411 - API",
    "Torr9 - API",
    "LaCale - API",
    "GenerationFree - API",
}


class TorrentItem:
    def __init__(self, raw_title, size, magnet, info_hash, link, seeders, languages, indexer,
                 privacy, type=None, parsed_data=None, torrent_download=None, tmdb_id=None):
        self.logger = logger

        self.raw_title = raw_title
        self.size = size
        self.magnet = magnet
        self.info_hash = info_hash
        self.link = link
        self.seeders = seeders
        self.languages = languages
        self.indexer = indexer
        self.type = type
        self.privacy = privacy
        self.tmdb_id = tmdb_id

        self.file_name = None
        self.files = None
        self.torrent_download = torrent_download
        self.trackers = []
        self.file_index = None
        self.full_index = None
        self.availability = False

        if parsed_data is None:
            from RTN import parse
            parsed_data = parse(raw_title)
        self.parsed_data: ParsedData = parsed_data

    def to_debrid_stream_query(self, media: Media, config: dict = None) -> dict:
        """Build the query dict embedded in playback URLs.

        For private indexers, credentials were stripped before caching.
        Reconstruct them here at serve time using the current user config
        and server settings so debrid services receive valid URLs.
        """
        from stream_fusion.settings import settings

        magnet = self.magnet
        torrent_download = self.torrent_download

        # Only add tracker credentials when the file must be downloaded (⬇️).
        # When the file is already cached on the debrid service (⚡), the tracker
        # URL is not needed and exposing credentials in the stream URL is unnecessary.
        if self.info_hash and self.indexer in _PRIVATE_CREDENTIAL_INDEXERS and not self.availability:
            if self.indexer == "C411 - API" and settings.c411_api_key:
                c411_tracker = settings.c411_passkey or ""
                if c411_tracker and magnet and "&tr=" not in magnet:
                    magnet = f"{magnet}&tr={quote(c411_tracker, safe='')}"
                if not torrent_download:
                    base = settings.c411_url.rstrip("/")
                    torrent_download = f"{base}/api?t=get&id={self.info_hash}&apikey={settings.c411_api_key}"

            elif self.indexer == "Torr9 - API" and settings.torr9_api_key:
                if magnet and "&tr=" not in magnet:
                    torr9_tracker = f"https://tracker.torr9.xyz/announce/{settings.torr9_api_key}"
                    magnet = f"{magnet}&tr={quote(torr9_tracker, safe='')}"

            elif self.indexer == "LaCale - API" and settings.lacale_api_key:
                if magnet and "&tr=" not in magnet:
                    lacale_tracker = f"https://tracker.lacale.fr/announce/{settings.lacale_api_key}"
                    magnet = f"{magnet}&tr={quote(lacale_tracker, safe='')}"

            elif self.indexer == "GenerationFree - API" and settings.generationfree_api_key:
                if magnet and "&tr=" not in magnet:
                    generationfree_tracker = f"https://tracker.generationfree.fr/announce/{settings.generationfree_api_key}"
                    magnet = f"{magnet}&tr={quote(generationfree_tracker, safe='')}"

            elif self.indexer == "Sharewood - API":
                sharewood_passkey = None
                if settings.sharewood_unique_account and settings.sharewood_passkey:
                    sharewood_passkey = settings.sharewood_passkey
                elif config:
                    sharewood_passkey = config.get("sharewoodPasskey")
                if sharewood_passkey and magnet and f"{settings.sharewood_url}/announce/" not in magnet:
                    tracker = f"{settings.sharewood_url}/announce/{sharewood_passkey}"
                    magnet = f"{magnet}&tr={quote(tracker, safe='')}"

        query = {
            "magnet": magnet,
            "type": self.type,
            "file_index": self.file_index,
            "season": media.season if isinstance(media, Series) else None,
            "episode": media.episode if isinstance(media, Series) else None,
            "torrent_download": quote(torrent_download) if (torrent_download is not None and not self.availability) else None,
            "service": self.availability if self.availability else "DL",
            "privacy": self.privacy if self.privacy else "private",
        }

        # When the torrent is not cached and comes from a TorBox-preferred indexer,
        # signal the playback endpoint to use TorBox for the download (if configured).
        if (
            not self.availability
            and self.indexer in _TORBOX_PREFERRED_INDEXERS
            and config
            and config.get("TBToken")
        ):
            query["preferred_service"] = "TorBox"

        return query

    def to_dict(self):
        resolution = getattr(self.parsed_data, 'resolution', 'UNKNOWN') if self.parsed_data else 'NONE'
        logger.trace(f"TorrentItem.to_dict(): '{self.raw_title[:60]}' → resolution='{resolution}' (caching)")

        parsed_data_dict = None
        if self.parsed_data:
            if isinstance(self.parsed_data, ParsedData):
                parsed_data_dict = self.parsed_data.model_dump()
            else:
                parsed_data_dict = self.parsed_data

        return {
            'raw_title': self.raw_title,
            'size': self.size,
            'magnet': self.magnet,
            'info_hash': self.info_hash,
            'link': self.link,
            'seeders': self.seeders,
            'languages': self.languages,
            'indexer': self.indexer,
            'type': self.type,
            'privacy': self.privacy,
            'tmdb_id': self.tmdb_id,
            'file_name': self.file_name,
            'files': self.files,
            'torrent_download': self.torrent_download,
            'trackers': self.trackers,
            'file_index': self.file_index,
            'full_index': self.full_index,
            'availability': self.availability,
            'parsed_data': parsed_data_dict,
        }

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            logger.error(f"Expected dict, got {type(data)}")
            return None

        instance = cls(
            raw_title=data['raw_title'],
            size=data['size'],
            magnet=data['magnet'],
            info_hash=data['info_hash'],
            link=data['link'],
            seeders=data['seeders'],
            languages=data['languages'],
            indexer=data['indexer'],
            privacy=data['privacy'],
            type=data['type'],
            tmdb_id=data.get('tmdb_id')
        )

        instance.file_name = data['file_name']
        instance.files = data['files']
        instance.torrent_download = data['torrent_download']
        instance.trackers = data['trackers']
        instance.file_index = data['file_index']
        instance.full_index = data['full_index']
        instance.availability = data['availability']

        if data.get('parsed_data'):
            try:
                reconstructed = ParsedData(**data['parsed_data'])
                logger.trace(f"TorrentItem.from_dict(): Reconstructed ParsedData: {reconstructed}, resolution={getattr(reconstructed, 'resolution', 'UNKNOWN')}")
                if reconstructed is not None:
                    instance.parsed_data = reconstructed
                else:
                    logger.warning("TorrentItem.from_dict(): Reconstructed ParsedData is None, will re-parse")
                    instance.parsed_data = parse(instance.raw_title)
            except Exception as e:
                logger.warning(f"Failed to reconstruct ParsedData from cache: {e}, will re-parse")
                instance.parsed_data = parse(instance.raw_title)
        else:
            logger.trace("TorrentItem.from_dict(): No parsed_data in cache dict, will parse")
            instance.parsed_data = parse(instance.raw_title)

        resolution = getattr(instance.parsed_data, 'resolution', 'UNKNOWN') if instance.parsed_data else 'NONE'
        logger.trace(f"TorrentItem.from_dict(): '{instance.raw_title[:60]}' → resolution='{resolution}'")

        return instance