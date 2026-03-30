from stream_fusion.utils.stremthru.debrid import StremThruDebrid
from stream_fusion.logging_config import logger


class PikPak(StremThruDebrid):
    def __init__(self, config, session=None):
        super().__init__(config, session)
        self.name = "PikPak"
        self.extension = "PP"

        # Retrieve PikPak credentials (email:password)
        self.credentials = config.get("pikpak_credentials", "")

        # Configure StremThru to use PikPak
        self.set_store_credentials("pikpak", self.credentials)

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """Check bulk torrent availability via StremThru."""
        results = await super().get_availability_bulk(hashes_or_magnets, ip)
        logger.debug(f"PikPak (via StremThru): {len(results)} cached torrents found")
        return results

    async def add_magnet(self, magnet, ip=None):
        """Add a magnet to PikPak via StremThru."""
        result = await super().add_magnet(magnet, ip)
        logger.debug(f"PikPak (via StremThru): magnet added successfully: {result is not None}")
        return result

    async def get_stream_link(self, query, config=None, ip=None):
        """Generate a streaming link via StremThru."""
        link = await super().get_stream_link(query, config, ip)
        logger.debug(f"PikPak (via StremThru): stream link generated: {link is not None}")
        return link
