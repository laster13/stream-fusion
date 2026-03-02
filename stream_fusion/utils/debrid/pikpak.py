from stream_fusion.utils.debrid.stremthru import StremThru
from stream_fusion.logging_config import logger


class PikPak(StremThru):
    def __init__(self, config, session=None):
        super().__init__(config, session)
        self.name = "PikPak"
        self.extension = "PP"

        # Récupérer les identifiants PikPak (email:password)
        self.credentials = config.get("pikpak_credentials", "")

        # Configurer StremThru pour utiliser PikPak
        self.set_store_credentials("pikpak", self.credentials)

    async def get_availability_bulk(self, hashes_or_magnets, ip=None):
        """Vérifie la disponibilité des torrents en masse via StremThru"""
        results = await super().get_availability_bulk(hashes_or_magnets, ip)
        logger.debug(f"PikPak (via StremThru): {len(results)} torrents en cache trouvés")
        return results

    async def add_magnet(self, magnet, ip=None):
        """Ajoute un magnet à PikPak via StremThru"""
        result = await super().add_magnet(magnet, ip)
        logger.debug(f"PikPak (via StremThru): Magnet ajouté avec succès: {result is not None}")
        return result

    async def get_stream_link(self, query, config=None, ip=None):
        """Génère un lien de streaming via StremThru"""
        link = await super().get_stream_link(query, config, ip)
        logger.debug(f"PikPak (via StremThru): Lien de streaming généré: {link is not None}")
        return link
