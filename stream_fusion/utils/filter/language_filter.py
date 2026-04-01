from stream_fusion.utils.filter.base_filter import BaseFilter
from stream_fusion.logging_config import logger


class LanguageFilter(BaseFilter):
    def __init__(self, config):
        super().__init__(config)
        # Load language rules from the title_matching module singleton
        try:
            from stream_fusion.utils.filter.title_matching import get_lang_manager
            self._lang_manager = get_lang_manager()
        except RuntimeError:
            self._lang_manager = None

    def filter(self, data):
        filtered_data = []
        for torrent in data:
            if not torrent.languages:
                logger.debug(f"Skipping {torrent.raw_title} with no languages")
                continue

            languages = torrent.languages.copy()

            if torrent.indexer == "DMM - API" and "multi" in languages:
                matched = (
                    self._lang_manager.match_release_group(torrent.raw_title)
                    if self._lang_manager
                    else False
                )
                logger.trace(f"Release group match for {torrent.raw_title}: {matched}")
                if not matched:
                    languages.remove("multi")

            if torrent.indexer == "DMM - API" and "fr" in languages:
                matched = (
                    self._lang_manager.match_release_group(torrent.raw_title)
                    if self._lang_manager
                    else False
                )
                logger.trace(f"Release group match for {torrent.raw_title}: {matched}")
                if not matched:
                    languages.remove("fr")

            if "multi" in languages or any(
                lang in self.config["languages"] for lang in languages
            ):
                torrent.languages = languages
                logger.trace(f"Keeping {torrent.raw_title} with lang : {languages} ")
                filtered_data.append(torrent)

        return filtered_data

    def can_filter(self):
        return self.config["languages"] is not None
