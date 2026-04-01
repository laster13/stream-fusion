from typing import List, Optional

from stream_fusion.utils.filter.base_filter import BaseFilter
from stream_fusion.logging_config import logger
from stream_fusion.utils.torrent.torrent_item import TorrentItem


class LanguagePriorityFilter(BaseFilter):
    """
    Sorts torrents by language priority.
    Priority groups and detection patterns are loaded from the title_matching module
    (backed by DB and Redis cache), replacing the former hardcoded constants.

    Priority order (default):
      1. VFF, VOF, VFI, MULTI
      2. VF2, VFQ, VQ, FRENCH
      3. VOSTFR
    """

    def __init__(self, config):
        super().__init__(config)
        self._vfq_mode = "vfq" in config.get("languages", [])

        try:
            from stream_fusion.utils.filter.title_matching import get_lang_manager
            self._lang_manager = get_lang_manager()
        except RuntimeError:
            self._lang_manager = None
            logger.warning("LanguagePriorityFilter: title_matching module not initialized, using fallback priorities")

    def filter(self, data: List[TorrentItem]) -> List[TorrentItem]:
        """Assign language_priority to each torrent and sort by it."""
        for torrent in data:
            torrent.language_priority = self._get_language_priority(torrent)
            logger.trace(f"Torrent {torrent.raw_title} — priority: {torrent.language_priority}")

        return sorted(data, key=lambda x: x.language_priority)

    def _get_language_priority(self, torrent: TorrentItem) -> int:
        lang_code = self._detect_language(torrent.raw_title)

        if lang_code and self._lang_manager:
            priority = self._lang_manager.get_priority_group(lang_code, self._vfq_mode)
            if priority != 999:
                return priority

        # Fallback: look at torrent.languages attribute
        if hasattr(torrent, "languages") and torrent.languages and self._lang_manager:
            best = 999
            for lang in torrent.languages:
                canonical = self._lang_manager.get_canonical_code(lang)
                if canonical:
                    p = self._lang_manager.get_priority_group(canonical, self._vfq_mode)
                    best = min(best, p)
            return best

        return 999

    def _detect_language(self, title: str) -> Optional[str]:
        if not title or not self._lang_manager:
            return None
        return self._lang_manager.detect_language_from_title(title)

    def can_filter(self) -> bool:
        return True
