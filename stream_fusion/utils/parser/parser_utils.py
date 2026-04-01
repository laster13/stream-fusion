import re
from typing import Dict

INSTANTLY_AVAILABLE = "⚡"
DOWNLOAD_REQUIRED = "⬇️​​"
DIRECT_TORRENT = "🏴‍☠️"

def get_emoji(language: str) -> str:
    emoji_dict = {
        "fr": "🇫🇷 FR", "en": "🇬🇧 EN", "es": "🇪🇸 ES",
        "de": "🇩🇪 GR", "it": "🇮🇹 IT", "pt": "🇵🇹 PO",
        "ru": "🇷🇺 RU", "in": "🇮🇳 IN", "nl": "🇳🇱 DU",
        "hu": "🇭🇺 HU", "la": "🇲🇽 LA", "multi": "🌍 MULTi",
    }
    return emoji_dict.get(language, "🇬🇧")

def filter_by_availability(item: Dict) -> int:
    return 0 if item["name"].startswith(INSTANTLY_AVAILABLE) else 1

def filter_by_direct_torrent(item: Dict) -> int:
    return 1 if item["name"].startswith(DIRECT_TORRENT) else 0

def extract_release_group(title: str) -> str:
    try:
        from stream_fusion.utils.filter.title_matching import get_lang_manager
        patterns = get_lang_manager().get_release_group_patterns()
        for pattern in patterns:
            match = pattern.search(title)
            if match:
                return match.group(0)
        return None
    except RuntimeError:
        return None

def detect_french_language(title: str) -> str:
    try:
        from stream_fusion.utils.filter.title_matching import get_lang_manager
        french_patterns = get_lang_manager().get_french_patterns()
        for language, pattern in french_patterns.items():
            if pattern.search(title):
                return language
        return None
    except RuntimeError:
        return None