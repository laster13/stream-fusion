import re
from typing import Optional

from sqlalchemy.ext.asyncio import async_sessionmaker

from stream_fusion.logging_config import logger
from stream_fusion.utils.filter.title_matching.cache import RedisRulesCache

_CACHE_KEY = "language_rules"


class LanguageRulesManager:
    """
    Manages language detection patterns, release group patterns, code mappings
    and priority groups from DB/Redis.

    Replaces the hardcoded constants FRENCH_PATTERNS, FR_RELEASE_GROUPS,
    lang_mapping and language_priority_groups from filters and constants.py.
    """

    def __init__(self, cache: RedisRulesCache, db_session_factory: async_sessionmaker) -> None:
        self._cache = cache
        self._db_factory = db_session_factory

        # Compiled state
        self._french_patterns: dict[str, re.Pattern] = {}
        self._release_group_patterns: list[re.Pattern] = []
        self._code_mapping: dict[str, str] = {}
        self._priority_groups_default: dict[str, int] = {}
        self._priority_groups_vfq: dict[str, int] = {}

    # ── Initialization ────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Load rules from Redis (or DB as fallback) and compile patterns."""
        rules = self._cache.get(_CACHE_KEY)
        if rules is None:
            logger.debug("LanguageRulesManager: cache miss — loading from DB")
            rules = await self._load_from_db()
            if rules:
                self._cache.set(_CACHE_KEY, rules)
        else:
            logger.debug(f"LanguageRulesManager: loaded {len(rules)} rules from Redis")
        self._apply_rules(rules or [])

    async def reload(self) -> None:
        """Invalidate Redis, reload from DB, recompile patterns."""
        self._cache.invalidate(_CACHE_KEY)
        await self.initialize()
        logger.info("LanguageRulesManager: reloaded")

    async def _load_from_db(self) -> list[dict]:
        from stream_fusion.services.postgresql.dao.language_rule_dao import LanguageRuleDAO
        try:
            async with self._db_factory() as session:
                dao = LanguageRuleDAO(session)
                rules = await dao.get_all_active()
                return [r.to_dict() for r in rules]
        except Exception as e:
            logger.error(f"LanguageRulesManager: failed to load rules from DB: {e}")
            return []

    def _apply_rules(self, rules: list[dict]) -> None:
        french_patterns: dict[str, str] = {}
        release_groups: list[str] = []
        code_mapping: dict[str, str] = {}
        priority_default: dict[str, int] = {}
        priority_vfq: dict[str, int] = {}

        for r in rules:
            rt = r.get("rule_type", "")
            key = r.get("key", "")
            value = r.get("value", "")
            extra = r.get("extra") or {}

            if not key or not value:
                continue

            if rt == "french_pattern":
                french_patterns[key] = value
            elif rt == "release_group":
                release_groups.append(value)
            elif rt == "code_mapping":
                code_mapping[key.lower()] = value
            elif rt == "priority_group":
                try:
                    priority = int(value)
                except (ValueError, TypeError):
                    continue
                if extra.get("vfq_mode"):
                    priority_vfq[key] = priority
                else:
                    priority_default[key] = priority

        # Compile french patterns
        self._french_patterns = {}
        for name, pattern in french_patterns.items():
            try:
                self._french_patterns[name] = re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                logger.warning(f"LanguageRulesManager: invalid french_pattern '{name}': {e}")

        # Compile release group patterns
        self._release_group_patterns = []
        for pattern in release_groups:
            try:
                self._release_group_patterns.append(re.compile(pattern))
            except re.error as e:
                logger.warning(f"LanguageRulesManager: invalid release_group pattern: {e}")

        self._code_mapping = code_mapping
        self._priority_groups_default = priority_default
        self._priority_groups_vfq = priority_vfq

        logger.debug(
            f"LanguageRulesManager: compiled {len(self._french_patterns)} french patterns, "
            f"{len(self._release_group_patterns)} release group patterns, "
            f"{len(self._code_mapping)} code mappings, "
            f"{len(self._priority_groups_default)} default priorities, "
            f"{len(self._priority_groups_vfq)} VFQ priorities"
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def get_french_patterns(self) -> dict[str, re.Pattern]:
        """Return compiled french language detection patterns keyed by language code."""
        return self._french_patterns

    def get_release_group_patterns(self) -> list[re.Pattern]:
        """Return compiled release group patterns for FR releases."""
        return self._release_group_patterns

    def get_canonical_code(self, lang_code: str) -> Optional[str]:
        """Convert a short language code (e.g. 'fr') to its canonical form (e.g. 'FRENCH')."""
        return self._code_mapping.get(lang_code.lower())

    def get_priority_group(self, lang_code: str, vfq_mode: bool = False) -> int:
        """
        Return priority group for a language code.
        Lower value = higher priority. Returns 999 if unknown.
        """
        groups = self._priority_groups_vfq if vfq_mode else self._priority_groups_default
        return groups.get(lang_code, 999)

    def detect_language_from_title(self, title: str) -> Optional[str]:
        """
        Detect the language code from a torrent raw title.
        Returns the first matching language code or None.
        """
        if not title:
            return None
        for lang_code, pattern in self._french_patterns.items():
            if pattern.search(title):
                return lang_code
        return None

    def match_release_group(self, title: str) -> bool:
        """Return True if the title matches any known FR release group."""
        for pattern in self._release_group_patterns:
            if pattern.search(title):
                return True
        return False
