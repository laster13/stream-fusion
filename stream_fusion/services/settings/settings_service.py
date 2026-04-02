from typing import Any, Optional

from redis import ConnectionPool, Redis
from sqlalchemy.ext.asyncio import async_sessionmaker

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.appsetting_dao import AppSettingDAO
from stream_fusion.services.settings.settings_registry import (
    CONFIGURABLE_KEYS,
    REGISTRY_BY_KEY,
    SETTINGS_REGISTRY,
)
from stream_fusion.settings import (
    DebridService,
    LogLevel,
    NoCacheVideoLanguages,
    settings as _singleton,
)

# Enum classes used by configurable settings, keyed by field name
_ENUM_TYPES: dict[str, type] = {
    "log_level": LogLevel,
    "download_service": DebridService,
    "no_cache_video_language": NoCacheVideoLanguages,
}

# For these enums, the form stores the member .name (not .value).
# e.g. NoCacheVideoLanguages has name="FR" but value="https://...long_url..."
_ENUM_BY_NAME: frozenset[str] = frozenset({"no_cache_video_language"})

REDIS_KEY_PREFIX = "app_setting:"
REDIS_TTL_SECONDS = 300  # 5 minutes


class SettingsService:
    """
    Multi-worker-safe dynamic settings service.

    Read path  (per request): Redis cache → DB → singleton fallback
    Write path (admin save) : DB upsert → Redis invalidation → singleton patch (this worker only)

    Other Gunicorn workers will see the new value within REDIS_TTL_SECONDS (5 min) on
    their next access, without requiring cross-process signalling.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        redis_pool: ConnectionPool,
    ) -> None:
        self._session_factory = session_factory
        self._redis_pool = redis_pool
        # Populated in seed_defaults() before apply_overrides_to_singleton() runs,
        # so it always holds the original env-var values — used by reset_many().
        self._env_defaults: dict[str, str] = {}

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _redis(self) -> Redis:
        return Redis(connection_pool=self._redis_pool)

    @staticmethod
    def _coerce(key: str, raw: str) -> Any:
        """Cast the raw string stored in DB/Redis back to the correct Python type."""
        defn = REGISTRY_BY_KEY.get(key)
        if defn is None:
            return raw
        t = defn.type
        if t == "bool":
            return raw.lower() in ("1", "true", "yes")
        if t == "int":
            return int(raw)
        if t == "float":
            return float(raw)
        # "str" and "enum" — return as-is
        return raw

    @staticmethod
    def _serialize(value: Any) -> str:
        """Serialize a Python value to the string form stored in DB/Redis."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return ""
        return str(value)

    def _redis_key(self, key: str) -> str:
        return f"{REDIS_KEY_PREFIX}{key}"

    def _cache_get(self, r: Redis, key: str) -> Optional[Any]:
        raw = r.get(self._redis_key(key))
        if raw is None:
            return None
        return self._coerce(key, raw.decode())

    def _cache_set(self, r: Redis, key: str, value: Any) -> None:
        r.setex(self._redis_key(key), REDIS_TTL_SECONDS, self._serialize(value))

    def _cache_invalidate(self, r: Redis, key: str) -> None:
        r.delete(self._redis_key(key))

    def _singleton_default(self, key: str) -> Any:
        """Return the current value from the Pydantic singleton as a plain string.

        For enum fields where the form choices are member *names* (not values),
        return the name.  For all other enums, return the value.
        """
        value = getattr(_singleton, key, None)
        if value is None:
            return ""
        if key in _ENUM_BY_NAME and hasattr(value, "name"):
            return value.name
        if hasattr(value, "value"):
            return value.value
        return value

    def _coerce_for_singleton(self, key: str, coerced: Any) -> Any:
        """Convert a plain string back to the proper enum (or None) before
        writing it onto the Pydantic singleton."""
        if key not in _ENUM_TYPES:
            return coerced
        enum_cls = _ENUM_TYPES[key]
        if not isinstance(coerced, str) or coerced == "":
            return None  # optional enum fields (e.g. download_service)
        if key in _ENUM_BY_NAME:
            try:
                return enum_cls[coerced]  # lookup by name
            except KeyError:
                logger.warning(f"SettingsService: unknown enum name '{coerced}' for '{key}'")
                return coerced
        else:
            try:
                return enum_cls(coerced)  # lookup by value
            except ValueError:
                logger.warning(f"SettingsService: unknown enum value '{coerced}' for '{key}'")
                return coerced

    # ── Public read interface ─────────────────────────────────────────────────

    async def get(self, key: str) -> Any:
        """Return the effective value for *key* (Redis → DB → singleton fallback)."""
        if key not in CONFIGURABLE_KEYS:
            return getattr(_singleton, key, None)

        r = self._redis()
        cached = self._cache_get(r, key)
        if cached is not None:
            return cached

        async with self._session_factory() as session:
            dao = AppSettingDAO(session)
            raw = await dao.get(key)

        if raw is not None:
            value = self._coerce(key, raw)
            self._cache_set(r, key, value)
            return value

        return self._singleton_default(key)

    async def get_all_effective(self) -> dict[str, Any]:
        """Return effective values for all configurable settings (for admin page render)."""
        async with self._session_factory() as session:
            dao = AppSettingDAO(session)
            db_overrides = await dao.get_all()

        result: dict[str, Any] = {}
        for defn in SETTINGS_REGISTRY:
            if defn.key in db_overrides:
                result[defn.key] = self._coerce(defn.key, db_overrides[defn.key])
            else:
                result[defn.key] = self._singleton_default(defn.key)
        return result

    # ── Public write interface ────────────────────────────────────────────────

    async def set(self, key: str, value: Any) -> None:
        """Persist *value* to DB, invalidate Redis cache, patch in-process singleton."""
        if key not in CONFIGURABLE_KEYS:
            raise ValueError(f"Setting '{key}' is not admin-configurable")

        raw = self._serialize(value)
        coerced = self._coerce(key, raw)

        async with self._session_factory() as session:
            dao = AppSettingDAO(session)
            await dao.upsert(key, raw)

        r = self._redis()
        self._cache_invalidate(r, key)

        # Patch singleton in this process so already-imported references see the new value
        # immediately (best-effort — other workers refresh via Redis TTL)
        try:
            singleton_value = self._coerce_for_singleton(key, coerced)
            object.__setattr__(_singleton, key, singleton_value)
        except Exception as exc:
            logger.warning(f"SettingsService: could not patch singleton for '{key}': {exc}")

    async def set_many(self, updates: dict[str, Any]) -> list[str]:
        """Batch update multiple settings. Returns keys that require a restart."""
        requires_restart: list[str] = []
        for key, value in updates.items():
            await self.set(key, value)
            defn = REGISTRY_BY_KEY.get(key)
            if defn and defn.requires_restart:
                requires_restart.append(key)
        return requires_restart

    # ── Startup lifecycle ─────────────────────────────────────────────────────

    async def seed_defaults(self) -> None:
        """On startup: INSERT env-var defaults for each configurable setting,
        only if no row exists yet (ON CONFLICT DO NOTHING).
        Existing admin overrides are preserved across restarts.
        """
        defaults = {
            defn.key: self._serialize(self._singleton_default(defn.key))
            for defn in SETTINGS_REGISTRY
        }
        # Capture env-var defaults NOW, before apply_overrides_to_singleton() patches
        # the singleton. This dict is the source of truth for reset_many().
        self._env_defaults = defaults
        async with self._session_factory() as session:
            dao = AppSettingDAO(session)
            inserted = await dao.seed_defaults(defaults)
        logger.info(
            f"SettingsService: seeded {len(defaults)} settings "
            f"({inserted} new defaults inserted)"
        )

    async def reset_many(self, keys: list[str]) -> list[str]:
        """Restore the given keys to their original env-var defaults.

        Upserts the env-var value back into DB, invalidates Redis,
        and patches the in-process singleton.
        Returns keys that require a restart to fully take effect.
        """
        if not self._env_defaults:
            logger.warning("SettingsService.reset_many called before seed_defaults — no-op")
            return []

        requires_restart: list[str] = []
        r = self._redis()

        for key in keys:
            if key not in CONFIGURABLE_KEYS:
                continue
            raw = self._env_defaults.get(key, "")
            async with self._session_factory() as session:
                dao = AppSettingDAO(session)
                await dao.upsert(key, raw, updated_by="reset")
            self._cache_invalidate(r, key)
            coerced = self._coerce(key, raw)
            singleton_value = self._coerce_for_singleton(key, coerced)
            try:
                object.__setattr__(_singleton, key, singleton_value)
            except Exception as exc:
                logger.warning(f"SettingsService: singleton reset failed for '{key}': {exc}")
            defn = REGISTRY_BY_KEY.get(key)
            if defn and defn.requires_restart:
                requires_restart.append(key)

        logger.info(f"SettingsService: reset {len(keys)} setting(s) to env-var defaults")
        return requires_restart

    async def apply_overrides_to_singleton(self) -> None:
        """On startup: load all DB overrides and patch the singleton in this process.

        This backward-compat bridge means the 57 existing files importing
        `from stream_fusion.settings import settings` will see DB-overridden
        values without modification.
        """
        async with self._session_factory() as session:
            dao = AppSettingDAO(session)
            db_overrides = await dao.get_all()

        patched = 0
        for key, raw in db_overrides.items():
            if key not in CONFIGURABLE_KEYS:
                continue
            coerced = self._coerce(key, raw)
            singleton_value = self._coerce_for_singleton(key, coerced)
            try:
                object.__setattr__(_singleton, key, singleton_value)
                patched += 1
            except Exception as exc:
                logger.warning(
                    f"SettingsService: singleton patch failed for '{key}': {exc}"
                )

        logger.info(
            f"SettingsService: applied {patched} DB overrides to in-process singleton"
        )
