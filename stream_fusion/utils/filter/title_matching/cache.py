import json

from redis import Redis

from stream_fusion.logging_config import logger


class RedisRulesCache:
    """
    Thin Redis cache layer for title matching rules.
    Serialises rules to/from JSON. No TTL — invalidated manually after admin changes.
    """

    PREFIX = "title_matching:"

    def __init__(self, redis_client: Redis) -> None:
        self._redis = redis_client

    def _key(self, name: str) -> str:
        return f"{self.PREFIX}{name}"

    def get(self, name: str) -> list | None:
        """Return deserialized rules list, or None if absent."""
        try:
            raw = self._redis.get(self._key(name))
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as e:
            logger.warning(f"RedisRulesCache.get({name}): {e}")
            return None

    def set(self, name: str, rules: list) -> None:
        """Serialize and store rules list."""
        try:
            self._redis.set(self._key(name), json.dumps(rules, ensure_ascii=False))
        except Exception as e:
            logger.warning(f"RedisRulesCache.set({name}): {e}")

    def invalidate(self, name: str) -> None:
        """Delete a specific cache key."""
        try:
            self._redis.delete(self._key(name))
            logger.debug(f"RedisRulesCache: invalidated '{name}'")
        except Exception as e:
            logger.warning(f"RedisRulesCache.invalidate({name}): {e}")

    def invalidate_all(self) -> None:
        """Delete all title_matching:* keys."""
        try:
            keys = self._redis.keys(f"{self.PREFIX}*")
            if keys:
                self._redis.delete(*keys)
                logger.debug(f"RedisRulesCache: invalidated {len(keys)} keys")
        except Exception as e:
            logger.warning(f"RedisRulesCache.invalidate_all: {e}")
