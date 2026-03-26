import time

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.debridcache_model import DebridCacheModel


class DebridCacheDAO:
    """DAO for the debrid_cache table — persistent L2 availability cache.

    Only stores confirmed-cached hashes (no negative / not-cached entries).
    All data is sanitised before storage: no private tokens, no announce URLs.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_batch(self, hashes: list[str], service: str) -> dict[str, dict]:
        """Return {info_hash: cached_data} for hashes that are cached and not yet expired."""
        if not hashes:
            return {}
        now = int(time.time())
        try:
            result = await self.session.execute(
                select(DebridCacheModel).where(
                    DebridCacheModel.info_hash.in_(hashes),
                    DebridCacheModel.service == service,
                    DebridCacheModel.expires_at > now,
                )
            )
            rows = result.scalars().all()
            return {row.info_hash: row.cached_data for row in rows}
        except Exception as e:
            logger.warning(f"DebridCacheDAO: get_batch failed ({e})")
            return {}

    async def upsert_batch(self, entries: list[dict], service: str, ttl: int) -> None:
        """Upsert a batch of cached entries (INSERT ... ON CONFLICT DO UPDATE).

        Args:
            entries: list of sanitised dicts, each must contain a 'hash' field.
            service: debrid service identifier (e.g. 'realdebrid').
            ttl: time-to-live in seconds from now.
        """
        if not entries:
            return
        now = int(time.time())
        expires_at = now + ttl
        values = [
            {
                "info_hash": e["hash"],
                "service": service,
                "cached_data": e,
                "checked_at": now,
                "expires_at": expires_at,
            }
            for e in entries
            if e.get("hash")
        ]
        if not values:
            return
        try:
            stmt = insert(DebridCacheModel).values(values)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_debrid_cache_hash_service",
                set_={
                    "cached_data": stmt.excluded.cached_data,
                    "checked_at": stmt.excluded.checked_at,
                    "expires_at": stmt.excluded.expires_at,
                },
            )
            await self.session.execute(stmt)
            await self.session.commit()
            logger.debug(f"DebridCacheDAO: upserted {len(values)} entries for {service}")
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"DebridCacheDAO: upsert_batch failed ({e})")

    async def invalidate(self, info_hash: str, service: str) -> None:
        """Delete a specific entry — called when a false positive is detected during playback."""
        try:
            await self.session.execute(
                delete(DebridCacheModel).where(
                    DebridCacheModel.info_hash == info_hash,
                    DebridCacheModel.service == service,
                )
            )
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"DebridCacheDAO: invalidate failed for {info_hash} ({e})")

    async def check_batch(self, hashes: list[str], service: str) -> set[str]:
        """Return the subset of `hashes` that are confirmed-cached and not expired.

        Hash-only projection — does NOT fetch cached_data (lighter than get_batch).
        Used by the share endpoint to answer availability queries without leaking file metadata.
        """
        if not hashes:
            return set()
        now = int(time.time())
        try:
            result = await self.session.execute(
                select(DebridCacheModel.info_hash).where(
                    DebridCacheModel.info_hash.in_(hashes),
                    DebridCacheModel.service == service,
                    DebridCacheModel.expires_at > now,
                )
            )
            return {row for (row,) in result.all()}
        except Exception as e:
            logger.warning(f"DebridCacheDAO: check_batch failed ({e})")
            return set()

    async def delete_expired(self) -> int:
        """Delete all expired entries. Returns the number of rows deleted."""
        now = int(time.time())
        try:
            result = await self.session.execute(
                delete(DebridCacheModel).where(DebridCacheModel.expires_at < now)
            )
            await self.session.commit()
            count = result.rowcount
            if count:
                logger.debug(f"DebridCacheDAO: deleted {count} expired entries")
            return count
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"DebridCacheDAO: delete_expired failed ({e})")
            return 0
