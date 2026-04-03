import time
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.appsetting_model import AppSettingModel


class AppSettingDAO:
    """DAO for the app_settings table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, key: str) -> Optional[str]:
        """Return the stored string value for *key*, or None if not overridden."""
        result = await self.session.execute(
            select(AppSettingModel).where(AppSettingModel.key == key)
        )
        row = result.scalar_one_or_none()
        return row.value if row else None

    async def get_all(self) -> dict[str, str]:
        """Return all stored overrides as {key: value_str}."""
        result = await self.session.execute(select(AppSettingModel))
        return {row.key: row.value for row in result.scalars().all()}

    async def upsert(self, key: str, value: str, updated_by: str = "admin") -> None:
        """INSERT ON CONFLICT UPDATE — idempotent upsert."""
        now = int(time.time())
        stmt = (
            pg_insert(AppSettingModel)
            .values(key=key, value=value, updated_at=now, updated_by=updated_by)
            .on_conflict_do_update(
                index_elements=["key"],
                set_={"value": value, "updated_at": now, "updated_by": updated_by},
            )
        )
        await self.session.execute(stmt)
        await self.session.commit()
        logger.trace(f"AppSettingDAO: upserted '{key}' = '{value}' by {updated_by}")

    async def delete(self, key: str) -> bool:
        """Remove the override for *key* (reverts to env-based default on next read)."""
        result = await self.session.execute(
            select(AppSettingModel).where(AppSettingModel.key == key)
        )
        row = result.scalar_one_or_none()
        if row:
            await self.session.delete(row)
            await self.session.commit()
            logger.trace(f"AppSettingDAO: deleted override for '{key}'")
            return True
        return False

    async def seed_defaults(self, defaults: dict[str, str]) -> int:
        """For each key in *defaults*, INSERT only if no row exists yet.

        Uses ON CONFLICT DO NOTHING to preserve existing admin overrides.
        Returns the number of keys inserted.
        """
        now = int(time.time())
        inserted = 0
        for key, value in defaults.items():
            stmt = (
                pg_insert(AppSettingModel)
                .values(key=key, value=value, updated_at=now, updated_by="seed")
                .on_conflict_do_nothing(index_elements=["key"])
            )
            result = await self.session.execute(stmt)
            inserted += result.rowcount or 0
        await self.session.commit()
        return inserted
