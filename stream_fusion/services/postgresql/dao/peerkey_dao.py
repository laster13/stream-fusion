import secrets
import time
import uuid
from typing import Optional

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.postgresql.models.peerkey_model import PeerKeyModel
from stream_fusion.logging_config import logger


class PeerKeyDAO:
    """CRUD for the peer_keys table."""

    def __init__(self, session: AsyncSession = Depends(get_db_session)) -> None:
        self.session = session

    async def create_key(
        self,
        name: str,
        rate_limit: int = 60,
        rate_window: int = 60,
        expires_at: Optional[int] = None,
    ) -> dict:
        """Create a new peer key.

        Returns a dict with key_id and secret — the secret is shown only once
        and is not recoverable afterwards.
        """
        async with self.session.begin():
            key_id = str(uuid.uuid4())
            secret = secrets.token_hex(32)  # 256-bit random secret
            key = PeerKeyModel(
                key_id=key_id,
                secret=secret,
                name=name,
                rate_limit=rate_limit,
                rate_window=rate_window,
                expires_at=expires_at,
                created_at=int(time.time()),
            )
            self.session.add(key)
            await self.session.flush()
            logger.success(f"PeerKeyDAO: created peer key '{name}' ({key_id})")
            return {"key_id": key_id, "secret": secret, "name": name}

    async def get_by_key_id(self, key_id: str) -> Optional[PeerKeyModel]:
        """Return an active peer key by key_id, or None if not found / revoked."""
        async with self.session.begin():
            result = await self.session.execute(
                select(PeerKeyModel).where(
                    PeerKeyModel.key_id == key_id,
                    PeerKeyModel.is_active == True,  # noqa: E712
                )
            )
            return result.scalar_one_or_none()

    async def record_usage(self, key_id: str) -> None:
        """Increment total_queries and update last_used_at for the given key."""
        async with self.session.begin():
            result = await self.session.execute(
                select(PeerKeyModel).where(PeerKeyModel.key_id == key_id)
            )
            key = result.scalar_one_or_none()
            if key:
                key.last_used_at = int(time.time())
                key.total_queries += 1

    async def revoke_key(self, key_id: str) -> bool:
        """Set is_active = False. Returns True if found."""
        async with self.session.begin():
            result = await self.session.execute(
                select(PeerKeyModel).where(PeerKeyModel.key_id == key_id)
            )
            key = result.scalar_one_or_none()
            if key:
                key.is_active = False
                logger.info(f"PeerKeyDAO: revoked peer key {key_id}")
                return True
            return False

    async def delete_key(self, key_id: str) -> bool:
        """Hard-delete a peer key. Returns True if found."""
        async with self.session.begin():
            result = await self.session.execute(
                select(PeerKeyModel).where(PeerKeyModel.key_id == key_id)
            )
            key = result.scalar_one_or_none()
            if key:
                await self.session.delete(key)
                logger.info(f"PeerKeyDAO: deleted peer key {key_id}")
                return True
            return False

    async def list_keys(self) -> list[PeerKeyModel]:
        """Return all peer keys (active and revoked), ordered by creation date."""
        async with self.session.begin():
            result = await self.session.execute(
                select(PeerKeyModel).order_by(PeerKeyModel.created_at.desc())
            )
            return list(result.scalars().all())
