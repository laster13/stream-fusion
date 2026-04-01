import time
from typing import Optional

from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.mismatch_model import TmdbMismatchModel
from stream_fusion.services.postgresql.models.torrentitem_model import TorrentItemModel


class TmdbMismatchDAO:
    """DAO for the tmdb_mismatches table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        info_hash: str,
        tmdb_id: int,
        raw_title: str,
        indexer: str,
        notes: Optional[str] = None,
    ) -> Optional[TmdbMismatchModel]:
        """Create a mismatch entry and immediately reset tmdb_id to NULL in torrent_items."""
        try:
            mismatch = TmdbMismatchModel(
                info_hash=info_hash,
                tmdb_id=tmdb_id,
                raw_title=raw_title,
                indexer=indexer,
                notes=notes or None,
            )
            self.session.add(mismatch)
            await self.session.flush()

            # Immediately clean the bad association in torrent_items
            stmt = (
                update(TorrentItemModel)
                .where(TorrentItemModel.info_hash == info_hash)
                .where(TorrentItemModel.tmdb_id == tmdb_id)
                .values(
                    tmdb_id=None,
                    updated_at=int(time.time()),
                )
            )
            await self.session.execute(stmt)
            await self.session.flush()

            await self.session.commit()
            await self.session.refresh(mismatch)
            logger.info(
                f"TmdbMismatchDAO: flagged ({info_hash[:8]}…, tmdb_id={tmdb_id}) as mismatch"
                f" and reset tmdb_id to NULL in torrent_items"
            )
            return mismatch
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"TmdbMismatchDAO: create failed ({e})")
            return None

    async def delete(self, mismatch_id: int) -> bool:
        try:
            result = await self.session.execute(
                delete(TmdbMismatchModel).where(TmdbMismatchModel.id == mismatch_id)
            )
            await self.session.commit()
            deleted = result.rowcount > 0
            if deleted:
                logger.info(f"TmdbMismatchDAO: deleted mismatch {mismatch_id}")
            return deleted
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"TmdbMismatchDAO: delete failed ({e})")
            return False

    async def list_all(self, limit: int = 200, offset: int = 0) -> list[TmdbMismatchModel]:
        try:
            result = await self.session.execute(
                select(TmdbMismatchModel)
                .order_by(TmdbMismatchModel.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.warning(f"TmdbMismatchDAO: list_all failed ({e})")
            return []

    async def exists(self, info_hash: str, tmdb_id: int) -> bool:
        """Return True if this (info_hash, tmdb_id) pair is a known mismatch."""
        try:
            result = await self.session.execute(
                select(TmdbMismatchModel.id)
                .where(TmdbMismatchModel.info_hash == info_hash)
                .where(TmdbMismatchModel.tmdb_id == tmdb_id)
                .limit(1)
            )
            return result.scalar_one_or_none() is not None
        except Exception as e:
            logger.warning(f"TmdbMismatchDAO: exists check failed ({e})")
            return False
