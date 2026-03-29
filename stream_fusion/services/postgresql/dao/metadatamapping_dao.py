import time
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.models.metadatamapping_model import MetadataMappingModel


# ── Standalone session (outside FastAPI request context) ──────────────────────

@asynccontextmanager
async def _standalone_db_session():
    from stream_fusion.settings import settings
    engine = create_async_engine(str(settings.pg_url), pool_size=1, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        await session.close()
        await engine.dispose()


async def lookup_mapping_by_imdb_id(imdb_id: str) -> Optional[MetadataMappingModel]:
    """Standalone IMDB→mapping lookup safe to call outside a FastAPI request."""
    try:
        async with _standalone_db_session() as session:
            result = await session.execute(
                select(MetadataMappingModel).where(MetadataMappingModel.imdb_id == imdb_id)
            )
            return result.scalar_one_or_none()
    except Exception as e:
        logger.warning(f"lookup_mapping_by_imdb_id({imdb_id}): {e}")
        return None


async def lookup_mapping_by_tmdb_id(tmdb_id: str) -> Optional[MetadataMappingModel]:
    """Standalone TMDB→mapping lookup safe to call outside a FastAPI request."""
    try:
        async with _standalone_db_session() as session:
            result = await session.execute(
                select(MetadataMappingModel).where(MetadataMappingModel.tmdb_id == str(tmdb_id))
            )
            return result.scalar_one_or_none()
    except Exception as e:
        logger.warning(f"lookup_mapping_by_tmdb_id({tmdb_id}): {e}")
        return None


# ── DAO ───────────────────────────────────────────────────────────────────────

class MetadataMappingDAO:
    """DAO for the metadata_mappings table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_imdb_id(self, imdb_id: str) -> Optional[MetadataMappingModel]:
        try:
            result = await self.session.execute(
                select(MetadataMappingModel).where(MetadataMappingModel.imdb_id == imdb_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.warning(f"MetadataMappingDAO: get_by_imdb_id failed ({e})")
            return None

    async def get_by_tmdb_id(self, tmdb_id: str) -> Optional[MetadataMappingModel]:
        try:
            result = await self.session.execute(
                select(MetadataMappingModel).where(MetadataMappingModel.tmdb_id == str(tmdb_id))
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.warning(f"MetadataMappingDAO: get_by_tmdb_id failed ({e})")
            return None

    async def list_all(self) -> list[MetadataMappingModel]:
        try:
            result = await self.session.execute(
                select(MetadataMappingModel).order_by(MetadataMappingModel.imdb_id)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.warning(f"MetadataMappingDAO: list_all failed ({e})")
            return []

    async def create(
        self,
        imdb_id: str,
        tmdb_id: Optional[str] = None,
        title_override: Optional[str] = None,
        media_type: str = "series",
        notes: Optional[str] = None,
        search_titles: Optional[list] = None,
        year_override: Optional[int] = None,
    ) -> MetadataMappingModel:
        mapping = MetadataMappingModel(
            imdb_id=imdb_id,
            tmdb_id=tmdb_id or None,
            title_override=title_override or None,
            media_type=media_type,
            notes=notes or None,
            search_titles=search_titles or None,
            year_override=year_override,
        )
        self.session.add(mapping)
        await self.session.commit()
        await self.session.refresh(mapping)
        logger.info(f"MetadataMappingDAO: created mapping for {imdb_id}")
        return mapping

    async def update(
        self,
        mapping_id: int,
        imdb_id: str,
        tmdb_id: Optional[str] = None,
        title_override: Optional[str] = None,
        media_type: str = "series",
        notes: Optional[str] = None,
        search_titles: Optional[list] = None,
        year_override: Optional[int] = None,
    ) -> Optional[MetadataMappingModel]:
        result = await self.session.execute(
            select(MetadataMappingModel).where(MetadataMappingModel.id == mapping_id)
        )
        mapping = result.scalar_one_or_none()
        if not mapping:
            return None

        mapping.imdb_id = imdb_id
        mapping.tmdb_id = tmdb_id or None
        mapping.title_override = title_override or None
        mapping.media_type = media_type
        mapping.notes = notes or None
        mapping.search_titles = search_titles or None
        mapping.year_override = year_override
        mapping.updated_at = int(time.time())

        await self.session.commit()
        await self.session.refresh(mapping)
        logger.info(f"MetadataMappingDAO: updated mapping {mapping_id}")
        return mapping

    async def delete(self, mapping_id: int) -> bool:
        try:
            result = await self.session.execute(
                delete(MetadataMappingModel).where(MetadataMappingModel.id == mapping_id)
            )
            await self.session.commit()
            deleted = result.rowcount > 0
            if deleted:
                logger.info(f"MetadataMappingDAO: deleted mapping {mapping_id}")
            return deleted
        except Exception as e:
            await self.session.rollback()
            logger.warning(f"MetadataMappingDAO: delete failed ({e})")
            return False
