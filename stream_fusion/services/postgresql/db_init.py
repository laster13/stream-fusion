from sqlalchemy.ext.asyncio import create_async_engine

from .base import Base
from .models.apikey_model import APIKeyModel
from stream_fusion.settings import settings


async def init_db():
    engine = create_async_engine(str(settings.pg_url))
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
