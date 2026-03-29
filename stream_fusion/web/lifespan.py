import aiohttp

from yarl import URL
from fastapi import FastAPI
from redis import ConnectionPool
from typing import AsyncGenerator
from aiohttp_socks import ProxyConnector
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from stream_fusion.logging_config import configure_logging, logger
from stream_fusion.services.postgresql.base import Base
from stream_fusion.services.postgresql.models import load_all_models
from stream_fusion.settings import settings


def _setup_db(app: FastAPI) -> None:  # pragma: no cover
    """
    Creates connection to the database.

    This function creates SQLAlchemy engine instance,
    session_factory for creating sessions
    and stores them in the application's state property.

    :param app: fastAPI application.
    """
    engine = create_async_engine(str(settings.pg_url), echo=settings.pg_echo, pool_size=settings.pg_pool_size, max_overflow=settings.pg_max_overflow)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.db_engine = engine
    app.state.db_session_factory = session_factory



@asynccontextmanager
async def lifespan_setup(
    app: FastAPI,
) -> AsyncGenerator[None, None]:  # pragma: no cover
    """
    Lifespan context manager for FastAPI application.
    This function handles startup and shutdown events.
    :param app: the FastAPI application.
    :yield: None
    """
    app.middleware_stack = None
    _setup_db(app)
    app.middleware_stack = app.build_middleware_stack()

    if not settings.config_secret_key:
        logger.warning(
            "CONFIG_SECRET_KEY non configurée. Les URLs de configuration utiliseront "
            "un encodage Base64 réversible (non chiffré). Définir CONFIG_SECRET_KEY "
            "pour activer le chiffrement Fernet des tokens de configuration."
        )

    if settings.playback_proxy and settings.proxy_url:
        parsed_url = URL(settings.proxy_url)
        if parsed_url.scheme in ("socks5", "socks5h", "socks4", "http", "https"):
            connector = ProxyConnector.from_url(parsed_url, limit=100, limit_per_host=50)
        else:
            raise ValueError(f"Unsupported proxy scheme: {parsed_url.scheme}")
    else:
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)

    timeout = aiohttp.ClientTimeout(total=settings.aiohttp_timeout)
    app.state.http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    # Session dédiée aux services debrid (avec proxy si configuré)
    if settings.proxy_url:
        debrid_connector = ProxyConnector.from_url(str(settings.proxy_url), limit=100, limit_per_host=50)
    else:
        debrid_connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
    debrid_timeout = aiohttp.ClientTimeout(total=30)
    app.state.debrid_session = aiohttp.ClientSession(timeout=debrid_timeout, connector=debrid_connector)

    app.state.redis_pool = ConnectionPool(
        host=settings.redis_host, port=settings.redis_port, db=settings.redis_db, max_connections=200
    )

    yield

    # Shutdown actions
    if app.state.http_session:
        await app.state.http_session.close()
    if app.state.debrid_session:
        await app.state.debrid_session.close()
    if app.state.redis_pool:
        app.state.redis_pool.disconnect()
    await app.state.db_engine.dispose()
