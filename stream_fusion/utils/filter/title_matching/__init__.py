"""
title_matching — module autonome de normalisation et matching de titres.

Usage:
    from stream_fusion.utils.filter.title_matching import (
        get_normalizer, get_matcher, get_lang_manager, initialize_title_matching
    )

Initialisation (dans le lifespan FastAPI) :
    await initialize_title_matching(redis_pool, db_session_factory)
"""

from __future__ import annotations

from redis import ConnectionPool, Redis
from sqlalchemy.ext.asyncio import async_sessionmaker

from stream_fusion.utils.filter.title_matching.cache import RedisRulesCache
from stream_fusion.utils.filter.title_matching.normalizer import TitleNormalizer
from stream_fusion.utils.filter.title_matching.matcher import TitleMatcher
from stream_fusion.utils.filter.title_matching.language_rules import LanguageRulesManager

__all__ = [
    "RedisRulesCache",
    "TitleNormalizer",
    "TitleMatcher",
    "LanguageRulesManager",
    "get_normalizer",
    "get_matcher",
    "get_lang_manager",
    "initialize_title_matching",
]

# Module-level singletons — initialized once at startup
_normalizer: TitleNormalizer | None = None
_matcher: TitleMatcher | None = None
_lang_manager: LanguageRulesManager | None = None


def get_normalizer() -> TitleNormalizer:
    if _normalizer is None:
        raise RuntimeError("title_matching module not initialized — call initialize_title_matching() first")
    return _normalizer


def get_matcher() -> TitleMatcher:
    if _matcher is None:
        raise RuntimeError("title_matching module not initialized — call initialize_title_matching() first")
    return _matcher


def get_lang_manager() -> LanguageRulesManager:
    if _lang_manager is None:
        raise RuntimeError("title_matching module not initialized — call initialize_title_matching() first")
    return _lang_manager


async def initialize_title_matching(
    redis_pool: ConnectionPool,
    db_session_factory: async_sessionmaker,
) -> None:
    """
    Initialize the title_matching module.
    Must be called once at application startup (in lifespan).
    """
    global _normalizer, _matcher, _lang_manager

    from stream_fusion.logging_config import logger
    from stream_fusion.utils.filter.title_matching.seeds import seed_if_empty

    redis_client = Redis(connection_pool=redis_pool)
    cache = RedisRulesCache(redis_client)

    # Seed DB if empty (first startup)
    await seed_if_empty(db_session_factory)

    # Build singletons
    _normalizer = TitleNormalizer(cache, db_session_factory)
    _lang_manager = LanguageRulesManager(cache, db_session_factory)
    _matcher = TitleMatcher(_normalizer)

    await _normalizer.initialize()
    await _lang_manager.initialize()

    logger.info("title_matching module initialized")
