import hashlib

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Security
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_403_FORBIDDEN

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.dao.debridcache_dao import DebridCacheDAO
from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.settings import settings
from stream_fusion.utils.security.security_api_key import api_key_header, api_key_query
from stream_fusion.web.api.share.schemas import CacheCheckRequest, CacheCheckResponse

router = APIRouter()

redis_client = redis.Redis(
    host=settings.redis_host, port=settings.redis_port, db=settings.redis_db
)


def _key_hash(api_key: str) -> str:
    """16-char SHA-256 of the API key — never stored in plain text."""
    return hashlib.sha256(api_key.encode()).hexdigest()[:16]


async def _enforce_rate_limit(api_key: str) -> None:
    """Fixed-window rate limiter per API key for the share/cache endpoint."""
    key = f"ratelimit:share:cache:{_key_hash(api_key)}"
    count = await redis_client.incr(key)
    if count == 1:
        await redis_client.expire(key, settings.share_cache_limit_seconds)
    if count > settings.share_cache_limit_requests:
        logger.warning(f"Share/cache: rate limit exceeded for key {_key_hash(api_key)}")
        raise HTTPException(status_code=429, detail="Too many requests. Please slow down.")


@router.post(
    "",
    response_model=CacheCheckResponse,
    summary="Check debrid availability cache",
    description=(
        "Returns the subset of the provided info-hashes that are confirmed cached "
        "on the given debrid service. Hashes absent from the response are not in "
        "the cache. No file names or private data are ever returned."
    ),
)
async def check_cache(
    body: CacheCheckRequest,
    apikey_dao: APIKeyDAO = Depends(),
    query_param: str = Security(api_key_query),
    header_param: str = Security(api_key_header),
    db: AsyncSession = Depends(get_db_session),
) -> CacheCheckResponse:
    api_key = query_param or header_param
    if not api_key:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="An API key must be passed as query or header",
        )
    is_valid = await apikey_dao.check_key(api_key)
    if not is_valid:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Wrong, revoked, or expired API key.",
        )
    await _enforce_rate_limit(api_key)
    dao = DebridCacheDAO(db)
    cached = await dao.check_batch(body.hashes, body.service)
    cached_list = sorted(cached)
    logger.debug(
        f"Share/cache: {len(cached_list)}/{len(body.hashes)} hashes cached "
        f"on {body.service}"
    )
    return CacheCheckResponse(
        service=body.service,
        cached_hashes=cached_list,
        count=len(cached_list),
    )
