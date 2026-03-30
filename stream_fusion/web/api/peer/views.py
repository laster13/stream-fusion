import hashlib
import time

import redis.asyncio as redis_lib
from fastapi import APIRouter, Header, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED, HTTP_429_TOO_MANY_REQUESTS

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.debridcache_dao import DebridCacheDAO
from stream_fusion.services.postgresql.dao.torrentitem_dao import TorrentItemDAO
from stream_fusion.services.postgresql.dao.peerkey_dao import PeerKeyDAO
from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.settings import settings
from stream_fusion.utils.peer.crypto import verify_request, encrypt_payload
from stream_fusion.web.api.peer.schemas import (
    PeerCacheRequest,
    PeerCacheResponse,
    PeerItemsRequest,
    PeerItemsResponse,
    SUPPORTED_SERVICES,
)

router = APIRouter()

_redis = redis_lib.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
)


def _key_hash(key_id: str) -> str:
    """16-char SHA-256 of the key_id — used as Redis rate-limit key."""
    return hashlib.sha256(key_id.encode()).hexdigest()[:16]


async def _authenticate_peer(
    key_id: str,
    timestamp: str,
    signature: str,
    raw_body: bytes,
    db: AsyncSession,
):
    """Authenticate a peer request: verify key existence, HMAC, expiry, and rate limit.

    Returns the PeerKeyModel on success, raises HTTPException on failure.
    """
    dao = PeerKeyDAO(db)
    key = await dao.get_by_key_id(key_id)
    if not key:
        logger.debug(f"Peer auth: unknown or revoked key_id {key_id[:8]}…")
        raise HTTPException(HTTP_401_UNAUTHORIZED, "Unknown or revoked peer key")

    if key.expires_at and key.expires_at < int(time.time()):
        logger.debug(f"Peer auth: expired key {key_id[:8]}…")
        raise HTTPException(HTTP_401_UNAUTHORIZED, "Peer key expired")

    if not verify_request(key.secret, timestamp, signature, raw_body):
        logger.debug(f"Peer auth: invalid signature for key {key_id[:8]}…")
        raise HTTPException(HTTP_401_UNAUTHORIZED, "Invalid or expired peer signature")

    # Fixed-window rate limiting per key_id
    rl_key = f"ratelimit:peer:{_key_hash(key_id)}"
    count = await _redis.incr(rl_key)
    if count == 1:
        await _redis.expire(rl_key, key.rate_window)
    if count > key.rate_limit:
        logger.warning(f"Peer rate limit exceeded for key {key_id[:8]}… ({count}/{key.rate_limit})")
        raise HTTPException(HTTP_429_TOO_MANY_REQUESTS, "Peer rate limit exceeded")

    await dao.record_usage(key_id)
    return key


# ── Data endpoints ────────────────────────────────────────────────────────────

@router.post(
    "/check",
    response_model=PeerCacheResponse,
    summary="Peer cache check",
    description=(
        "Returns debrid availability data for the provided hashes as a Fernet-encrypted "
        "payload. Only authenticated peers (valid HMAC signature) can decrypt the response."
    ),
)
async def peer_check(
    request: Request,
    body: PeerCacheRequest,
    x_peer_key_id: str = Header(..., alias="X-Peer-Key-Id"),
    x_peer_timestamp: str = Header(..., alias="X-Peer-Timestamp"),
    x_peer_signature: str = Header(..., alias="X-Peer-Signature"),
    db: AsyncSession = Depends(get_db_session),
) -> PeerCacheResponse:
    if body.service not in SUPPORTED_SERVICES:
        raise HTTPException(HTTP_400_BAD_REQUEST, f"Unsupported service: {body.service}")

    raw_body = await request.body()
    key = await _authenticate_peer(x_peer_key_id, x_peer_timestamp, x_peer_signature, raw_body, db)

    debrid_data = await DebridCacheDAO(db).get_batch(body.hashes, body.service)

    logger.debug(
        f"Peer/check: {len(debrid_data)}/{len(body.hashes)} hashes found "
        f"for {body.service} (key {x_peer_key_id[:8]}…)"
    )
    return PeerCacheResponse(
        payload=encrypt_payload(key.secret, {
            "service": body.service,
            "debrid_cache": debrid_data,
        })
    )


@router.post(
    "/items",
    response_model=PeerItemsResponse,
    summary="Peer torrent items",
    description=(
        "Returns torrent metadata (title, files, languages, quality) for the provided hashes "
        "as a Fernet-encrypted payload."
    ),
)
async def peer_items(
    request: Request,
    body: PeerItemsRequest,
    x_peer_key_id: str = Header(..., alias="X-Peer-Key-Id"),
    x_peer_timestamp: str = Header(..., alias="X-Peer-Timestamp"),
    x_peer_signature: str = Header(..., alias="X-Peer-Signature"),
    db: AsyncSession = Depends(get_db_session),
) -> PeerItemsResponse:
    raw_body = await request.body()
    key = await _authenticate_peer(x_peer_key_id, x_peer_timestamp, x_peer_signature, raw_body, db)

    torrent_data = await TorrentItemDAO(db).get_batch_by_hashes(body.hashes)

    logger.debug(
        f"Peer/items: {len(torrent_data)}/{len(body.hashes)} items found "
        f"(key {x_peer_key_id[:8]}…)"
    )
    return PeerItemsResponse(
        payload=encrypt_payload(key.secret, {
            "torrent_items": torrent_data,
        })
    )
