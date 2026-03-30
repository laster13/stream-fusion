import hashlib
import secrets
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as redis
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.schemas.apikey_schemas import APIKeyCreate
from stream_fusion.settings import settings
from stream_fusion.web.utils import get_client_ip

router = APIRouter()

_TEMPLATE_DIR = Path(__file__).parent.parent.parent / "static" / "register"
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))

_redis_client = redis.Redis(
    host=settings.redis_host, port=settings.redis_port, db=settings.redis_db
)

_REGISTER_RATE_LIMIT = 5     # max creations per IP
_REGISTER_RATE_WINDOW = 3600  # per hour (seconds)


def _ip_hash(ip: str) -> str:
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


async def _enforce_rate_limit(ip: str) -> bool:
    """Returns True if the request is allowed, False if rate limit exceeded."""
    key = f"ratelimit:register:{_ip_hash(ip)}"
    count = await _redis_client.incr(key)
    if count == 1:
        await _redis_client.expire(key, _REGISTER_RATE_WINDOW)
    return count <= _REGISTER_RATE_LIMIT


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/register", response_class=HTMLResponse, include_in_schema=False)
async def register_page(request: Request):
    """Serve the public registration page.

    Returns the enabled form when ``allow_public_key_registration`` is
    ``True``, otherwise a notice that registrations are closed.
    """
    template = "register.html" if settings.allow_public_key_registration else "disabled.html"
    return templates.TemplateResponse(template, {"request": request})


@router.post("/register", include_in_schema=False)
async def register_create_key(request: Request, apikey_dao: APIKeyDAO = Depends()):
    """Create a new API key via the public registration page.

    Guarded by the ``allow_public_key_registration`` setting and a
    Redis-based rate limiter (5 requests per IP per hour).
    """
    if not settings.allow_public_key_registration:
        return JSONResponse(
            status_code=403,
            content={"detail": "Les inscriptions sont fermées pour le moment."},
        )

    client_ip = get_client_ip(request)
    if not await _enforce_rate_limit(client_ip):
        logger.warning(f"Public registration: rate limit exceeded for IP {_ip_hash(client_ip)}")
        return JSONResponse(
            status_code=429,
            content={"detail": "Trop de tentatives. Veuillez réessayer dans 1 heure."},
        )

    logger.info("Public registration: creating new API key")
    try:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        suffix = secrets.token_hex(4)
        key_name = f"public-{date_str}-{suffix}"
        api_key = await apikey_dao.create_key(
            APIKeyCreate(name=key_name, never_expire=True)
        )
        logger.info("Public registration: API key created successfully")
        return JSONResponse(content={"api_key": str(api_key.api_key)})
    except Exception as e:
        logger.error(f"Public registration: error creating API key – {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Impossible de créer la clé API. Veuillez réessayer."},
        )
