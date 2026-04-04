import hashlib
import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request as URLRequest, urlopen

from cachetools import TTLCache
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.models.apikey_model import APIKeyModel
from stream_fusion.settings import settings
from stream_fusion.utils.general import datetime_to_timestamp, timestamp_to_datetime
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.security.security_api_key import check_api_key
from stream_fusion.utils.string_encoding import generate_csrf_token
from stream_fusion.version import get_version
from stream_fusion.web.root.config.schemas import ManifestResponse

router = APIRouter()

templates = Jinja2Templates(directory="/app/stream_fusion/static")
stream_cache = TTLCache(maxsize=1000, ttl=3600)

SSD_STREAMFUSION_RESOLVE_URL = os.getenv(
    "SSD_STREAMFUSION_RESOLVE_URL",
    "http://172.17.0.1:8000/streamfusion/resolve",
)
STREAMFUSION_API_KEY_TTL_DAYS = int(os.getenv("STREAMFUSION_API_KEY_TTL_DAYS", "15"))
STREAMFUSION_API_KEY_NAME_PREFIX = os.getenv("STREAMFUSION_API_KEY_NAME_PREFIX", "ssd")


def _validate_ssd_token(token: str) -> bool:
    token = (token or "").strip()
    if not token:
        return False

    query = urlencode({"token": token})
    url = f"{SSD_STREAMFUSION_RESOLVE_URL}?{query}"

    req = URLRequest(url, method="GET")
    try:
        with urlopen(req, timeout=5) as resp:
            return 200 <= resp.status < 300
    except HTTPError:
        return False
    except URLError:
        return False
    except Exception:
        return False


def _api_key_name_from_token(token: str) -> str:
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
    return f"{STREAMFUSION_API_KEY_NAME_PREFIX}:{digest}"


async def _get_or_create_api_key_for_token(token: str, apikey_dao: APIKeyDAO) -> APIKeyModel:
    key_name = _api_key_name_from_token(token)
    now = datetime.now(timezone.utc)
    now_ts = datetime_to_timestamp(now)

    query = select(APIKeyModel).where(
        APIKeyModel.name == key_name,
        APIKeyModel.is_active == True,
        (APIKeyModel.never_expire == True) | (APIKeyModel.expiration_date > now_ts),
    )
    result = await apikey_dao.session.execute(query)
    existing = result.scalar_one_or_none()
    if existing:
        return existing

    expiration_timestamp = datetime_to_timestamp(now + timedelta(days=STREAMFUSION_API_KEY_TTL_DAYS))
    new_key = APIKeyModel(
        api_key=str(uuid.uuid4()),
        is_active=True,
        never_expire=False,
        expiration_date=expiration_timestamp,
        name=key_name,
        proxied_links=False,
    )
    apikey_dao.session.add(new_key)
    await apikey_dao.session.commit()
    await apikey_dao.session.refresh(new_key)
    return new_key


@router.get("/")
async def root():
    logger.info("Redirecting to /configure")
    return RedirectResponse(url="/configure")


@router.get("/configure")
@router.get("/{config}/configure")
async def configure(request: Request):
    logger.info("Serving configuration page")
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "rd_unique_account": settings.rd_unique_account,
            "ad_unique_account": settings.ad_unique_account,
            "ygg_unique_account": settings.ygg_unique_account,
            "jackett_enable": settings.jackett_enable,
            "tb_unique_account": settings.tb_unique_account,
            "c411_enable": settings.c411_enable,
            "torr9_enable": settings.torr9_enable,
            "lacale_enable": settings.lacale_enable,
            "c411_unique_account": settings.c411_unique_account,
            "torr9_unique_account": settings.torr9_unique_account,
            "lacale_unique_account": settings.lacale_unique_account,
            "generationfree_enable": settings.generationfree_enable,
            "generationfree_unique_account": settings.generationfree_unique_account,
            "abn_enable": settings.abn_enable,
            "abn_unique_account": settings.abn_unique_account,
            "g3mini_enable": settings.g3mini_enable,
            "g3mini_unique_account": settings.g3mini_unique_account,
            "theoldschool_enable": settings.theoldschool_enable,
            "theoldschool_unique_account": settings.theoldschool_unique_account,
            "pm_unique_account": settings.pm_unique_account,
            "allow_public_key_registration": settings.allow_public_key_registration,
            "csrf_token": generate_csrf_token(),
        },
    )


@router.post("/configure/api-key")
async def generate_streamfusion_api_key(request: Request, apikey_dao: APIKeyDAO = Depends()):
    token = (request.query_params.get("token") or "").strip()
    if not _validate_ssd_token(token):
        raise HTTPException(status_code=404, detail="Not found")

    key = await _get_or_create_api_key_for_token(token, apikey_dao)
    expiration_date = timestamp_to_datetime(key.expiration_date)

    return {
        "api_key": key.api_key,
        "name": key.name,
        "is_active": key.is_active,
        "never_expire": key.never_expire,
        "expiration_date": expiration_date.isoformat() if expiration_date else None,
        "proxied_links": key.proxied_links,
    }


@router.get("/manifest.json")
async def get_manifest():
    logger.info("Serving manifest.json")
    return ManifestResponse(
        id="community.limedrive.streamfusion",
        logo="https://i.ibb.co/ZRZcVBLd/SF-modern.png",
        version=str(get_version()),
        resources=[
            "catalog",
            {
                "name": "stream",
                "types": ["movie", "series"],
                "idPrefixes": ["tt"],
            },
        ],
        types=["movie", "series"],
        name="StreamFusion" + (" (dev)" if settings.develop else ""),
        description=(
            "StreamFusion enhances Stremio by integrating torrent indexers and debrid services, "
            "providing access to a vast array of cached torrent sources. This plugin seamlessly bridges "
            "Stremio with popular indexers and debrid platforms, offering users an expanded content "
            "library and a smooth streaming experience."
        ),
        catalogs=[
            {"type": "movie", "id": "latest_movies", "name": "Yggflix - Films Récents"},
            {"type": "movie", "id": "recently_added_movies", "name": "YGGtorrent - Films Récemment Ajoutés"},
            {"type": "series", "id": "latest_tv_shows", "name": "Yggflix - Séries Récentes"},
            {"type": "series", "id": "recently_added_tv_shows", "name": "YGGtorrent - Séries Récemment Ajoutées"},
        ],
        behaviorHints={"configurable": True, "configurationRequired": True},
        config=[{"key": "api_key", "title": "API Key", "type": "text", "required": True}],
    )


@router.get("/{config}/manifest.json")
async def get_manifest(config: str, apikey_dao: APIKeyDAO = Depends()):
    config = parse_config(config)
    api_key = config.get("apiKey")
    if api_key:
        await check_api_key(api_key, apikey_dao)
    else:
        logger.warning("Manifest: API key not found in config.")
        raise HTTPException(status_code=401, detail="API key not found in config.")

    yggflix_ctg = config.get("yggflixCtg", True)
    yggtorrent_ctg = config.get("yggtorrentCtg", True)

    catalogs = []

    if yggflix_ctg:
        catalogs.extend(
            [
                {"type": "movie", "id": "latest_movies", "name": "Yggflix"},
                {"type": "series", "id": "latest_tv_shows", "name": "Yggflix"},
            ]
        )

    if yggtorrent_ctg:
        catalogs.extend(
            [
                {
                    "type": "movie",
                    "id": "recently_added_movies",
                    "name": "YGGtorrent - Récemment Ajoutés",
                },
                {
                    "type": "series",
                    "id": "recently_added_tv_shows",
                    "name": "YGGtorrent - Récemment Ajoutées",
                },
            ]
        )

    logger.info("Serving manifest.json")
    return ManifestResponse(
        id="community.limedrive.streamfusion",
        logo="https://i.ibb.co/ZRZcVBLd/SF-modern.png",
        version=str(get_version()),
        resources=[
            "catalog",
            {
                "name": "stream",
                "types": ["movie", "series"],
                "idPrefixes": ["tt"],
            },
        ],
        types=["movie", "series"],
        name="StreamFusion" + (" (dev)" if settings.develop else ""),
        description=(
            "StreamFusion enhances Stremio by integrating torrent indexers and debrid services, "
            "providing access to a vast array of cached torrent sources. This plugin seamlessly bridges "
            "Stremio with popular indexers and debrid platforms, offering users an expanded content "
            "library and a smooth streaming experience."
        ),
        catalogs=catalogs,
    )
