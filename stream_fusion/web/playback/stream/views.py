import hashlib
import json
from urllib.parse import unquote
import redis.asyncio as redis
import asyncio
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from redis.exceptions import LockError
from fastapi.responses import RedirectResponse, StreamingResponse
from starlette.background import BackgroundTask

from stream_fusion.services.postgresql.dao.apikey_dao import APIKeyDAO
from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.redis.redis_config import get_redis_cache_dependency
from sqlalchemy.ext.asyncio import AsyncSession
from stream_fusion.utils.cache.local_redis import RedisCache
from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.debrid.get_debrid_service import (
    get_debrid_service,
    get_download_service,
)
from stream_fusion.utils.debrid.realdebrid import RealDebrid
from stream_fusion.utils.debrid.torbox import Torbox
from stream_fusion.utils.debrid.debrid_exceptions import DebridError
from stream_fusion.utils.debrid.status_video import build_status_video_response, get_status_video_url
from stream_fusion.utils.parse_config import parse_config
from stream_fusion.utils.string_encoding import decodeb64
from stream_fusion.utils.security import check_api_key
from stream_fusion.web.utils import get_client_ip
from stream_fusion.web.playback.stream.schemas import (
    ErrorResponse,
    HeadResponse,
)

router = APIRouter()

redis_client = redis.Redis(
    host=settings.redis_host, port=settings.redis_port, db=settings.redis_db
)

DOWNLOAD_IN_PROGRESS_FLAG = "DOWNLOAD_IN_PROGRESS"

_STREAM_LINK_TTL = 6 * 3600   # 6 hours — debrid links are valid well beyond 20 min
_DIRECT_LINK_TTL = 6 * 3600   # 6 hours — same rationale
_READY_TTL = 6 * 3600         # 6 hours — align with direct_link
_DOWNLOAD_TTL = 600            # 10 min  — in-progress flag, short by design
_LOCK_TTL = 60                 # 60 s    — mutex, short by design


def _user_hash(identifier: str) -> str:
    """16-char SHA-256 of the user identifier (api_key or IP). Never stored in plain text."""
    return hashlib.sha256(identifier.encode()).hexdigest()[:16]


def _content_hash(query: dict) -> str:
    """Stable 16-char hash of the stream content (info_hash + episode + service).

    - Same torrent + same episode + same service → same hash.
    - Naturally disambiguates episodes of the same series torrent.
    - Never contains magnet URIs or tracker credentials.
    """
    info_hash = query.get("info_hash", "")
    if not info_hash:
        magnet = query.get("magnet", "")
        if "btih:" in magnet:
            info_hash = magnet.split("btih:")[1].split("&")[0].split(":")[0].lower()
    content = f"{info_hash}:{query.get('season', '')}:{query.get('episode', '')}:{query.get('service', '')}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


async def _enforce_rate_limit(user_id: str) -> None:
    """Fixed-window rate limiter per user (api_key or IP).

    HEAD requests are excluded — call this only on the GET path.
    Uses redis_client.incr() + expire() for a simple, dependency-free counter.
    """
    key = f"ratelimit:playback:{_user_hash(user_id)}"
    count = await redis_client.incr(key)
    if count == 1:
        await redis_client.expire(key, settings.playback_limit_seconds)
    if count > settings.playback_limit_requests:
        logger.warning(f"Playback: Rate limit exceeded for user {_user_hash(user_id)}")
        raise HTTPException(status_code=429, detail="Too many requests. Please slow down.")


class ProxyStreamer:
    def __init__(self, request: Request, url: str, headers: dict):
        self.request = request
        self.url = url
        self.headers = headers
        self.response = None

    async def stream_content(self):
        async with self.request.app.state.http_session.get(
            self.url, headers=self.headers
        ) as self.response:
            async for chunk in self.response.content.iter_any():
                yield chunk

    async def close(self):
        if self.response:
            await self.response.release()
        logger.debug("Playback: Streaming connection closed")


async def handle_download(
    query: dict, config: dict, ip: str, redis_cache: RedisCache, debrid_session=None
) -> str:
    def _effective_service():
        """Return the debrid service to use for this download.

        If the query carries preferred_service=TorBox (set for special French indexers
        when TorBox is configured), force TorBox regardless of debridDownloader.
        Otherwise fall back to the user's configured download service.
        """
        if query.get("preferred_service") == "TorBox" and config.get("TBToken"):
            return get_debrid_service(config, "TB", debrid_session)
        return get_download_service(config, debrid_session)

    api_key = config.get("apiKey")
    user_id = api_key if api_key else ip
    uhash = _user_hash(user_id)
    chash = _content_hash(query)

    download_key = f"download:{uhash}:{chash}"
    ready_key = f"ready:{uhash}:{chash}"
    direct_link_key = f"direct_link:{uhash}:{chash}"

    if await redis_cache.get(ready_key) == "READY":
        logger.debug("Playback: File already marked as ready, checking for cached direct link")

        cached_direct_link = await redis_cache.get(direct_link_key)
        if cached_direct_link:
            logger.debug("Playback: Direct link found in cache, returning immediately")
            return cached_direct_link

        debrid_service = _effective_service()
        if debrid_service:
            try:
                direct_link = await debrid_service.get_stream_link(query, config, ip)
                if direct_link and direct_link != settings.no_cache_video_url:
                    await redis_cache.set(direct_link_key, direct_link, expiration=_DIRECT_LINK_TTL)
                    logger.debug("Playback: Direct link generated and cached")
                    return direct_link
            except Exception:
                pass

    download_flag = await redis_cache.get(download_key)
    if download_flag == DOWNLOAD_IN_PROGRESS_FLAG:
        logger.debug("Playback: Download in progress, checking if file is now ready")

        try:
            debrid_service = _effective_service()
            if debrid_service:
                try:
                    direct_link = await debrid_service.get_stream_link(query, config, ip)
                    if direct_link and direct_link != settings.no_cache_video_url:
                        logger.success("Playback: File is now ready! Clearing download flag and returning direct link")
                        await redis_cache.delete(download_key)
                        await redis_cache.set(ready_key, "READY", expiration=_READY_TTL)
                        await redis_cache.set(direct_link_key, direct_link, expiration=_DIRECT_LINK_TTL)
                        return direct_link
                except DebridError as debrid_err:
                    logger.warning(f"Playback: Debrid error in progress check {debrid_err.status_keys}, returning status video")
                    await redis_cache.delete(download_key)
                    error_url = get_status_video_url(debrid_err.status_keys, default_key="UNKNOWN")
                    return RedirectResponse(url=error_url, status_code=302)
                except Exception as link_error:
                    logger.debug(f"Playback: File not ready yet: {str(link_error)}")
        except Exception as e:
            logger.warning(f"Playback: Error checking download status: {str(e)}")

        return settings.no_cache_video_url

    await redis_cache.set(download_key, DOWNLOAD_IN_PROGRESS_FLAG, expiration=_DOWNLOAD_TTL)

    try:
        debrid_service = _effective_service()
        if not debrid_service:
            raise HTTPException(
                status_code=500, detail="Download service not available"
            )

        if isinstance(debrid_service, RealDebrid):
            torrent_id = await debrid_service.add_magnet_or_torrent_and_select(query, ip)
            logger.success(
                f"Playback: Added magnet or torrent to Real-Debrid: {torrent_id}"
            )
            if not torrent_id:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to add magnet or torrent to Real-Debrid",
                )
        elif isinstance(debrid_service, Torbox):
            magnet = query["magnet"]
            torrent_download = (
                unquote(query["torrent_download"])
                if query["torrent_download"] is not None
                else None
            )
            privacy = query.get("privacy", "private")
            torrent_info = await debrid_service.add_magnet_or_torrent(
                magnet, torrent_download, ip, privacy
            )
            logger.success(
                f"Playback: Added magnet or torrent to TorBox: {magnet[:50]}"
            )
            if not torrent_info:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to add magnet or torrent to TorBox",
                )
        else:
            magnet = query["magnet"]
            torrent_download = (
                unquote(query["torrent_download"])
                if query["torrent_download"] is not None
                else None
            )
            try:
                if await debrid_service.start_background_caching(magnet, query):
                    logger.success(
                        f"Playback: Started background caching for magnet: {magnet[:50]}"
                    )
                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to start background caching"
                    )
            except DebridError:
                raise
            except Exception as e:
                logger.error(f"Error starting background caching: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to start background caching: {str(e)}"
                )
        return settings.no_cache_video_url
    except Exception as e:
        await redis_cache.delete(download_key)
        logger.error(f"Playback: Error handling download: {str(e)}", exc_info=True)
        if "451" in str(e):
            logger.warning(f"Playback: Torrent banned (451), returning banned video")
            return settings.banned_video_url
        if isinstance(e, DebridError):
            logger.warning(f"Playback: Debrid error {e.status_keys}, returning status video")
            error_url = get_status_video_url(e.status_keys, default_key="UNKNOWN")
            return RedirectResponse(url=error_url, status_code=302)
        raise e


async def get_stream_link(
    decoded_query: str, config: dict, ip: str, redis_cache: RedisCache,
    uhash: str, chash: str, debrid_session=None, db_session=None
) -> str:
    logger.debug(f"Playback: Getting stream link for query: {decoded_query}, IP: {ip}")

    cache_key = f"stream_link:{uhash}:{chash}"
    cached_link = await redis_cache.get(cache_key)
    if cached_link:
        logger.debug(f"Playback: Stream link found in cache")
        return cached_link

    query = json.loads(decoded_query)
    service = query.get("service", False)

    if not service:
        logger.error("Playback: Service not found in query")
        raise HTTPException(status_code=500, detail="Service not found in query")

    debrid_service = get_debrid_service(config, service, debrid_session)

    logger.debug(f"Playback: Getting stream link from service {service}")
    link = await debrid_service.get_stream_link(query, config, ip)

    if link is None:
        logger.warning(f"Playback: Debrid service returned None for service {service}")
        raise DebridError("Debrid service failed to provide a valid stream link", error_code="UNKNOWN")

    if link != settings.no_cache_video_url:
        logger.debug(f"Playback: Caching new stream link")
        await redis_cache.set(cache_key, link, expiration=_STREAM_LINK_TTL)
        logger.debug(f"Playback: New stream link generated and cached")
    else:
        # False positive: hash was marked as cached but debrid returned no_cache_video_url
        # Invalidate both Redis and PG so the next search re-checks the API
        info_hash = debrid_service._extract_hash(query.get("magnet", ""))
        if info_hash:
            await debrid_service.invalidate_availability_cache(
                info_hash, redis_client, db_session
            )
        logger.debug("Playback: Stream link not cached (NO_CACHE_VIDEO_URL) — cache invalidated")
    return link


@router.api_route("/{config}/{query:path}", methods=["GET", "HEAD"], responses={500: {"model": ErrorResponse}})
async def get_playback(
    config: str,
    query: str,
    request: Request,
    redis_cache: RedisCache = Depends(get_redis_cache_dependency),
    apikey_dao: APIKeyDAO = Depends(),
    db: AsyncSession = Depends(get_db_session),
):
    if request.method == "HEAD":
        return Response(status_code=200)
    try:
        config = parse_config(config)
        api_key = config.get("apiKey")
        ip = get_client_ip(request)
        user_id = api_key if api_key else ip

        await _enforce_rate_limit(user_id)

        if api_key:
            try:
                await check_api_key(api_key, apikey_dao)
                logger.debug(f"Playback: Valid API key provided by {ip}")
            except HTTPException as e:
                logger.warning(f"Playback: Invalid API key provided by {ip}. Error: {e.detail}")
                raise e
        else:
            logger.debug(f"Playback: No API key provided by {ip}. Proceeding without API key validation.")

        if not query:
            logger.warning("Playback: Query is empty")
            raise HTTPException(status_code=400, detail="Query required.")

        decoded_query = decodeb64(query)
        logger.debug(f"Playback: Decoded query: {decoded_query}, Client IP: {ip}")

        query_dict = json.loads(decoded_query)
        logger.debug(f"Playback: Received playback request for query: {decoded_query}")
        service = query_dict.get("service", False)

        debrid_session = getattr(request.app.state, 'debrid_session', None)

        if service == "DL":
            result = await handle_download(query_dict, config, ip, redis_cache, debrid_session)
            if isinstance(result, Response):
                return result
            return RedirectResponse(url=result, status_code=status.HTTP_302_FOUND)

        uhash = _user_hash(user_id)
        chash = _content_hash(query_dict)
        fast_cache_key = f"stream_link:{uhash}:{chash}"

        fast_cached = await redis_cache.get(fast_cache_key)
        if fast_cached:
            logger.debug("Playback: Fast path cache hit, skipping lock")
            link = fast_cached
        else:
            lock_key = f"lock:stream:{uhash}:{chash}"
            lock = redis_client.lock(lock_key, timeout=_LOCK_TTL)

            try:
                if await lock.acquire(blocking=False):
                    logger.debug("Playback: Lock acquired, getting stream link")
                    link = await get_stream_link(decoded_query, config, ip, redis_cache, uhash, chash, debrid_session, db_session=db)
                else:
                    logger.debug("Playback: Lock not acquired, waiting for cached link")
                    for _ in range(60):
                        cached_link = await redis_cache.get(fast_cache_key)
                        if cached_link:
                            logger.debug("Playback: Cached link found while waiting")
                            link = cached_link
                            break
                        await asyncio.sleep(0.5)
                    else:
                        logger.warning("Playback: Timed out waiting for cached link")
                        raise HTTPException(
                            status_code=503,
                            detail="Service temporarily unavailable. Please try again.",
                        )
            finally:
                try:
                    await lock.release()
                    logger.debug("Playback: Lock released")
                except LockError:
                    logger.debug("Playback: Failed to release lock (already released)")

        use_proxy = settings.proxied_link

        if api_key:
            try:
                api_key_info = await apikey_dao.get_key_by_uuid(api_key)
                if api_key_info and hasattr(api_key_info, 'proxied_links'):
                    use_proxy = api_key_info.proxied_links
                    logger.debug(f"Playback: API key has proxied_links={use_proxy}")
            except Exception as e:
                logger.error(f"Playback: Error checking API key proxification status: {e}")

        if not use_proxy:
            logger.debug(f"Playback: Redirecting to non-proxied link")
            return RedirectResponse(
                url=link, status_code=status.HTTP_302_FOUND
            )

        if service == "TB":
            logger.debug("Playback: Bypass proxied link for TorBox")
            return RedirectResponse(url=link, status_code=status.HTTP_302_FOUND)

        logger.debug("Playback: Preparing to proxy stream")
        headers = {}
        range_header = request.headers.get("range")
        if range_header and "=" in range_header:
            logger.debug(f"Playback: Range header found: {range_header}")
            range_value = range_header.strip().split("=")[1]
            range_parts = range_value.split("-")
            start = int(range_parts[0]) if range_parts[0] else 0
            end = int(range_parts[1]) if len(range_parts) > 1 and range_parts[1] else ""
            headers["Range"] = f"bytes={start}-{end}"
            logger.debug(f"Playback: Range header set: {headers['Range']}")

        streamer = ProxyStreamer(request, link, headers)

        logger.debug(f"Playback: Initiating request to stream link")
        async with request.app.state.http_session.head(
            link, headers=headers
        ) as response:
            logger.debug(f"Playback: Response status: {response.status}")
            stream_headers = {
                "Content-Type": "video/mp4",
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Connection": "keep-alive",
                "Content-Disposition": "inline",
                "Access-Control-Allow-Origin": "*",
            }

            if response.status == 206:
                logger.debug("Playback: Partial content response")
                stream_headers["Content-Range"] = response.headers["Content-Range"]

            for header in ["Content-Length", "ETag", "Last-Modified"]:
                if header in response.headers:
                    stream_headers[header] = response.headers[header]
                    logger.debug(
                        f"Playback: Header set: {header}: {stream_headers[header]}"
                    )

            logger.debug("Playback: Preparing streaming response")
            return StreamingResponse(
                streamer.stream_content(),
                status_code=206 if "Range" in headers else 200,
                headers=stream_headers,
                background=BackgroundTask(streamer.close),
            )

    except DebridError as e:
        logger.warning(f"Playback: Debrid error {e.status_keys}, returning status video")
        error_url = get_status_video_url(e.status_keys, default_key="UNKNOWN")
        return RedirectResponse(url=error_url, status_code=302)
    except Exception as e:
        logger.error(f"Playback: Playback error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                detail="An error occurred while processing the request."
            ).model_dump(),
        )
