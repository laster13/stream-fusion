from fastapi import Response
from fastapi.responses import RedirectResponse, JSONResponse

_BASE = "https://raw.githubusercontent.com/Telkaoss/stream-fusion/refs/heads/master/stream_fusion/static/videos"

_SLOTS_FULL_URL = f"{_BASE}/slots_full.mp4"
_ERROR_URL = f"{_BASE}/error.mp4"

_TORBOX_RATE_LIMIT_URL = f"{_BASE}/torbox_rate_limit.mp4"

_STATUS_VIDEO_URLS: dict[str, str] = {
    "DIFF_ISSUE": _SLOTS_FULL_URL,
    "STORE_LIMIT_EXCEEDED": _SLOTS_FULL_URL,
    "TORBOX_RATE_LIMIT": _TORBOX_RATE_LIMIT_URL,
}

_DEFAULT_URL = _ERROR_URL


def _normalize(key: str) -> str:
    return key.upper().replace("-", "_").replace(" ", "_") if key else ""


def get_status_video_url(status_keys: list, default_key: str = "UNKNOWN") -> str:
    for key in status_keys:
        if not key:
            continue
        url = _STATUS_VIDEO_URLS.get(_normalize(key))
        if url:
            return url
    return _STATUS_VIDEO_URLS.get(_normalize(default_key), _DEFAULT_URL)


def build_status_video_response(status_keys: list, default_key: str = "UNKNOWN") -> Response:
    return RedirectResponse(url=get_status_video_url(status_keys, default_key), status_code=302)
