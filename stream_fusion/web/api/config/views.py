import json

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings
from stream_fusion.utils.string_encoding import encrypt_config, encodeb64, verify_csrf_token

router = APIRouter()


class ConfigEncodeRequest(BaseModel):
    config: dict


class ConfigEncodeResponse(BaseModel):
    token: str
    encrypted: bool  # False = degraded mode (CONFIG_SECRET_KEY not set)


@router.post("/encode", response_model=ConfigEncodeResponse)
async def encode_config(
    body: ConfigEncodeRequest,
    x_csrf_token: str = Header(alias="X-CSRF-Token"),
):
    """
    Encrypts the user configuration with Fernet and returns an opaque token.
    Protected by a signed CSRF token injected into the config page at render time.
    No decode endpoint exists — only the server can decrypt the token.
    """
    if not verify_csrf_token(x_csrf_token):
        raise HTTPException(status_code=403, detail="Invalid or expired CSRF token.")

    raw = json.dumps(body.config)

    if settings.config_secret_key:
        try:
            token = encrypt_config(raw)
            return ConfigEncodeResponse(token=token, encrypted=True)
        except Exception:
            logger.warning("Fernet encryption failed, falling back to Base64.")

    return ConfigEncodeResponse(token=encodeb64(raw), encrypted=False)
