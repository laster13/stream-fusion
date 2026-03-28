from typing import Optional

from pydantic import BaseModel, Field

SUPPORTED_SERVICES = {"realdebrid", "alldebrid", "torbox"}


# ── Data endpoints ────────────────────────────────────────────────────────────

class PeerCacheRequest(BaseModel):
    hashes: list[str] = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Info-hashes to look up (max 200 per request)",
    )
    service: str = Field(
        ...,
        description="Debrid service identifier: realdebrid | alldebrid | torbox",
    )


class PeerCacheResponse(BaseModel):
    payload: str = Field(
        description=(
            "Fernet-encrypted JSON. Decrypted shape: "
            '{"service": str, "debrid_cache": {hash: cached_data_dict}}'
        )
    )


class PeerItemsRequest(BaseModel):
    hashes: list[str] = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Info-hashes to look up (max 200 per request)",
    )


class PeerItemsResponse(BaseModel):
    payload: str = Field(
        description=(
            "Fernet-encrypted JSON. Decrypted shape: "
            '{"torrent_items": {hash: torrent_item_dict}}'
        )
    )
