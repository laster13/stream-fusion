from pydantic import BaseModel, Field, field_validator

SUPPORTED_SERVICES = {"realdebrid", "alldebrid", "torbox"}


class CacheCheckRequest(BaseModel):
    hashes: list[str] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Info-hashes to check (max 100 per request)",
    )
    service: str = Field(
        ...,
        description="Debrid service identifier: realdebrid | alldebrid | torbox",
    )

    @field_validator("service")
    @classmethod
    def validate_service(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in SUPPORTED_SERVICES:
            raise ValueError(f"service must be one of {sorted(SUPPORTED_SERVICES)}")
        return v

    @field_validator("hashes", mode="before")
    @classmethod
    def normalise_hashes(cls, v: list) -> list[str]:
        return [str(h).lower().strip() for h in v]


class CacheCheckResponse(BaseModel):
    service: str
    cached_hashes: list[str] = Field(
        description="Hashes confirmed cached on the given service. "
                    "Hashes absent from this list are not in the cache."
    )
    count: int = Field(description="Number of cached hashes found")
