from typing import Optional

from sqlalchemy import BigInteger, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from stream_fusion.services.postgresql.base import Base


class DebridCacheModel(Base):
    """Persistent cache of debrid availability results, shared across all users.

    Only confirmed-cached hashes are stored (no negative / not-cached entries).
    All data stored here has been sanitised: no private tokens, no announce URLs.
    """

    __tablename__ = "debrid_cache"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    info_hash: Mapped[str] = mapped_column(String(40), nullable=False)
    service: Mapped[str] = mapped_column(String(20), nullable=False)  # e.g. 'realdebrid', 'alldebrid'
    cached_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)  # sanitised file metadata
    checked_at: Mapped[int] = mapped_column(BigInteger, nullable=False)   # Unix timestamp
    expires_at: Mapped[int] = mapped_column(BigInteger, nullable=False)   # Unix timestamp

    __table_args__ = (
        UniqueConstraint("info_hash", "service", name="uq_debrid_cache_hash_service"),
    )
