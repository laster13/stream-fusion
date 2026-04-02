import time
from typing import Optional
from sqlalchemy import BigInteger, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from stream_fusion.services.postgresql.base import Base


class TmdbMismatchModel(Base):
    """Admin-reported (info_hash, tmdb_id) pairs that are known mismatches.

    When a torrent is flagged here, it will never be re-assigned to that
    TMDB ID by the automatic enrichment pipeline.
    """

    __tablename__ = "tmdb_mismatches"
    __table_args__ = (
        UniqueConstraint("info_hash", "tmdb_id", name="uq_mismatch_hash_tmdb"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    info_hash: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    tmdb_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    raw_title: Mapped[str] = mapped_column(String, nullable=False)
    indexer: Mapped[str] = mapped_column(String, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    def __init__(self, **kwargs):
        if "created_at" not in kwargs:
            kwargs["created_at"] = int(time.time())
        super().__init__(**kwargs)
