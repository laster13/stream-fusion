import time
from typing import Optional
from sqlalchemy import BigInteger, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column
from stream_fusion.services.postgresql.base import Base


class MetadataMappingModel(Base):
    """Manual IMDb → TMDB / title override mappings, managed from the admin UI."""

    __tablename__ = "metadata_mappings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    imdb_id: Mapped[str] = mapped_column(String(20), nullable=False, unique=True, index=True)
    tmdb_id: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)
    title_override: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    media_type: Mapped[str] = mapped_column(String(10), nullable=False, default="series")
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    search_titles: Mapped[Optional[list]] = mapped_column(ARRAY(String()), nullable=True)
    year_override: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    def __init__(self, **kwargs):
        now = int(time.time())
        if "created_at" not in kwargs:
            kwargs["created_at"] = now
        if "updated_at" not in kwargs:
            kwargs["updated_at"] = now
        super().__init__(**kwargs)
