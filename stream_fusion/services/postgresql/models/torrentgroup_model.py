from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from stream_fusion.services.postgresql.base import Base


class TorrentGroupModel(Base):
    """Model for a group of torrent items that represent the same content.

    Torrents are grouped when they share the same info_hash (different indexers)
    or when they have the same normalized title and near-identical file size
    (same content re-published by different trackers with a different hash).
    """

    __tablename__ = "torrent_groups"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # The reference info_hash for the group (most representative one).
    canonical_info_hash: Mapped[Optional[str]] = mapped_column(
        String(40), nullable=True, index=True
    )

    # Human-readable title (normalized) used for admin display.
    canonical_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Unified TMDB ID for the group — propagated from any member that has one.
    tmdb_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    # Denormalized count of torrent_items with this group_id (updated on assign/unassign).
    item_count: Mapped[int] = mapped_column(Integer, server_default="0", nullable=False)

    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    def __init__(self, **kwargs):
        if "created_at" not in kwargs:
            kwargs["created_at"] = int(datetime.now().timestamp())
        super().__init__(**kwargs)
