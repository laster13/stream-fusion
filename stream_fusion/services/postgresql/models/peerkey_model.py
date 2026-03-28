import time
from typing import Optional

from sqlalchemy import BigInteger, Boolean, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from stream_fusion.services.postgresql.base import Base


class PeerKeyModel(Base):
    """Application key for authenticated peer-to-peer cache sharing.

    Each peer instance is granted a unique (key_id, secret) pair.
    The secret is used for both HMAC request authentication and Fernet
    response encryption — only the holder of the secret can authenticate
    requests and decrypt responses.
    """

    __tablename__ = "peer_keys"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key_id: Mapped[str] = mapped_column(String(36), nullable=False)           # UUID4 — sent in header
    secret: Mapped[str] = mapped_column(String(128), nullable=False)          # 64-char hex (256 bits)
    name: Mapped[str] = mapped_column(String(100), nullable=False)            # human label
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    rate_limit: Mapped[int] = mapped_column(Integer, default=60, nullable=False)   # max req per window
    rate_window: Mapped[int] = mapped_column(Integer, default=60, nullable=False)  # window in seconds
    expires_at: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)   # None = never expires
    last_used_at: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    total_queries: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=lambda: int(time.time())
    )

    __table_args__ = (
        UniqueConstraint("key_id", name="uq_peer_keys_key_id"),
    )
