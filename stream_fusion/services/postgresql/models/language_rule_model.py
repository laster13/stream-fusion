import time
from typing import Optional

from sqlalchemy import BigInteger, Boolean, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from stream_fusion.services.postgresql.base import Base


class LanguageRuleModel(Base):
    """Configurable rules for language detection, priority and release group matching."""

    __tablename__ = "language_rules"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    rule_type: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )  # 'french_pattern' | 'release_group' | 'code_mapping' | 'priority_group'
    key: Mapped[str] = mapped_column(String(100), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    extra: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    def __init__(self, **kwargs):
        now = int(time.time())
        if "created_at" not in kwargs:
            kwargs["created_at"] = now
        if "updated_at" not in kwargs:
            kwargs["updated_at"] = now
        super().__init__(**kwargs)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "rule_type": self.rule_type,
            "key": self.key,
            "value": self.value,
            "extra": self.extra,
            "description": self.description,
            "is_active": self.is_active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
