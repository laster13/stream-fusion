import time

from sqlalchemy import BigInteger, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from stream_fusion.services.postgresql.base import Base


class AppSettingModel(Base):
    """Admin-managed overrides for application settings.

    One row per setting key. Value is stored as UTF-8 text;
    type coercion is handled by SettingsService at read time.
    """

    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_by: Mapped[str] = mapped_column(String(100), nullable=False, default="admin")

    def __init__(self, **kwargs):
        if "updated_at" not in kwargs:
            kwargs["updated_at"] = int(time.time())
        super().__init__(**kwargs)
