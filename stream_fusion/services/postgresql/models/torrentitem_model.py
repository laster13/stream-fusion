from sqlalchemy import BigInteger, ForeignKey, String, Boolean, Integer, JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import ARRAY
from stream_fusion.services.postgresql.base import Base
from datetime import datetime
from typing import Optional, List
import hashlib
import json

from stream_fusion.logging_config import logger
from stream_fusion.utils.torrent.torrent_item import TorrentItem

class TorrentItemModel(Base):
    """Model for TorrentItem in PostgreSQL."""

    __tablename__ = "torrent_items"

    id: Mapped[str] = mapped_column(String(16), primary_key=True)
    raw_title: Mapped[str] = mapped_column(String, nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)  # Kept as BigInteger
    magnet: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    info_hash: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    link: Mapped[str] = mapped_column(String, nullable=False)
    seeders: Mapped[int] = mapped_column(Integer, nullable=False)
    languages: Mapped[List[str]] = mapped_column(ARRAY(String), nullable=False)
    indexer: Mapped[str] = mapped_column(String, nullable=False)
    privacy: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tmdb_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    file_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    files: Mapped[Optional[List[dict]]] = mapped_column(JSON, nullable=True)  # Kept as JSON
    torrent_download: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    trackers: Mapped[List[str]] = mapped_column(ARRAY(String), default=[])
    file_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    full_index: Mapped[Optional[List[dict]]] = mapped_column(JSON, nullable=True)  # Kept as JSON
    availability: Mapped[bool] = mapped_column(Boolean, default=False)

    parsed_data: Mapped[dict] = mapped_column(JSON, nullable=True)

    # Group of torrents representing the same content (same info_hash from different
    # indexers, or same title+size from trackers that re-hash).  Nullable — ungrouped
    # items work exactly as before.
    group_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey("torrent_groups.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)

    @staticmethod
    def generate_unique_id(raw_title: str, size: int, indexer: str = "cached", info_hash: str = "") -> str:
        unique_string = f"{raw_title}_{size}_{indexer}_{info_hash}"
        full_hash = hashlib.sha256(unique_string.encode()).hexdigest()
        return full_hash[:16]

    def __init__(self, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = self.generate_unique_id(
                kwargs.get('raw_title', ''),
                self._parse_size(kwargs.get('size', 0)),
                kwargs.get('indexer', 'cached'),
                kwargs.get('info_hash', '')
            )
        super().__init__(**kwargs)
        current_time = int(datetime.now().timestamp())
        if 'created_at' not in kwargs:
            self.created_at = current_time
        if 'updated_at' not in kwargs:
            self.updated_at = current_time

    @staticmethod
    def _remove_ed2k_from_files(value):
        """Remove ed2k bytes field from files/full_index for JSON serialization"""
        if not value:
            return value

        if isinstance(value, list):
            result = []
            for item in value:
                if isinstance(item, dict):
                    # Create a new dict without ed2k key
                    new_item = {k: v for k, v in item.items() if k != 'ed2k'}
                    result.append(new_item)
                else:
                    result.append(item)
            return result
        return value

    @classmethod
    def from_torrent_item(cls, torrent_item: TorrentItem):
        model_dict = {}
        for attr, value in torrent_item.__dict__.items():
            if hasattr(cls, attr):
                if attr == 'size':
                    model_dict[attr] = cls._parse_size(value)
                elif attr in ['files', 'full_index']:
                    # Remove ed2k bytes before JSON serialization
                    cleaned_value = cls._remove_ed2k_from_files(value)
                    model_dict[attr] = cls._parse_json(cleaned_value)
                elif attr == 'parsed_data':
                    if value:
                        try:
                            # Try to convert to dict if it's a Pydantic model
                            if hasattr(value, 'model_dump'):
                                model_dict[attr] = value.model_dump()
                            elif isinstance(value, dict):
                                model_dict[attr] = value
                            else:
                                # Convert to dict representation
                                model_dict[attr] = vars(value) if hasattr(value, '__dict__') else str(value)
                        except Exception as e:
                            logger.warning(f"Could not serialize parsed_data: {e}")
                            model_dict[attr] = None
                    else:
                        model_dict[attr] = None
                elif attr == "availability":
                    model_dict[attr] = False
                elif attr == "tmdb_id":
                    model_dict[attr] = int(value) if value is not None else None
                elif attr == "seeders":
                    model_dict[attr] = int(value) if value else 0
                elif attr in ["torrent_file", "torrent_file_path"]:
                    # Skip both torrent_file and torrent_file_path - not storing these in DB
                    pass
                else:
                    model_dict[attr] = value

        return cls(**model_dict)

    def to_torrent_item(self):
        from RTN.models import ParsedData
        from RTN import parse
        from stream_fusion.utils.torrent.torrent_item import TorrentItem

        torrent_item_dict = {}
        raw_title = None

        for attr, value in self.__dict__.items():
            if attr not in ['_sa_instance_state', 'created_at', 'updated_at']:
                if attr == 'raw_title':
                    raw_title = value
                    torrent_item_dict[attr] = value
                elif attr == 'parsed_data':
                    # Handle parsed_data conversion with validation
                    if value is None:
                        torrent_item_dict[attr] = None
                    elif isinstance(value, dict):
                        try:
                            torrent_item_dict[attr] = ParsedData(**value)
                        except Exception:
                            # If parsing dict fails, reparse from raw_title
                            torrent_item_dict[attr] = parse(raw_title) if raw_title else None
                    else:
                        # If it's a string or other type, reparse from raw_title
                        torrent_item_dict[attr] = parse(raw_title) if raw_title else None
                else:
                    torrent_item_dict[attr] = value

        # Supprimez les attributs qui ne sont pas dans TorrentItem
        valid_attrs = set(TorrentItem.__init__.__code__.co_varnames)
        torrent_item_dict = {k: v for k, v in torrent_item_dict.items() if k in valid_attrs}

        return TorrentItem(**torrent_item_dict)

    @staticmethod
    def _parse_size(size):
        if isinstance(size, str):
            try:
                return int(size)
            except ValueError:
                return 0
        return size

    @staticmethod
    def _parse_json(value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return value
