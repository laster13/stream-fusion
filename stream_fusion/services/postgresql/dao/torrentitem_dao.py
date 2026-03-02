from typing import List, Optional
from fastapi import Depends
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone, timedelta

from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.postgresql.models.torrentitem_model import TorrentItemModel
from stream_fusion.logging_config import logger
from stream_fusion.utils.torrent.torrent_item import TorrentItem

class TorrentItemDAO:

    def __init__(self, session: AsyncSession = Depends(get_db_session)) -> None:
        self.session = session

    async def create_torrent_item(self, torrent_item: TorrentItem, id: str) -> TorrentItemModel:
        async with self.session.begin():
            try:
                new_item = TorrentItemModel.from_torrent_item(torrent_item)
                new_item.id = id
                self.session.add(new_item)
                await self.session.flush()
                await self.session.refresh(new_item)
                logger.debug(f"TorrentItemDAO: Created new TorrentItem: {new_item.id}")
                return new_item
            except Exception as e:
                if "duplicate key value violates unique constraint" not in str(e):
                    logger.error(f"TorrentItemDAO: Error creating TorrentItem: {str(e)}")

    async def get_all_torrent_items(self, limit: int, offset: int) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).limit(limit).offset(offset)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.debug(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems: {str(e)}")

    async def get_torrent_item_by_id(self, item_id: str) -> Optional[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()
                if db_item:
                    logger.debug(f"TorrentItemDAO: Retrieved TorrentItem: {item_id}")
                    return db_item
                else:
                    logger.debug(f"TorrentItemDAO: TorrentItem not found: {item_id}")
                    return None
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItem {item_id}: {str(e)}")
                return None

    async def update_torrent_item(self, item_id: str, torrent_item: TorrentItem) -> TorrentItemModel:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if not db_item:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for update: {item_id}")
                    return None

                for key, value in torrent_item.__dict__.items():
                    if key == 'size' and value is not None:
                        try:
                            value = int(value)
                        except (ValueError, TypeError):
                            logger.warning(f"TorrentItemDAO: Invalid size value '{value}' for item {item_id}, skipping")
                            continue
                    setattr(db_item, key, value)

                db_item.updated_at = int(datetime.now(timezone.utc).timestamp())
                await self.session.flush()
                await self.session.refresh(db_item)
                logger.debug(f"TorrentItemDAO: Updated TorrentItem: {item_id}")
                return db_item
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating TorrentItem {item_id}: {str(e)}")
                return None

    async def delete_torrent_item(self, item_id: str) -> bool:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if db_item:
                    await self.session.delete(db_item)
                    logger.debug(f"TorrentItemDAO: Deleted TorrentItem: {item_id}")
                    return True
                else:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for deletion: {item_id}")
                    return False
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error deleting TorrentItem {item_id}: {str(e)}")
                return False

    async def get_torrent_items_by_info_hash(self, info_hash: str) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.info_hash == info_hash)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.debug(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems with info_hash: {info_hash}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by info_hash {info_hash}: {str(e)}")
                return None

    async def get_torrent_items_by_indexer(self, indexer: str) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.indexer == indexer)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.debug(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems from indexer: {indexer}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by indexer {indexer}: {str(e)}")
                return None

    async def is_torrent_item_cached(self, item_id: str) -> bool:
        async with self.session.begin():
            try:
                query = select(func.count()).where(TorrentItemModel.id == item_id)
                result = await self.session.execute(query)
                count = result.scalar_one()
                is_cached = count > 0
                logger.debug(f"TorrentItemDAO: TorrentItem {item_id} {'is' if is_cached else 'is not'} in cache")
                return is_cached
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error checking if TorrentItem {item_id} is cached: {str(e)}")
                return None

    async def get_torrent_items_by_type(self, item_type: str) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.type == item_type)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.debug(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems of type: {item_type}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by type {item_type}: {str(e)}")
                return None

    async def get_torrent_items_by_availability(self, available: bool) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.availability == available)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.debug(f"TorrentItemDAO: Retrieved {len(items)} TorrentItems with availability: {available}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error retrieving TorrentItems by availability {available}: {str(e)}")
                return None

    async def search_by_info_hash(self, info_hash: str) -> Optional[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.info_hash == info_hash).limit(1)
                result = await self.session.execute(query)
                item = result.scalar_one_or_none()
                if item:
                    logger.debug(f"TorrentItemDAO: Found torrent with info_hash: {info_hash}")
                return item
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error searching by info_hash {info_hash}: {str(e)}")
                return None

    async def search_by_tmdb_id(self, tmdb_id: int) -> List[TorrentItemModel]:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.tmdb_id == tmdb_id)
                result = await self.session.execute(query)
                items = result.scalars().all()
                logger.debug(f"TorrentItemDAO: Found {len(items)} torrents for TMDB ID: {tmdb_id}")
                return items
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error searching by TMDB ID {tmdb_id}: {str(e)}")
                return []

    async def update_torrent_file_path(self, torrent_id: str, file_path: str) -> bool:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == torrent_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if not db_item:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for file path update: {torrent_id}")
                    return False

                db_item.torrent_file_path = file_path
                db_item.updated_at = int(datetime.now(timezone.utc).timestamp())
                await self.session.flush()
                await self.session.refresh(db_item)
                logger.debug(f"TorrentItemDAO: Updated torrent_file_path for {torrent_id}: {file_path}")
                return True
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating torrent_file_path for {torrent_id}: {str(e)}")
                return False

    async def update_torrent_file_path_and_tmdb_id(self, torrent_id: str, file_path: str, tmdb_id: Optional[int]) -> bool:
        async with self.session.begin():
            try:
                query = select(TorrentItemModel).where(TorrentItemModel.id == torrent_id)
                result = await self.session.execute(query)
                db_item = result.scalar_one_or_none()

                if not db_item:
                    logger.warning(f"TorrentItemDAO: TorrentItem not found for update: {torrent_id}")
                    return False

                db_item.torrent_file_path = file_path
                if tmdb_id:
                    db_item.tmdb_id = tmdb_id
                db_item.updated_at = int(datetime.now(timezone.utc).timestamp())
                await self.session.flush()
                await self.session.refresh(db_item)
                logger.debug(f"TorrentItemDAO: Updated torrent_file_path ({file_path}) and TMDB ID ({tmdb_id}) for {torrent_id}")
                return True
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating torrent_file_path and tmdb_id for {torrent_id}: {str(e)}")
                return False

    async def update_tmdb_id_by_raw_title(self, raw_title: str, tmdb_id: int) -> int:
        async with self.session.begin():
            try:
                stmt = (
                    update(TorrentItemModel)
                    .where(TorrentItemModel.raw_title == raw_title)
                    .where(TorrentItemModel.tmdb_id.is_(None))
                    .values(
                        tmdb_id=tmdb_id,
                        updated_at=int(datetime.now(timezone.utc).timestamp())
                    )
                )
                result = await self.session.execute(stmt)
                await self.session.flush()
                row_count = result.rowcount
                logger.debug(f"TorrentItemDAO: Updated {row_count} torrents with raw_title '{raw_title}' to tmdb_id {tmdb_id}")
                return row_count
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error updating tmdb_id for raw_title '{raw_title}': {str(e)}")
                return 0

    async def get_latest_tmdb_ids(self, item_type: str, limit: int = 50) -> List[int]:
        async with self.session.begin():
            try:
                query = select(
                    TorrentItemModel.tmdb_id,
                    func.min(TorrentItemModel.created_at).label('first_seen')
                ).where(
                    TorrentItemModel.type == item_type,
                    TorrentItemModel.tmdb_id.isnot(None),
                    TorrentItemModel.indexer == "Yggtorrent - API",
                    TorrentItemModel.languages.any('fr')
                ).group_by(
                    TorrentItemModel.tmdb_id
                ).order_by(
                    func.min(TorrentItemModel.created_at).desc()
                ).limit(limit)

                result = await self.session.execute(query)
                rows = result.fetchall()
                tmdb_ids = [row.tmdb_id for row in rows]
                logger.debug(f"TorrentItemDAO: Retrieved {len(tmdb_ids)} latest TMDB IDs (FR/MULTI) for {item_type}")
                return tmdb_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error getting latest TMDB IDs for {item_type}: {str(e)}")
                return []

    async def get_recently_added_tmdb_ids(self, item_type: str, limit: int = 50) -> List[int]:
        async with self.session.begin():
            try:
                query = select(
                    TorrentItemModel.tmdb_id,
                    func.max(TorrentItemModel.created_at).label('last_added')
                ).where(
                    TorrentItemModel.type == item_type,
                    TorrentItemModel.tmdb_id.isnot(None),
                    TorrentItemModel.indexer == "Yggtorrent - API"
                ).group_by(
                    TorrentItemModel.tmdb_id
                ).order_by(
                    func.max(TorrentItemModel.created_at).desc()
                ).limit(limit)

                result = await self.session.execute(query)
                rows = result.fetchall()
                tmdb_ids = [row.tmdb_id for row in rows]
                logger.debug(f"TorrentItemDAO: Retrieved {len(tmdb_ids)} recently added TMDB IDs for {item_type}")
                return tmdb_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error getting recently added TMDB IDs for {item_type}: {str(e)}")
                return []

    async def get_series_with_new_episodes(self, recent_days: int = 7, limit: int = 50) -> List[int]:
        async with self.session.begin():
            try:
                from sqlalchemy.sql.expression import literal_column

                cutoff_timestamp = int((datetime.now(timezone.utc) - timedelta(days=recent_days)).timestamp())

                subquery = select(
                    TorrentItemModel.tmdb_id,
                    literal_column("parsed_data->>'seasons'").label('season_json'),
                    literal_column("parsed_data->>'episodes'").label('episode_json'),
                    func.min(TorrentItemModel.created_at).label('first_seen')
                ).where(
                    TorrentItemModel.type == 'series',
                    TorrentItemModel.tmdb_id.isnot(None),
                    TorrentItemModel.parsed_data.isnot(None)
                ).group_by(
                    TorrentItemModel.tmdb_id,
                    literal_column("parsed_data->>'seasons'"),
                    literal_column("parsed_data->>'episodes'")
                ).having(
                    func.min(TorrentItemModel.created_at) >= cutoff_timestamp
                ).subquery()

                query = select(
                    subquery.c.tmdb_id,
                    func.max(subquery.c.first_seen).label('latest_new_episode')
                ).group_by(
                    subquery.c.tmdb_id
                ).order_by(
                    func.max(subquery.c.first_seen).desc()
                ).limit(limit)

                result = await self.session.execute(query)
                rows = result.fetchall()
                tmdb_ids = [row.tmdb_id for row in rows]
                logger.debug(f"TorrentItemDAO: Retrieved {len(tmdb_ids)} series with new episodes (last {recent_days} days)")
                return tmdb_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error getting series with new episodes: {str(e)}")
                return []

    async def filter_existing_tmdb_ids(self, tmdb_ids: List[int], item_type: str, recent_days: Optional[int] = None, sort_by_added: bool = False, return_episode_info: bool = False):
        if not tmdb_ids:
            return []

        async with self.session.begin():
            try:
                conditions = [
                    TorrentItemModel.tmdb_id.in_(tmdb_ids),
                    TorrentItemModel.type == item_type,
                    TorrentItemModel.indexer == "Yggtorrent - API",
                ]

                if recent_days is not None:
                    cutoff_timestamp = int((datetime.now(timezone.utc) - timedelta(days=recent_days)).timestamp())

                    if item_type == "series":
                        from sqlalchemy.sql.expression import literal_column

                        subquery = select(
                            TorrentItemModel.tmdb_id,
                            literal_column("parsed_data->>'seasons'").label('season_json'),
                            literal_column("parsed_data->>'episodes'").label('episode_json'),
                            func.min(TorrentItemModel.created_at).label('first_seen')
                        ).where(
                            TorrentItemModel.tmdb_id.in_(tmdb_ids),
                            TorrentItemModel.type == item_type,
                            TorrentItemModel.parsed_data.isnot(None)
                        ).group_by(
                            TorrentItemModel.tmdb_id,
                            literal_column("parsed_data->>'seasons'"),
                            literal_column("parsed_data->>'episodes'")
                        ).having(
                            func.min(TorrentItemModel.created_at) >= cutoff_timestamp
                        ).subquery()

                        query = select(subquery.c.tmdb_id.distinct())
                    else:
                        conditions.append(TorrentItemModel.created_at >= cutoff_timestamp)
                        query = select(TorrentItemModel.tmdb_id.distinct()).where(*conditions)
                else:
                    from sqlalchemy import or_
                    conditions.append(or_(TorrentItemModel.languages.any('fr'), TorrentItemModel.languages.any('multi')))
                    if sort_by_added:
                        vostfr_conditions = conditions + [
                            ~TorrentItemModel.raw_title.ilike('%VOSTFR%'),
                            ~TorrentItemModel.raw_title.ilike('%FANSUB%'),
                            ~TorrentItemModel.raw_title.ilike('%SUBFRENCH%'),
                        ]
                        if item_type == "series":
                            from sqlalchemy.sql.expression import literal_column

                            subquery = select(
                                TorrentItemModel.tmdb_id,
                                literal_column("parsed_data->>'seasons'").label('season_json'),
                                literal_column("parsed_data->>'episodes'").label('episode_json'),
                                func.min(TorrentItemModel.created_at).label('first_seen')
                            ).where(
                                *vostfr_conditions,
                                TorrentItemModel.parsed_data.isnot(None)
                            ).group_by(
                                TorrentItemModel.tmdb_id,
                                literal_column("parsed_data->>'seasons'"),
                                literal_column("parsed_data->>'episodes'")
                            ).subquery()

                            cutoff_30d = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())

                            if return_episode_info:
                                query = select(
                                    subquery.c.tmdb_id,
                                    subquery.c.season_json,
                                    subquery.c.episode_json,
                                    subquery.c.first_seen
                                ).where(
                                    subquery.c.first_seen >= cutoff_30d
                                ).distinct(
                                    subquery.c.tmdb_id
                                ).order_by(
                                    subquery.c.tmdb_id,
                                    subquery.c.first_seen.desc()
                                )
                            else:
                                query = select(
                                    subquery.c.tmdb_id,
                                    func.max(subquery.c.first_seen).label('latest_new_episode')
                                ).group_by(
                                    subquery.c.tmdb_id
                                ).having(
                                    func.max(subquery.c.first_seen) >= cutoff_30d
                                ).order_by(
                                    func.max(subquery.c.first_seen).desc()
                                )
                        else:
                            query = select(
                                TorrentItemModel.tmdb_id,
                                func.min(TorrentItemModel.created_at).label('first_seen')
                            ).where(*vostfr_conditions).group_by(
                                TorrentItemModel.tmdb_id
                            ).order_by(
                                func.min(TorrentItemModel.created_at).desc()
                            )
                    else:
                        query = select(TorrentItemModel.tmdb_id.distinct()).where(*conditions)

                result = await self.session.execute(query)
                rows = result.fetchall()

                if return_episode_info and item_type == "series" and sort_by_added:
                    episode_data = []
                    for row in rows:
                        episode_data.append({
                            'tmdb_id': row[0],
                            'season': row[1],
                            'episode': row[2],
                            'first_seen': row[3]
                        })
                    episode_data.sort(key=lambda x: x['first_seen'], reverse=True)
                    logger.debug(f"TorrentItemDAO: Filtered {len(tmdb_ids)} TMDB IDs to {len(episode_data)} with episode info for {item_type}")
                    return episode_data
                elif sort_by_added and recent_days is None:
                    filtered_ids = [row[0] for row in rows]
                else:
                    existing_ids = {row[0] for row in rows}
                    filtered_ids = [tid for tid in tmdb_ids if tid in existing_ids]

                recent_info = f" (recent {recent_days}d, by episode)" if recent_days and item_type == "series" else (f" (recent {recent_days}d)" if recent_days else "")
                sort_info = ", sorted by added date" if sort_by_added else ""
                logger.debug(f"TorrentItemDAO: Filtered {len(tmdb_ids)} TMDB IDs to {len(filtered_ids)} existing (FR/MULTI{recent_info}{sort_info}) for {item_type}")
                return filtered_ids
            except Exception as e:
                logger.error(f"TorrentItemDAO: Error filtering TMDB IDs for {item_type}: {str(e)}")
                return []
