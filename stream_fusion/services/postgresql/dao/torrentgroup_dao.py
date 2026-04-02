"""DAO for torrent_groups table.

Handles group creation, item assignment, TMDB-ID propagation and batch-grouping
strategies used both by the automatic ingestion path and admin maintenance actions.
"""
from collections import Counter
from datetime import datetime
from typing import Optional

from fastapi import Depends
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from stream_fusion.logging_config import logger
from stream_fusion.services.postgresql.dependencies import get_db_session
from stream_fusion.services.postgresql.models.torrentgroup_model import TorrentGroupModel
from stream_fusion.services.postgresql.models.torrentitem_model import TorrentItemModel


class TorrentGroupDAO:
    """Data-access layer for torrent_groups and the group_id column on torrent_items."""

    def __init__(self, session: AsyncSession = Depends(get_db_session)) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Basic group CRUD
    # ------------------------------------------------------------------

    async def create_group(
        self,
        canonical_info_hash: Optional[str] = None,
        canonical_title: Optional[str] = None,
        tmdb_id: Optional[int] = None,
    ) -> TorrentGroupModel:
        """Insert a new group row and return it."""
        async with self.session.begin():
            group = TorrentGroupModel(
                canonical_info_hash=canonical_info_hash,
                canonical_title=canonical_title,
                tmdb_id=tmdb_id,
                created_at=int(datetime.now().timestamp()),
            )
            self.session.add(group)
            await self.session.flush()
            await self.session.refresh(group)
            logger.debug(f"TorrentGroupDAO: Created group id={group.id} hash={canonical_info_hash}")
            return group

    async def find_group_by_info_hash(self, info_hash: str) -> Optional[TorrentGroupModel]:
        """Return the group whose canonical_info_hash matches, if any."""
        async with self.session.begin():
            result = await self.session.execute(
                select(TorrentGroupModel).where(
                    TorrentGroupModel.canonical_info_hash == info_hash.lower()
                )
            )
            return result.scalar_one_or_none()

    async def find_group_by_item_id(self, item_id: str) -> Optional[TorrentGroupModel]:
        """Return the group that contains the given torrent_item id, if any."""
        async with self.session.begin():
            result = await self.session.execute(
                select(TorrentGroupModel)
                .join(TorrentItemModel, TorrentItemModel.group_id == TorrentGroupModel.id)
                .where(TorrentItemModel.id == item_id)
            )
            return result.scalar_one_or_none()

    async def get_groups_paginated(
        self, limit: int = 100, offset: int = 0
    ) -> list[TorrentGroupModel]:
        async with self.session.begin():
            result = await self.session.execute(
                select(TorrentGroupModel).order_by(TorrentGroupModel.id).limit(limit).offset(offset)
            )
            return list(result.scalars().all())

    # ------------------------------------------------------------------
    # Item assignment
    # ------------------------------------------------------------------

    async def assign_items_to_group(self, group_id: int, item_ids: list[str]) -> int:
        """Assign multiple torrent_items to a group, returning the count updated."""
        if not item_ids:
            return 0
        try:
            stmt = (
                update(TorrentItemModel)
                .where(TorrentItemModel.id.in_(item_ids))
                .values(group_id=group_id)
            )
            result = await self.session.execute(stmt)
            await self.session.flush()
            count = result.rowcount
            # Update denormalized item_count
            await self._refresh_item_count(group_id)
            logger.debug(f"TorrentGroupDAO: Assigned {count} items to group {group_id}")
            return count
        except Exception as e:
            logger.error(f"TorrentGroupDAO: Error assigning items to group {group_id}: {e}")
            return 0

    async def assign_item_to_group(self, group_id: int, item_id: str) -> bool:
        """Assign a single torrent_item to a group."""
        try:
            stmt = (
                update(TorrentItemModel)
                .where(TorrentItemModel.id == item_id)
                .values(group_id=group_id)
            )
            await self.session.execute(stmt)
            await self.session.flush()
            await self._refresh_item_count(group_id)
            logger.debug(f"TorrentGroupDAO: Assigned item {item_id} to group {group_id}")
            return True
        except Exception as e:
            logger.error(f"TorrentGroupDAO: Error assigning item {item_id} to group {group_id}: {e}")
            return False

    async def _refresh_item_count(self, group_id: int) -> None:
        """Recompute and persist the denormalized item_count for a group."""
        try:
            count_result = await self.session.execute(
                select(func.count()).where(TorrentItemModel.group_id == group_id)
            )
            cnt = count_result.scalar_one()
            await self.session.execute(
                update(TorrentGroupModel)
                .where(TorrentGroupModel.id == group_id)
                .values(item_count=cnt)
            )
            await self.session.flush()
        except Exception as e:
            logger.warning(f"TorrentGroupDAO: Could not refresh item_count for group {group_id}: {e}")

    # ------------------------------------------------------------------
    # TMDB propagation
    # ------------------------------------------------------------------

    async def propagate_tmdb_within_group(self, group_id: int) -> int:
        """Propagate the consensus tmdb_id to all group members that lack one.

        Consensus = most-frequent tmdb_id among members that already have one.
        Skips the group if no member has a tmdb_id yet.
        Returns the number of rows updated.
        """
        try:
            # Load all member tmdb_ids
            rows = await self.session.execute(
                select(TorrentItemModel.tmdb_id).where(TorrentItemModel.group_id == group_id)
            )
            all_tmdb = [r[0] for r in rows.fetchall() if r[0] is not None]
            if not all_tmdb:
                return 0

            consensus = Counter(all_tmdb).most_common(1)[0][0]

            # Propagate to members without a tmdb_id
            result = await self.session.execute(
                update(TorrentItemModel)
                .where(TorrentItemModel.group_id == group_id)
                .where(TorrentItemModel.tmdb_id.is_(None))
                .values(
                    tmdb_id=consensus,
                    updated_at=int(datetime.now().timestamp()),
                )
            )
            await self.session.flush()
            updated = result.rowcount

            # Keep the group row itself in sync
            await self.session.execute(
                update(TorrentGroupModel)
                .where(TorrentGroupModel.id == group_id)
                .values(tmdb_id=consensus)
            )
            await self.session.flush()

            if updated:
                logger.debug(
                    f"TorrentGroupDAO: Propagated tmdb_id={consensus} to {updated} items in group {group_id}"
                )
            return updated
        except Exception as e:
            logger.error(f"TorrentGroupDAO: Error propagating tmdb_id in group {group_id}: {e}")
            return 0

    async def update_group_tmdb(self, group_id: int, tmdb_id: int) -> bool:
        """Set the tmdb_id on the group row and propagate to ungrouped members."""
        try:
            await self.session.execute(
                update(TorrentGroupModel)
                .where(TorrentGroupModel.id == group_id)
                .values(tmdb_id=tmdb_id)
            )
            await self.session.flush()
            await self.propagate_tmdb_within_group(group_id)
            return True
        except Exception as e:
            logger.error(f"TorrentGroupDAO: Error updating tmdb_id for group {group_id}: {e}")
            return False

    # ------------------------------------------------------------------
    # Batch grouping (admin maintenance actions)
    # ------------------------------------------------------------------

    async def batch_group_by_info_hash(self) -> dict:
        """Create/extend groups for all torrent_items sharing the same info_hash.

        Works only on items that are not yet in a group.
        Returns {"groups_created": N, "items_grouped": N}.
        """
        groups_created = 0
        items_grouped = 0

        try:
            # Find info_hashes shared by ≥2 ungrouped items (or already grouped items
            # without a group for the same hash)
            dup_hashes_result = await self.session.execute(
                select(TorrentItemModel.info_hash)
                .where(
                    TorrentItemModel.info_hash.isnot(None),
                    TorrentItemModel.info_hash != "",
                    TorrentItemModel.group_id.is_(None),
                )
                .group_by(TorrentItemModel.info_hash)
                .having(func.count() > 1)
            )
            dup_hashes = [r[0] for r in dup_hashes_result.fetchall()]
            logger.debug(f"TorrentGroupDAO: batch_group_by_info_hash — {len(dup_hashes)} hashes to process")

            for info_hash in dup_hashes:
                # Items (ungrouped) sharing this hash
                items_result = await self.session.execute(
                    select(TorrentItemModel.id, TorrentItemModel.raw_title, TorrentItemModel.tmdb_id)
                    .where(
                        TorrentItemModel.info_hash == info_hash,
                        TorrentItemModel.group_id.is_(None),
                    )
                )
                item_rows = items_result.fetchall()
                if len(item_rows) < 2:
                    continue

                item_ids = [r[0] for r in item_rows]
                canonical_title = item_rows[0][1]
                tmdb_ids = [r[2] for r in item_rows if r[2] is not None]
                tmdb_id = Counter(tmdb_ids).most_common(1)[0][0] if tmdb_ids else None

                # Check if a group already exists for this hash
                existing_group = await self.session.execute(
                    select(TorrentGroupModel).where(
                        TorrentGroupModel.canonical_info_hash == info_hash.lower()
                    )
                )
                group = existing_group.scalar_one_or_none()

                if group is None:
                    group = TorrentGroupModel(
                        canonical_info_hash=info_hash.lower(),
                        canonical_title=canonical_title,
                        tmdb_id=tmdb_id,
                        created_at=int(datetime.now().timestamp()),
                    )
                    self.session.add(group)
                    await self.session.flush()
                    await self.session.refresh(group)
                    groups_created += 1

                await self.session.execute(
                    update(TorrentItemModel)
                    .where(TorrentItemModel.id.in_(item_ids))
                    .values(group_id=group.id)
                )
                await self.session.flush()
                items_grouped += len(item_ids)

                # Propagate TMDB within the group (may cover previously grouped siblings)
                await self.propagate_tmdb_within_group(group.id)

        except Exception as e:
            logger.error(f"TorrentGroupDAO: batch_group_by_info_hash error: {e}")

        # Refresh all item_counts in one pass
        await self._refresh_all_item_counts()
        logger.info(
            f"TorrentGroupDAO: batch_group_by_info_hash done — "
            f"{groups_created} groups created, {items_grouped} items grouped"
        )
        return {"groups_created": groups_created, "items_grouped": items_grouped}

    async def batch_group_by_title_size(self, max_size_diff: int = 524288) -> dict:
        """Create/extend groups for ungrouped items with matching normalized title and size.

        Algorithm:
          1. SQL: load ungrouped items in batches ordered by size.
          2. Python: normalize each raw_title and cluster by normalized title.
          3. Per normalized-title cluster, apply absolute size tolerance (≤512 KB by default).
          4. Create/extend a group per cluster.

        Returns {"groups_created": N, "items_grouped": N}.
        """
        from stream_fusion.utils.filter.title_matching import get_normalizer

        groups_created = 0
        items_grouped = 0

        try:
            normalizer = get_normalizer()
            if normalizer is None:
                logger.warning("TorrentGroupDAO: TitleNormalizer not initialized, skipping title+size grouping")
                return {"groups_created": 0, "items_grouped": 0}

            # Load all ungrouped items with a non-zero size
            # We work in Python-side batches of 5000 rows to avoid huge memory usage
            batch_size = 5000
            offset = 0

            while True:
                items_result = await self.session.execute(
                    select(
                        TorrentItemModel.id,
                        TorrentItemModel.raw_title,
                        TorrentItemModel.size,
                        TorrentItemModel.info_hash,
                        TorrentItemModel.tmdb_id,
                        TorrentItemModel.group_id,
                    )
                    .where(
                        TorrentItemModel.group_id.is_(None),
                        TorrentItemModel.size > 0,
                    )
                    .order_by(TorrentItemModel.size)
                    .limit(batch_size)
                    .offset(offset)
                )
                rows = items_result.fetchall()
                if not rows:
                    break
                offset += batch_size

                # Build normalized-title → list[row] within SIZE buckets
                # We first pass through all rows, then refine by exact-size threshold
                norm_map: dict[str, list] = {}
                for row in rows:
                    item_id, raw_title, size, info_hash, tmdb_id, group_id = row
                    try:
                        clean = normalizer.extract_clean_title(raw_title)
                        norm = normalizer.normalize(clean)
                    except Exception:
                        norm = raw_title.lower()
                    norm_map.setdefault(norm, []).append(
                        {"id": item_id, "raw_title": raw_title, "size": size,
                         "info_hash": info_hash, "tmdb_id": tmdb_id}
                    )

                for norm_title, candidates in norm_map.items():
                    if len(candidates) < 2:
                        continue

                    # Sub-cluster by absolute size tolerance
                    clusters = _cluster_by_size(candidates, max_size_diff)
                    for cluster in clusters:
                        if len(cluster) < 2:
                            continue

                        item_ids = [c["id"] for c in cluster]
                        tmdb_ids = [c["tmdb_id"] for c in cluster if c["tmdb_id"] is not None]
                        tmdb_id = Counter(tmdb_ids).most_common(1)[0][0] if tmdb_ids else None
                        canonical_hash = next(
                            (c["info_hash"] for c in cluster if c["info_hash"]), None
                        )

                        # Check for an existing group matching the canonical_info_hash
                        group = None
                        if canonical_hash:
                            gr_result = await self.session.execute(
                                select(TorrentGroupModel).where(
                                    TorrentGroupModel.canonical_info_hash == canonical_hash.lower()
                                )
                            )
                            group = gr_result.scalar_one_or_none()

                        if group is None:
                            group = TorrentGroupModel(
                                canonical_info_hash=canonical_hash.lower() if canonical_hash else None,
                                canonical_title=norm_title,
                                tmdb_id=tmdb_id,
                                created_at=int(datetime.now().timestamp()),
                            )
                            self.session.add(group)
                            await self.session.flush()
                            await self.session.refresh(group)
                            groups_created += 1

                        await self.session.execute(
                            update(TorrentItemModel)
                            .where(TorrentItemModel.id.in_(item_ids))
                            .values(group_id=group.id)
                        )
                        await self.session.flush()
                        items_grouped += len(item_ids)

                        await self.propagate_tmdb_within_group(group.id)

        except Exception as e:
            logger.error(f"TorrentGroupDAO: batch_group_by_title_size error: {e}")

        await self._refresh_all_item_counts()
        logger.info(
            f"TorrentGroupDAO: batch_group_by_title_size done — "
            f"{groups_created} groups created, {items_grouped} items grouped"
        )
        return {"groups_created": groups_created, "items_grouped": items_grouped}

    async def propagate_tmdb_all_groups(self) -> dict:
        """Propagate TMDB IDs across all existing groups.

        Returns {"groups_updated": N, "items_updated": N}.
        """
        groups_updated = 0
        total_items_updated = 0
        try:
            group_ids_result = await self.session.execute(select(TorrentGroupModel.id))
            group_ids = [r[0] for r in group_ids_result.fetchall()]
            for gid in group_ids:
                n = await self.propagate_tmdb_within_group(gid)
                if n:
                    groups_updated += 1
                    total_items_updated += n
        except Exception as e:
            logger.error(f"TorrentGroupDAO: propagate_tmdb_all_groups error: {e}")
        logger.info(
            f"TorrentGroupDAO: propagate_tmdb_all_groups done — "
            f"{groups_updated} groups updated, {total_items_updated} items updated"
        )
        return {"groups_updated": groups_updated, "items_updated": total_items_updated}

    # ------------------------------------------------------------------
    # Maintenance helpers
    # ------------------------------------------------------------------

    async def delete_empty_groups(self) -> int:
        """Delete groups with item_count=0 or no matching torrent_items row.

        Returns count of deleted groups.
        """
        try:
            # Groups whose item_count was already set to 0
            result = await self.session.execute(
                select(TorrentGroupModel.id).where(TorrentGroupModel.item_count == 0)
            )
            empty_ids = [r[0] for r in result.fetchall()]

            # Also verify via a real count (to catch drift in the denormalized column)
            all_groups_result = await self.session.execute(select(TorrentGroupModel.id))
            all_group_ids = [r[0] for r in all_groups_result.fetchall()]
            for gid in all_group_ids:
                cnt_result = await self.session.execute(
                    select(func.count()).where(TorrentItemModel.group_id == gid)
                )
                if cnt_result.scalar_one() == 0:
                    if gid not in empty_ids:
                        empty_ids.append(gid)

            if not empty_ids:
                return 0

            from sqlalchemy import delete as sa_delete
            from stream_fusion.services.postgresql.models.torrentgroup_model import TorrentGroupModel as TGM
            del_result = await self.session.execute(
                sa_delete(TGM).where(TGM.id.in_(empty_ids))
            )
            await self.session.flush()
            count = del_result.rowcount
            logger.info(f"TorrentGroupDAO: Deleted {count} empty groups")
            return count
        except Exception as e:
            logger.error(f"TorrentGroupDAO: delete_empty_groups error: {e}")
            return 0

    async def get_group_stats(self) -> dict:
        """Return summary statistics for admin UI."""
        try:
            total_groups = (await self.session.execute(
                select(func.count()).select_from(TorrentGroupModel)
            )).scalar_one()

            grouped_items = (await self.session.execute(
                select(func.count()).where(TorrentItemModel.group_id.isnot(None))
            )).scalar_one()

            ungrouped_items = (await self.session.execute(
                select(func.count()).where(TorrentItemModel.group_id.is_(None))
            )).scalar_one()

            groups_with_tmdb = (await self.session.execute(
                select(func.count()).where(
                    TorrentGroupModel.tmdb_id.isnot(None)
                ).select_from(TorrentGroupModel)
            )).scalar_one()

            return {
                "total_groups": total_groups,
                "grouped_items": grouped_items,
                "ungrouped_items": ungrouped_items,
                "groups_with_tmdb": groups_with_tmdb,
            }
        except Exception as e:
            logger.error(f"TorrentGroupDAO: get_group_stats error: {e}")
            return {
                "total_groups": 0,
                "grouped_items": 0,
                "ungrouped_items": 0,
                "groups_with_tmdb": 0,
            }

    # ------------------------------------------------------------------
    # Search methods (admin group browser)
    # ------------------------------------------------------------------

    async def search_groups_by_tmdb_id(self, tmdb_id: int) -> list[dict]:
        """Return groups with their items for a given TMDB ID.

        Finds groups via two paths:
        1. Groups whose tmdb_id column matches directly.
        2. Items with this tmdb_id that have a group_id → fetch those groups.

        Returns a list of dicts: {"group": TorrentGroupModel, "items": [TorrentItemModel, ...]}
        """
        try:
            async with self.session.begin():
                # Path 1 — groups with matching tmdb_id
                r1 = await self.session.execute(
                    select(TorrentGroupModel).where(TorrentGroupModel.tmdb_id == tmdb_id)
                )
                direct_groups = {g.id: g for g in r1.scalars().all()}

                # Path 2 — items with tmdb_id that belong to a group
                r2 = await self.session.execute(
                    select(TorrentItemModel.group_id)
                    .where(
                        TorrentItemModel.tmdb_id == tmdb_id,
                        TorrentItemModel.group_id.isnot(None),
                    )
                    .distinct()
                )
                extra_group_ids = [row[0] for row in r2.fetchall() if row[0] not in direct_groups]
                if extra_group_ids:
                    r3 = await self.session.execute(
                        select(TorrentGroupModel).where(TorrentGroupModel.id.in_(extra_group_ids))
                    )
                    for g in r3.scalars().all():
                        direct_groups[g.id] = g

                if not direct_groups:
                    return []

                return await self._load_groups_with_items(list(direct_groups.values()))
        except Exception as e:
            logger.error(f"TorrentGroupDAO: search_groups_by_tmdb_id({tmdb_id}) error: {e}")
            return []

    async def search_groups_by_info_hash(self, info_hash: str) -> list[dict]:
        """Return the group containing the item with this info_hash, plus all members."""
        try:
            async with self.session.begin():
                norm = info_hash.lower().strip()
                # Look for item with this hash
                r = await self.session.execute(
                    select(TorrentItemModel.group_id)
                    .where(TorrentItemModel.info_hash == norm)
                    .where(TorrentItemModel.group_id.isnot(None))
                    .limit(1)
                )
                row = r.fetchone()
                if row is None:
                    # Hash exists but no group — fall back to the item itself ungrouped
                    r2 = await self.session.execute(
                        select(TorrentItemModel).where(TorrentItemModel.info_hash == norm)
                    )
                    items = list(r2.scalars().all())
                    if not items:
                        return []
                    # Return as a virtual group (no group row)
                    return [{"group": None, "members": items}]

                group_id = row[0]
                r3 = await self.session.execute(
                    select(TorrentGroupModel).where(TorrentGroupModel.id == group_id)
                )
                group = r3.scalar_one_or_none()
                if not group:
                    return []
                return await self._load_groups_with_items([group])
        except Exception as e:
            logger.error(f"TorrentGroupDAO: search_groups_by_info_hash({info_hash}) error: {e}")
            return []

    async def search_groups_by_title(self, title: str, limit: int = 20) -> list[dict]:
        """Return groups whose canonical_title contains the search term (case-insensitive)."""
        try:
            async with self.session.begin():
                pattern = f"%{title}%"
                r = await self.session.execute(
                    select(TorrentGroupModel)
                    .where(TorrentGroupModel.canonical_title.ilike(pattern))
                    .order_by(TorrentGroupModel.item_count.desc())
                    .limit(limit)
                )
                groups = list(r.scalars().all())
                if not groups:
                    return []
                return await self._load_groups_with_items(groups)
        except Exception as e:
            logger.error(f"TorrentGroupDAO: search_groups_by_title({title!r}) error: {e}")
            return []

    async def _load_groups_with_items(self, groups: list[TorrentGroupModel]) -> list[dict]:
        """For each group, load all torrent_items members, sorted by seeders desc."""
        result = []
        group_ids = [g.id for g in groups]
        r = await self.session.execute(
            select(TorrentItemModel)
            .where(TorrentItemModel.group_id.in_(group_ids))
            .order_by(TorrentItemModel.group_id, TorrentItemModel.seeders.desc().nullslast())
        )
        all_items = r.scalars().all()

        # Index items by group_id
        items_by_group: dict[int, list] = {}
        for item in all_items:
            items_by_group.setdefault(item.group_id, []).append(item)

        for group in sorted(groups, key=lambda g: g.item_count, reverse=True):
            result.append({
                "group": group,
                "members": items_by_group.get(group.id, []),
            })
        return result

    async def _refresh_all_item_counts(self) -> None:
        """Recalculate item_count for every group in a single pass."""
        try:
            counts_result = await self.session.execute(
                select(TorrentItemModel.group_id, func.count().label("cnt"))
                .where(TorrentItemModel.group_id.isnot(None))
                .group_by(TorrentItemModel.group_id)
            )
            for group_id, cnt in counts_result.fetchall():
                await self.session.execute(
                    update(TorrentGroupModel)
                    .where(TorrentGroupModel.id == group_id)
                    .values(item_count=cnt)
                )
            await self.session.flush()
        except Exception as e:
            logger.warning(f"TorrentGroupDAO: _refresh_all_item_counts error: {e}")


# ---------------------------------------------------------------------------
# Private helper
# ---------------------------------------------------------------------------

def _cluster_by_size(items: list[dict], max_diff: int = 524288) -> list[list[dict]]:
    """Group items into clusters where all sizes are within *max_diff* bytes of each other.

    Uses a greedy single-linkage approach: items are sorted by size and collected
    into a cluster as long as the new item is within *max_diff* bytes of the
    cluster's first item (the smallest, since the list is sorted).

    Default tolerance: 512 KB — enough to cover the presence/absence of an NFO
    while being far too small to confuse different encodes.
    """
    if not items:
        return []

    sorted_items = sorted(items, key=lambda x: x["size"])
    clusters: list[list[dict]] = []
    current: list[dict] = [sorted_items[0]]

    for item in sorted_items[1:]:
        base_size = current[0]["size"]
        if abs(item["size"] - base_size) <= max_diff:
            current.append(item)
        else:
            clusters.append(current)
            current = [item]

    clusters.append(current)
    return clusters
