import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from redis import Redis, ConnectionPool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from stream_fusion.logging_config import logger
from stream_fusion.settings import settings

LEADER_KEY  = "scheduler:leader"
LEADER_TTL  = 90    # secondes — TTL du lock Redis
RENEW_EVERY = 60    # secondes — intervalle de renouvellement du lock

# Métadonnées des jobs pour l'affichage dans l'interface admin
JOB_META: dict[str, dict] = {
    "debrid_cache_cleanup":    {"name": "Purge cache debrid expiré",          "icon": "bi-database-x"},
    "torrent_old_cleanup":     {"name": "Suppression torrents anciens",        "icon": "bi-trash3"},
    "torrent_orphan_cleanup":  {"name": "Suppression torrents orphelins",      "icon": "bi-file-earmark-x"},
    "torrent_dedup":           {"name": "Déduplication torrents",              "icon": "bi-copy"},
    "torrent_group_hash":      {"name": "Groupement par info_hash",            "icon": "bi-diagram-3"},
    "torrent_group_title_size":{"name": "Groupement par titre + taille",       "icon": "bi-diagram-2"},
    "api_keys_cleanup":        {"name": "Désactivation clés API expirées",     "icon": "bi-key"},
    "peer_keys_cleanup":       {"name": "Désactivation peer keys expirées",    "icon": "bi-diagram-3"},
}


def _job_redis_key(job_id: str) -> str:
    return f"scheduler:job:{job_id}"


class StreamFusionScheduler:
    """
    Scheduler APScheduler avec leader election Redis.

    Un seul worker Gunicorn devient "leader" via SET NX sur Redis et
    exécute les jobs de nettoyage. Les autres workers skippent le démarrage.
    Si le worker leader meurt, le lock expire (LEADER_TTL secondes) et un
    autre worker peut acquérir le leadership au prochain redémarrage.
    """

    def __init__(self, session_factory: async_sessionmaker, redis_pool: ConnectionPool) -> None:
        self._session_factory = session_factory
        self._redis_pool = redis_pool
        self._scheduler = AsyncIOScheduler()
        self._is_leader = False
        self._renew_task: Optional[asyncio.Task] = None
        self._job_funcs = {
            "debrid_cache_cleanup":     self._cleanup_debrid_cache,
            "torrent_old_cleanup":      self._cleanup_old_torrents,
            "torrent_orphan_cleanup":   self._cleanup_orphan_torrents,
            "torrent_dedup":            self._dedup_torrents,
            "torrent_group_hash":       self._group_by_info_hash,
            "torrent_group_title_size": self._group_by_title_size,
            "api_keys_cleanup":         self._cleanup_api_keys,
            "peer_keys_cleanup":        self._cleanup_peer_keys,
        }

    def _redis(self) -> Redis:
        return Redis(connection_pool=self._redis_pool)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if not settings.scheduler_enabled:
            logger.info("Scheduler: disabled via SCHEDULER_ENABLED, skipping")
            return

        r = self._redis()
        acquired = r.set(LEADER_KEY, str(os.getpid()), nx=True, ex=LEADER_TTL)
        if not acquired:
            stored = r.get(LEADER_KEY)
            leader_pid = stored.decode() if stored else "?"
            logger.info(f"Scheduler: worker {os.getpid()} is not leader (leader={leader_pid}), skipping")
            return

        self._is_leader = True
        logger.info(f"Scheduler: worker {os.getpid()} acquired leadership, registering jobs")
        self._register_jobs()
        self._scheduler.start()
        self._renew_task = asyncio.create_task(self._renew_loop())
        logger.info("Scheduler: started successfully")

    async def stop(self) -> None:
        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        if self._is_leader:
            self._redis().delete(LEADER_KEY)
            logger.info("Scheduler: stopped and leader lock released")

    # ── Job registration ──────────────────────────────────────────────────────

    def _register_jobs(self) -> None:
        # Premier run échelonné : +5min, +25min, +45min, +65min, +85min, +105min
        # pour éviter de surcharger la DB au démarrage.
        now = datetime.now(timezone.utc)
        stagger = timedelta(minutes=20)

        dh = settings.scheduler_debrid_cleanup_interval_hours
        th = settings.scheduler_torrent_cleanup_interval_hours
        kh = settings.scheduler_keys_cleanup_interval_hours

        jobs = [
            (self._cleanup_debrid_cache,    "debrid_cache_cleanup",     dh),
            (self._cleanup_old_torrents,    "torrent_old_cleanup",      th),
            (self._cleanup_orphan_torrents, "torrent_orphan_cleanup",   th),
            (self._dedup_torrents,          "torrent_dedup",            th),
            (self._group_by_info_hash,      "torrent_group_hash",       th),
            (self._group_by_title_size,     "torrent_group_title_size", th),
            (self._cleanup_api_keys,        "api_keys_cleanup",         kh),
            (self._cleanup_peer_keys,       "peer_keys_cleanup",        kh),
        ]
        for i, (func, job_id, interval_hours) in enumerate(jobs):
            start_date = now + timedelta(minutes=5) + stagger * i
            self._scheduler.add_job(
                func, "interval",
                hours=interval_hours, id=job_id,
                start_date=start_date, replace_existing=True,
            )

    # ── Leader lock renewal ───────────────────────────────────────────────────

    async def _renew_loop(self) -> None:
        while True:
            await asyncio.sleep(RENEW_EVERY)
            self._redis().expire(LEADER_KEY, LEADER_TTL)

    # ── Core job runner ───────────────────────────────────────────────────────

    async def _run_job(self, job_id: str, sql: str, params: dict | None = None) -> int:
        """Exécute une requête SQL, commit, et enregistre les métriques dans Redis."""
        r = self._redis()
        t0 = time.monotonic()
        try:
            async with self._session_factory() as session:
                result = await session.execute(text(sql), params or {})
                await session.commit()
                count = result.rowcount
            duration_ms = int((time.monotonic() - t0) * 1000)
            logger.warning(f"[scheduler] {job_id}: {count} rows affected in {duration_ms}ms")
            r.hset(_job_redis_key(job_id), mapping={
                "last_run_at":        int(time.time()),
                "last_duration_ms":   duration_ms,
                "last_rows_affected": count,
                "last_status":        "ok",
                "last_error":         "",
            })
            return count
        except Exception as exc:
            duration_ms = int((time.monotonic() - t0) * 1000)
            logger.error(f"[scheduler] {job_id} failed after {duration_ms}ms: {exc}")
            r.hset(_job_redis_key(job_id), mapping={
                "last_run_at":        int(time.time()),
                "last_duration_ms":   duration_ms,
                "last_rows_affected": 0,
                "last_status":        "error",
                "last_error":         str(exc)[:500],
            })
            return 0

    async def _run_dao_job(self, job_id: str, dao_coro) -> int:
        """Execute a DAO-based coroutine, commit, and record metrics in Redis.

        `dao_coro` is a coroutine that already has the session bound — it is
        created inside this method via a factory callable that receives the DAO.
        """
        from stream_fusion.services.postgresql.dao.torrentgroup_dao import TorrentGroupDAO

        r = self._redis()
        t0 = time.monotonic()
        try:
            async with self._session_factory() as session:
                dao = TorrentGroupDAO(session)
                result = await dao_coro(dao)
                await session.commit()
            # result is expected to be a dict with "groups_created" and "items_grouped"
            count = result.get("groups_created", 0) + result.get("items_grouped", 0)
            duration_ms = int((time.monotonic() - t0) * 1000)
            logger.warning(
                f"[scheduler] {job_id}: groups_created={result.get('groups_created', 0)}, "
                f"items_grouped={result.get('items_grouped', 0)} in {duration_ms}ms"
            )
            r.hset(_job_redis_key(job_id), mapping={
                "last_run_at":        int(time.time()),
                "last_duration_ms":   duration_ms,
                "last_rows_affected": count,
                "last_status":        "ok",
                "last_error":         "",
            })
            return count
        except Exception as exc:
            duration_ms = int((time.monotonic() - t0) * 1000)
            logger.error(f"[scheduler] {job_id} failed after {duration_ms}ms: {exc}")
            r.hset(_job_redis_key(job_id), mapping={
                "last_run_at":        int(time.time()),
                "last_duration_ms":   duration_ms,
                "last_rows_affected": 0,
                "last_status":        "error",
                "last_error":         str(exc)[:500],
            })
            return 0

    # ── Jobs ──────────────────────────────────────────────────────────────────

    async def _cleanup_debrid_cache(self) -> None:
        await self._run_job(
            "debrid_cache_cleanup",
            "DELETE FROM debrid_cache WHERE expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT",
        )

    async def _cleanup_old_torrents(self) -> None:
        cutoff = int(time.time()) - settings.scheduler_torrent_max_age_days * 86400
        await self._run_job(
            "torrent_old_cleanup",
            "DELETE FROM torrent_items WHERE updated_at < :cutoff",
            {"cutoff": cutoff},
        )

    async def _cleanup_orphan_torrents(self) -> None:
        cutoff = int(time.time()) - settings.scheduler_torrent_orphan_max_age_days * 86400
        await self._run_job(
            "torrent_orphan_cleanup",
            "DELETE FROM torrent_items WHERE tmdb_id IS NULL AND created_at < :cutoff",
            {"cutoff": cutoff},
        )

    async def _dedup_torrents(self) -> None:
        await self._run_job(
            "torrent_dedup",
            """
            DELETE FROM torrent_items
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY info_hash, indexer
                               ORDER BY seeders DESC NULLS LAST, updated_at DESC
                           ) AS rn
                    FROM torrent_items
                ) ranked
                WHERE rn > 1
            )
            """,
        )

    async def _group_by_info_hash(self) -> None:
        """Create/extend torrent groups for all items sharing the same info_hash."""
        await self._run_dao_job(
            "torrent_group_hash",
            lambda dao: dao.batch_group_by_info_hash(),
        )

    async def _group_by_title_size(self) -> None:
        """Create/extend torrent groups for items with the same normalized title and similar size."""
        await self._run_dao_job(
            "torrent_group_title_size",
            lambda dao: dao.batch_group_by_title_size(),
        )

    async def _cleanup_api_keys(self) -> None:
        await self._run_job(
            "api_keys_cleanup",
            """
            UPDATE api_keys SET is_active = false
            WHERE never_expire = false
              AND expiration_date < EXTRACT(EPOCH FROM NOW())::BIGINT
              AND is_active = true
            """,
        )

    async def _cleanup_peer_keys(self) -> None:
        await self._run_job(
            "peer_keys_cleanup",
            """
            UPDATE peer_keys SET is_active = false
            WHERE expires_at IS NOT NULL
              AND expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT
              AND is_active = true
            """,
        )

    # ── Public API for admin views ─────────────────────────────────────────────

    async def trigger_job(self, job_id: str) -> bool:
        """Déclenche un job immédiatement. Utilisable depuis n'importe quel worker."""
        func = self._job_funcs.get(job_id)
        if not func:
            return False
        asyncio.create_task(func())
        logger.info(f"[scheduler] job '{job_id}' manually triggered")
        return True

    def get_status(self) -> dict:
        """Retourne l'état du scheduler (depuis Redis + APScheduler)."""
        r = self._redis()
        leader_raw = r.get(LEADER_KEY)
        leader_pid = int(leader_raw) if leader_raw else None

        job_intervals = {
            "debrid_cache_cleanup":     settings.scheduler_debrid_cleanup_interval_hours,
            "torrent_old_cleanup":      settings.scheduler_torrent_cleanup_interval_hours,
            "torrent_orphan_cleanup":   settings.scheduler_torrent_cleanup_interval_hours,
            "torrent_dedup":            settings.scheduler_torrent_cleanup_interval_hours,
            "torrent_group_hash":       settings.scheduler_torrent_cleanup_interval_hours,
            "torrent_group_title_size": settings.scheduler_torrent_cleanup_interval_hours,
            "api_keys_cleanup":         settings.scheduler_keys_cleanup_interval_hours,
            "peer_keys_cleanup":        settings.scheduler_keys_cleanup_interval_hours,
        }

        jobs = []
        for job_id, meta in JOB_META.items():
            raw: dict = r.hgetall(_job_redis_key(job_id))

            def _int(key: bytes) -> int:
                return int(raw.get(key, 0) or 0)

            def _str(key: bytes) -> str:
                val = raw.get(key, b"")
                return val.decode() if isinstance(val, bytes) else (val or "")

            # next_run_time uniquement disponible sur le worker leader
            next_run_at = None
            if self._is_leader and self._scheduler.running:
                job = self._scheduler.get_job(job_id)
                if job and job.next_run_time:
                    next_run_at = int(job.next_run_time.timestamp())

            jobs.append({
                "id":                job_id,
                "name":              meta["name"],
                "icon":              meta["icon"],
                "interval_hours":    job_intervals.get(job_id, 0),
                "next_run_at":       next_run_at,
                "last_run_at":       _int(b"last_run_at"),
                "last_duration_ms":  _int(b"last_duration_ms"),
                "last_rows_affected":_int(b"last_rows_affected"),
                "last_status":       _str(b"last_status"),
                "last_error":        _str(b"last_error"),
            })

        return {
            "scheduler_enabled":  settings.scheduler_enabled,
            "is_leader":          self._is_leader,
            "leader_pid":         leader_pid,
            "scheduler_running":  self._scheduler.running if self._is_leader else False,
            "jobs":               jobs,
        }
