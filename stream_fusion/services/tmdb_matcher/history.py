"""Redis-based history tracker for the TMDB orphan-matching background job.

Each run is stored as a JSON string under  scheduler:tmdb_match:run:{run_id}
with a 7-day TTL.  An index sorted-set  scheduler:tmdb_match:runs  keeps
(score=timestamp, member=run_id) for fast listing newest-first.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Optional

from redis import ConnectionPool, Redis

from stream_fusion.logging_config import logger

_RUNS_ZSET = "scheduler:tmdb_match:runs"
_RUN_KEY_PREFIX = "scheduler:tmdb_match:run:"
_TTL_SECONDS = 7 * 24 * 3600  # 7 days


@dataclass
class MatchEntry:
    info_hash: str
    raw_title: str
    tmdb_id: int
    tmdb_title: str
    tmdb_year: Optional[int]
    item_type: str
    match_level: str  # "exact_normalized" | "ordered_subset"


@dataclass
class MatchResult:
    processed: int = 0
    matched: int = 0
    unmatched: int = 0
    matches: list[MatchEntry] = field(default_factory=list)


@dataclass
class RunSummary:
    run_id: str
    run_at: int
    processed: int
    matched: int
    unmatched: int


@dataclass
class RunDetail(RunSummary):
    matches: list[MatchEntry] = field(default_factory=list)


class MatchHistoryTracker:
    """Saves and retrieves TMDB match run history from Redis."""

    def __init__(self, redis_pool: ConnectionPool) -> None:
        self._pool = redis_pool

    def _redis(self) -> Redis:
        return Redis(connection_pool=self._pool)

    def save_run(self, result: MatchResult) -> str:
        """Persist a MatchResult and return the generated run_id."""
        run_id = str(uuid.uuid4())
        run_at = int(time.time())
        payload = {
            "run_id": run_id,
            "run_at": run_at,
            "processed": result.processed,
            "matched": result.matched,
            "unmatched": result.unmatched,
            "matches": [asdict(m) for m in result.matches],
        }
        r = self._redis()
        try:
            r.set(f"{_RUN_KEY_PREFIX}{run_id}", json.dumps(payload), ex=_TTL_SECONDS)
            r.zadd(_RUNS_ZSET, {run_id: run_at})
            # Clean up entries older than 7 days from the index
            cutoff = run_at - _TTL_SECONDS
            r.zremrangebyscore(_RUNS_ZSET, "-inf", cutoff)
        except Exception as exc:
            logger.error(f"MatchHistoryTracker.save_run failed: {exc}")
        return run_id

    def list_runs(self) -> list[RunSummary]:
        """Return all runs from newest to oldest (summaries only)."""
        r = self._redis()
        try:
            run_ids = r.zrevrange(_RUNS_ZSET, 0, -1)
        except Exception as exc:
            logger.error(f"MatchHistoryTracker.list_runs failed: {exc}")
            return []

        summaries: list[RunSummary] = []
        for raw_id in run_ids:
            rid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            raw = r.get(f"{_RUN_KEY_PREFIX}{rid}")
            if not raw:
                continue
            try:
                d = json.loads(raw)
                summaries.append(RunSummary(
                    run_id=d["run_id"],
                    run_at=d["run_at"],
                    processed=d["processed"],
                    matched=d["matched"],
                    unmatched=d["unmatched"],
                ))
            except Exception:
                pass
        return summaries

    def get_run(self, run_id: str) -> Optional[RunDetail]:
        """Return full details for a specific run (including match entries)."""
        r = self._redis()
        try:
            raw = r.get(f"{_RUN_KEY_PREFIX}{run_id}")
        except Exception as exc:
            logger.error(f"MatchHistoryTracker.get_run failed: {exc}")
            return None
        if not raw:
            return None
        try:
            d = json.loads(raw)
            return RunDetail(
                run_id=d["run_id"],
                run_at=d["run_at"],
                processed=d["processed"],
                matched=d["matched"],
                unmatched=d["unmatched"],
                matches=[MatchEntry(**m) for m in d.get("matches", [])],
            )
        except Exception as exc:
            logger.warning(f"MatchHistoryTracker.get_run parse error for {run_id}: {exc}")
            return None
