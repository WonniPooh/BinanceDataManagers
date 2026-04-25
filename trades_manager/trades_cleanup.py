"""
Trades DB cleanup — common DB row pruning + historical DB removal.

Two independent cleanup functions that can be run on a timer or on demand:

- ``cleanup_common_db`` — delete rows older than *max_age_days* from a
  symbol's ``trades.db``.
- ``cleanup_stale_historical_dbs`` — remove ``trades_hist_*.db`` files
  whose **mtime** is older than *unused_days*.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from .trades_db_manager import AggTradeDB

logger = logging.getLogger("trades_cleanup")

_MS_PER_DAY = 86_400_000


def cleanup_common_db(
    db_root: str | Path,
    symbol: str,
    max_age_days: int = 7,
) -> int:
    """Delete rows older than *max_age_days* from the symbol's common
    ``trades.db``.

    Returns the number of rows deleted.
    """
    sym = symbol.upper()
    db_path = Path(db_root) / sym / "trades.db"
    if not db_path.exists():
        return 0

    cutoff_ms = int(time.time() * 1000) - max_age_days * _MS_PER_DAY
    db = AggTradeDB(str(db_path))
    try:
        cur = db.conn.execute(
            "DELETE FROM agg_trade WHERE trade_ts_ms < ?",
            (cutoff_ms,),
        )
        deleted = cur.rowcount
        db.conn.commit()
        if deleted > 0:
            logger.info("[%s] Pruned %d rows older than %d days", sym, deleted, max_age_days)
        return deleted
    finally:
        db.close()


def cleanup_stale_historical_dbs(
    db_root: str | Path,
    symbol: str,
    unused_days: int = 3,
) -> list[str]:
    """Remove ``trades_hist_*.db`` files whose last-modified time
    is older than *unused_days*.

    Returns list of removed file names.
    """
    sym = symbol.upper()
    sym_dir = Path(db_root) / sym
    if not sym_dir.is_dir():
        return []

    cutoff_s = time.time() - unused_days * 86_400
    removed: list[str] = []

    for p in sym_dir.glob("trades_hist_*.db"):
        if p.stat().st_mtime < cutoff_s:
            p.unlink()
            removed.append(p.name)
            logger.info("[%s] Removed stale historical DB: %s", sym, p.name)

    return removed


def cleanup_all_symbols(
    db_root: str | Path,
    max_age_days: int = 7,
    unused_days: int = 3,
) -> dict[str, dict]:
    """Run both cleanup operations for every symbol directory under *db_root*.

    Returns ``{symbol: {"common_pruned": int, "hist_removed": [str, ...]}}``
    """
    root = Path(db_root)
    if not root.is_dir():
        return {}

    results: dict[str, dict] = {}
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        sym = entry.name
        pruned = cleanup_common_db(root, sym, max_age_days)
        removed = cleanup_stale_historical_dbs(root, sym, unused_days)
        if pruned or removed:
            results[sym] = {"common_pruned": pruned, "hist_removed": removed}

    return results
