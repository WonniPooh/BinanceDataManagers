"""
Klines loader — orchestrates archive + REST API to fully load klines for a
symbol with no gaps.

Usage::

    from data_manager.klines_manager.klines_loader import load_symbol

    queue = asyncio.Queue()
    await load_symbol("ADAUSDT", lookback_days=7, db_root="db_files",
                      status_queue=queue)

Flow:
    1.  REST API: fetch from NOW backwards (newest data available first)
    2.  Archive: bulk-fill history from S3 (no rate limit)
    3.  Gap scan: verify continuity, REST-fill any remaining holes
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp

from .klines_db_manager import CandleDB
from .klines_archive_downloader import (
    INTERVAL,
    db_path_for_symbol,
    process_symbol as archive_process_symbol,
)
from .klines_exchange_downloader import (
    CANDLE_MS,
    fill_gap,
)

logger = logging.getLogger("klines_loader")
UTC = timezone.utc


async def load_symbol(
    symbol: str,
    lookback_days: int = 7,
    db_root: str | Path = "db_files",
    rate_limiter: Any | None = None,
    status_queue: asyncio.Queue | None = None,
    testnet: bool = False,
    archive_concurrency: int = 10,
    io_semaphore_limit: int = 16,
) -> Dict[str, Any]:
    """Fully load klines for *symbol* from ``now - lookback_days`` to now.

    Steps executed sequentially:
      1. REST API newest-first (fast, gets recent data)
      2. Archive bulk download
      3. Gap verification + fill

    Returns summary dict with row counts and gap info.
    """
    symbol = symbol.upper()
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - lookback_days * 24 * 60 * 60 * 1000
    db_path = db_path_for_symbol(db_root, symbol)

    def _emit(phase: str, pct: float, detail: str = "") -> None:
        if status_queue is None:
            return
        msg: Dict[str, Any] = {
            "symbol": symbol, "source": "loader", "phase": phase,
            "pct": round(pct, 1),
        }
        if detail:
            msg["detail"] = detail
        status_queue.put_nowait(msg)

    _emit("start", 0.0, f"lookback={lookback_days}d")
    logger.info("[%s] Loading %d days of klines", symbol, lookback_days)

    # ── Step 1: REST API (newest-first) ─────────────────────────────
    _emit("api", 0.0, "starting REST gap-fill")

    api_queue: asyncio.Queue | None = None
    if status_queue is not None:
        api_queue = asyncio.Queue()

    async with aiohttp.ClientSession() as session:
        db = CandleDB(str(db_path))
        try:
            # Check if DB already has some recent data
            existing_max = _get_max_time(db)
            api_start = existing_max + CANDLE_MS if existing_max and existing_max >= start_ms else start_ms

            api_rows = 0
            if api_start < now_ms:
                api_rows = await fill_gap(
                    session, symbol, api_start, now_ms, db,
                    rate_limiter=rate_limiter,
                    status_queue=status_queue,
                    testnet=testnet,
                )
            logger.info("[%s] REST API: %d candles", symbol, api_rows)
        finally:
            db.close()

        # ── Step 2: Archive download ────────────────────────────────
        _emit("archive", 0.0, "starting archive download")

        start_dt = datetime.fromtimestamp(start_ms / 1000.0, tz=UTC)
        io_sem = asyncio.Semaphore(io_semaphore_limit)

        await archive_process_symbol(
            session, symbol, start_dt, str(db_root), io_sem,
            per_asset_conc=archive_concurrency,
            status_queue=status_queue,
        )
        logger.info("[%s] Archive download complete", symbol)

    # ── Step 3: Gap scan + fill ─────────────────────────────────────
    _emit("verify", 80.0, "scanning for gaps")

    db = CandleDB(str(db_path))
    try:
        gaps = find_gaps(db, start_ms, now_ms)

        if gaps:
            logger.warning("[%s] Found %d gaps, filling via REST", symbol, len(gaps))
            async with aiohttp.ClientSession() as session:
                fill_count = 0
                for gap_start, gap_end in gaps:
                    filled = await fill_gap(
                        session, symbol, gap_start, gap_end, db,
                        rate_limiter=rate_limiter, testnet=testnet,
                    )
                    fill_count += filled
                logger.info("[%s] Gap-filled %d candles across %d gaps",
                            symbol, fill_count, len(gaps))

        # Final stats
        total_candles = _count_candles(db, start_ms, now_ms)
        remaining_gaps = find_gaps(db, start_ms, now_ms)
    finally:
        db.close()

    _emit("done", 100.0, f"{total_candles} candles")
    logger.info("[%s] Load complete — %d candles, %d remaining gaps",
                symbol, total_candles, len(remaining_gaps))

    return {
        "symbol": symbol,
        "candles": total_candles,
        "api_rows": api_rows,
        "gaps_found": len(gaps),
        "gaps_remaining": len(remaining_gaps),
        "start_ms": start_ms,
        "end_ms": now_ms,
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_max_time(db: CandleDB) -> Optional[int]:
    row = db.conn.execute("SELECT MAX(open_time_ms) FROM candle").fetchone()
    return row[0] if row and row[0] is not None else None


def _count_candles(db: CandleDB, start_ms: int, end_ms: int) -> int:
    row = db.conn.execute(
        "SELECT COUNT(*) FROM candle WHERE open_time_ms BETWEEN ? AND ?",
        (start_ms, end_ms),
    ).fetchone()
    return row[0] if row else 0


def find_gaps(
    db: CandleDB, start_ms: int, end_ms: int,
    max_gap_ms: int = CANDLE_MS,
) -> list[tuple[int, int]]:
    """Find gaps in the candle sequence.

    Returns list of ``(gap_start_ms, gap_end_ms)`` where ``gap_start_ms`` is
    the last known candle + CANDLE_MS and ``gap_end_ms`` is the next known
    candle.  Only gaps > *max_gap_ms* are reported.
    """
    rows = db.conn.execute(
        "SELECT open_time_ms FROM candle "
        "WHERE open_time_ms BETWEEN ? AND ? "
        "ORDER BY open_time_ms",
        (start_ms, end_ms),
    ).fetchall()

    if not rows:
        return [(start_ms, end_ms)]

    gaps: list[tuple[int, int]] = []

    # Gap at the beginning?
    if rows[0][0] - start_ms > max_gap_ms:
        gaps.append((start_ms, rows[0][0]))

    # Gaps between consecutive candles
    prev = rows[0][0]
    for (ts,) in rows[1:]:
        if ts - prev > max_gap_ms:
            gaps.append((prev + CANDLE_MS, ts))
        prev = ts

    # Gap at the end?  Allow some slack (current candle may not be closed yet)
    slack = 2 * CANDLE_MS
    if end_ms - prev > max_gap_ms + slack:
        gaps.append((prev + CANDLE_MS, end_ms))

    return gaps
