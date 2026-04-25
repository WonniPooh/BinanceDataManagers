"""
Integration test — download 60 days of ADAUSDT klines from S3 archive.

Calls the archive downloader (``process_symbol``) directly to verify S3
monthly/daily ZIP download + CSV parsing + DB insertion works end-to-end.
Then runs the full orchestrator (``load_symbol``) on the same DB to verify
REST API fills the remaining gap (most recent 1–2 days the archive lags).
Finally checks the combined DB for zero gaps.

Hits mainnet Binance S3 + REST API — NOT for CI.

Usage:
    cd <project_root>
    python -m data_manager.klines_manager.tests.test_klines_archive_integration
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

_project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from data_manager.klines_manager.klines_archive_downloader import (
    process_symbol as archive_process_symbol,
    db_path_for_symbol,
)
from data_manager.klines_manager.klines_loader import load_symbol, find_gaps
from data_manager.klines_manager.klines_db_manager import CandleDB
from data_manager.klines_manager.klines_exchange_downloader import CANDLE_MS
from data_manager.binance_rate_limiter import bnx_limiter

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL = "ADAUSDT"
LOOKBACK_DAYS = 60
DB_ROOT = str(_project_root / "logs" / "test_klines_archive_integration")
UTC = timezone.utc


def _cleanup():
    p = Path(DB_ROOT)
    if p.exists():
        shutil.rmtree(p)


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def test_archive_then_loader():
    """Test archive download directly, then run full orchestrator for gaps."""
    _cleanup()

    status_queue: asyncio.Queue = asyncio.Queue()
    archive_events: list[dict] = []

    async def _drain():
        while True:
            try:
                msg = await asyncio.wait_for(status_queue.get(), timeout=1.0)
                logging.info("PROGRESS: %s", msg)
                phase = msg.get("phase", "")
                if phase in ("download", "insert"):
                    archive_events.append(msg)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

    drain_task = asyncio.create_task(_drain())

    t0 = time.monotonic()
    now = datetime.now(tz=UTC)
    start_dt = now - __import__("datetime").timedelta(days=LOOKBACK_DAYS)

    # ── Step 1: Archive download directly ─────────────────────────────
    logging.info("Step 1: Archive download for %s, %d days back", SYMBOL, LOOKBACK_DAYS)

    io_sem = asyncio.Semaphore(16)
    async with aiohttp.ClientSession() as session:
        await archive_process_symbol(
            session, SYMBOL, start_dt, DB_ROOT, io_sem,
            per_asset_conc=10,
            status_queue=status_queue,
        )

    archive_elapsed = time.monotonic() - t0
    logging.info("Archive download completed in %.1fs", archive_elapsed)

    # Verify archive actually downloaded files
    assert len(archive_events) > 0, (
        "No archive download/insert events received! "
        "S3 archive path was not exercised.")
    logging.info("Archive events: %d (download+insert)", len(archive_events))

    # Verify DB was created with data
    db_path = db_path_for_symbol(DB_ROOT, SYMBOL)
    db = CandleDB(str(db_path))
    try:
        archive_count = db.conn.execute("SELECT COUNT(*) FROM candle").fetchone()[0]
        assert archive_count > 0, "Archive produced 0 candles"
        logging.info("Archive inserted %d candles into DB", archive_count)

        bounds = db.conn.execute(
            "SELECT MIN(open_time_ms), MAX(open_time_ms) FROM candle"
        ).fetchone()
        logging.info("Archive data range: %d -> %d", bounds[0], bounds[1])
    finally:
        db.close()

    # ── Step 2: Run full orchestrator to fill remaining gap ───────────
    logging.info("Step 2: Running full orchestrator to fill remaining gap...")

    t1 = time.monotonic()
    result = await load_symbol(
        SYMBOL,
        lookback_days=LOOKBACK_DAYS,
        db_root=DB_ROOT,
        rate_limiter=bnx_limiter,
        status_queue=status_queue,
    )

    drain_task.cancel()
    try:
        await drain_task
    except asyncio.CancelledError:
        pass

    loader_elapsed = time.monotonic() - t1
    logging.info("Orchestrator completed in %.1fs: %s", loader_elapsed, result)

    # ── Assertions ────────────────────────────────────────────────────

    # 1. Total candle count
    expected_min = LOOKBACK_DAYS * 24 * 60 - 10
    assert result["candles"] >= expected_min, (
        f"Too few candles: {result['candles']} < {expected_min}")

    # 2. REST API added candles on top of archive
    assert result["api_rows"] > 0, (
        "REST API inserted 0 rows — should have filled the archive gap")
    logging.info("REST API filled %d candles on top of archive's %d",
                  result["api_rows"], archive_count)

    # 3. No gaps remain
    assert result["gaps_remaining"] == 0, (
        f"Expected 0 remaining gaps, got {result['gaps_remaining']}")

    # 4. Direct DB gap check
    start_ms = result["start_ms"]
    end_ms = result["end_ms"]
    db = CandleDB(str(db_path))
    try:
        gaps = find_gaps(db, start_ms, end_ms)
        logging.info("Direct gap check: %d gaps", len(gaps))
        for g_start, g_end in gaps:
            gap_minutes = (g_end - g_start) / CANDLE_MS
            logging.warning("  Gap: %d -> %d (%.1f minutes)", g_start, g_end, gap_minutes)
        assert len(gaps) == 0, f"Direct gap check found {len(gaps)} gaps"

        total = db.conn.execute(
            "SELECT COUNT(*) FROM candle WHERE open_time_ms BETWEEN ? AND ?",
            (start_ms, end_ms),
        ).fetchone()[0]
        assert total == result["candles"]
    finally:
        db.close()

    total_elapsed = time.monotonic() - t0
    logging.info("ALL CHECKS PASSED — %d candles (archive=%d, api=%d), "
                 "0 gaps, %.1fs total",
                 result["candles"], archive_count, result["api_rows"],
                 total_elapsed)


def main():
    _setup_logging()
    try:
        asyncio.run(test_archive_then_loader())
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_klines_archive_integration: PASSED")


if __name__ == "__main__":
    main()
