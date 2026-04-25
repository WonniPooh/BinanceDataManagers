"""
Integration test — download 60 days of ADAUSDT trades from S3 archive.

Calls the archive downloader (``process_symbol``) directly to verify S3
monthly/daily ZIP download + aggTrade CSV parsing + DB insertion works
end-to-end.  Then runs REST gap-fill on the same DB to verify the most
recent days are covered.  Finally checks the combined DB for data continuity.

Hits mainnet Binance S3 + REST API — NOT for CI.

Usage:
    cd <project_root>
    python -m data_manager.trades_manager.tests.test_trades_archive_integration
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp

_project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from data_manager.trades_manager.trades_archive_downloader import (
    process_symbol as archive_process_symbol,
    db_path_for_symbol,
)
from data_manager.trades_manager.trades_rest_downloader import fill_gap
from data_manager.trades_manager.trades_db_manager import AggTradeDB
from data_manager.binance_rate_limiter import bnx_limiter

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL = "ADAUSDT"
LOOKBACK_DAYS = 60
DB_ROOT = str(_project_root / "logs" / "test_trades_archive_integration")
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


async def test_archive_then_rest():
    """Test archive download directly, then REST gap-fill for recent data."""
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
    start_dt = now - timedelta(days=LOOKBACK_DAYS)

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
    db = AggTradeDB(str(db_path))
    try:
        archive_count = db.conn.execute("SELECT COUNT(*) FROM agg_trade").fetchone()[0]
        assert archive_count > 0, "Archive produced 0 trades"
        logging.info("Archive inserted %d trades into DB", archive_count)

        bounds = db.conn.execute(
            "SELECT MIN(trade_ts_ms), MAX(trade_ts_ms) FROM agg_trade"
        ).fetchone()
        logging.info("Archive data range: %d -> %d (%.1f days)",
                      bounds[0], bounds[1],
                      (bounds[1] - bounds[0]) / 86_400_000)
        archive_max_ts = bounds[1]
    finally:
        db.close()

    # ── Step 2: REST gap-fill for recent data ─────────────────────────
    logging.info("Step 2: REST gap-fill from archive end to now...")

    now_ms = int(time.time() * 1000)
    rest_start_ms = archive_max_ts + 1

    t1 = time.monotonic()
    db = AggTradeDB(str(db_path))
    try:
        async with aiohttp.ClientSession() as session:
            rest_inserted = await fill_gap(
                session, SYMBOL,
                rest_start_ms, now_ms,
                db,
                rate_limiter=bnx_limiter,
                status_queue=status_queue,
            )
    finally:
        db.close()

    drain_task.cancel()
    try:
        await drain_task
    except asyncio.CancelledError:
        pass

    rest_elapsed = time.monotonic() - t1
    logging.info("REST gap-fill completed in %.1fs: %d trades", rest_elapsed, rest_inserted)

    # ── Assertions ────────────────────────────────────────────────────

    # 1. REST added on top of archive
    assert rest_inserted > 0, (
        "REST API inserted 0 rows — should have filled the archive gap")
    logging.info("REST filled %d trades on top of archive's %d",
                  rest_inserted, archive_count)

    # 2. Total count
    db = AggTradeDB(str(db_path))
    try:
        total = db.conn.execute("SELECT COUNT(*) FROM agg_trade").fetchone()[0]
        logging.info("Total trades in DB: %d (archive=%d + rest=%d)",
                      total, archive_count, rest_inserted)
        assert total >= archive_count + rest_inserted, (
            f"Total {total} < archive {archive_count} + rest {rest_inserted}")

        # 3. Verify time range covers the full lookback
        bounds = db.conn.execute(
            "SELECT MIN(trade_ts_ms), MAX(trade_ts_ms) FROM agg_trade"
        ).fetchone()
        start_ms = int(start_dt.timestamp() * 1000)
        # Archive may not have the very first day, but should be within 2 days
        assert bounds[0] <= start_ms + 2 * 86_400_000, (
            f"Oldest trade {bounds[0]} too far from requested start {start_ms}")
        # Max should be very recent
        assert bounds[1] >= now_ms - 60_000, (
            f"Newest trade {bounds[1]} too far from now {now_ms}")

        logging.info("Combined data range: %d -> %d (%.1f days)",
                      bounds[0], bounds[1],
                      (bounds[1] - bounds[0]) / 86_400_000)

        # 4. Sanity check on sample rows
        sample = db.conn.execute(
            "SELECT agg_trade_id, price, qty FROM agg_trade LIMIT 5"
        ).fetchall()
        for trade_id, price, qty in sample:
            assert trade_id > 0
            assert price > 0
            assert qty > 0
    finally:
        db.close()

    total_elapsed = time.monotonic() - t0
    logging.info("ALL CHECKS PASSED — %d trades total in %.1fs "
                  "(archive=%.1fs, rest=%.1fs)",
                  total, total_elapsed, archive_elapsed, rest_elapsed)


def main():
    _setup_logging()
    try:
        asyncio.run(test_archive_then_rest())
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_trades_archive_integration: PASSED")


if __name__ == "__main__":
    main()
