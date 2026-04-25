"""
Integration test — load 3 days of ADAUSDT klines and verify no gaps.

Hits mainnet Binance (S3 archive + REST API) — NOT for CI.

Usage:
    cd data_manager/klines_manager
    python -m tests.test_klines_loader_integration

    # or from project root:
    python -m data_manager.klines_manager.tests.test_klines_loader_integration
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import time
from pathlib import Path

# Ensure data_manager is importable when run from various CWDs
_project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from data_manager.klines_manager.klines_loader import load_symbol, find_gaps
from data_manager.klines_manager.klines_db_manager import CandleDB
from data_manager.klines_manager.klines_archive_downloader import db_path_for_symbol
from data_manager.klines_manager.klines_exchange_downloader import CANDLE_MS
from data_manager.binance_rate_limiter import bnx_limiter

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL = "ADAUSDT"
LOOKBACK_DAYS = 3
DB_ROOT = str(_project_root / "logs" / "test_klines_loader_integration")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cleanup():
    p = Path(DB_ROOT)
    if p.exists():
        shutil.rmtree(p)


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


# ── Test ──────────────────────────────────────────────────────────────────────

async def test_load_3days_no_gaps():
    """Load 3 days of ADAUSDT and verify continuous 1m candles."""
    _cleanup()

    status_queue: asyncio.Queue = asyncio.Queue()

    # Drain progress messages in background
    async def _drain():
        while True:
            try:
                msg = await asyncio.wait_for(status_queue.get(), timeout=1.0)
                logging.info("PROGRESS: %s", msg)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

    drain_task = asyncio.create_task(_drain())

    t0 = time.monotonic()
    try:
        result = await load_symbol(
            SYMBOL,
            lookback_days=LOOKBACK_DAYS,
            db_root=DB_ROOT,
            rate_limiter=bnx_limiter,
            status_queue=status_queue,
        )
    finally:
        drain_task.cancel()
        try:
            await drain_task
        except asyncio.CancelledError:
            pass

    elapsed = time.monotonic() - t0
    logging.info("load_symbol returned in %.1fs: %s", elapsed, result)

    # ── Assertions ────────────────────────────────────────────────────────
    assert result["symbol"] == SYMBOL, f"Expected {SYMBOL}, got {result['symbol']}"
    assert result["candles"] > 0, "No candles loaded"

    # Expected candle count: 3 days * 24h * 60m = 4320 (minus current incomplete)
    expected_min = LOOKBACK_DAYS * 24 * 60 - 10  # small slack
    assert result["candles"] >= expected_min, (
        f"Too few candles: {result['candles']} < {expected_min}")

    # Verify no gaps remain
    assert result["gaps_remaining"] == 0, (
        f"Expected 0 remaining gaps, got {result['gaps_remaining']}")

    # Double-check by opening DB and running find_gaps directly
    db_path = db_path_for_symbol(DB_ROOT, SYMBOL)
    db = CandleDB(str(db_path))
    try:
        start_ms = result["start_ms"]
        end_ms = result["end_ms"]

        gaps = find_gaps(db, start_ms, end_ms)
        logging.info("Direct gap check: %d gaps found", len(gaps))
        for g_start, g_end in gaps:
            gap_minutes = (g_end - g_start) / CANDLE_MS
            logging.warning("  Gap: %d -> %d (%.1f minutes)", g_start, g_end, gap_minutes)

        assert len(gaps) == 0, f"Direct gap check found {len(gaps)} gaps: {gaps}"

        # Verify candle count in DB matches
        row = db.conn.execute(
            "SELECT COUNT(*) FROM candle WHERE open_time_ms BETWEEN ? AND ?",
            (start_ms, end_ms),
        ).fetchone()
        db_count = row[0]
        logging.info("DB candle count: %d (result reported: %d)", db_count, result["candles"])
        assert db_count == result["candles"], (
            f"DB count {db_count} != result count {result['candles']}")

        # Verify oldest and newest candle are in range
        bounds = db.conn.execute(
            "SELECT MIN(open_time_ms), MAX(open_time_ms) FROM candle "
            "WHERE open_time_ms BETWEEN ? AND ?",
            (start_ms, end_ms),
        ).fetchone()
        logging.info("Candle range: %d -> %d", bounds[0], bounds[1])
        assert bounds[0] <= start_ms + CANDLE_MS, (
            f"Oldest candle {bounds[0]} too far from start {start_ms}")
    finally:
        db.close()

    logging.info("ALL CHECKS PASSED — %d candles, 0 gaps", result["candles"])


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    _setup_logging()
    try:
        asyncio.run(test_load_3days_no_gaps())
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_klines_loader_integration: PASSED")


if __name__ == "__main__":
    main()
