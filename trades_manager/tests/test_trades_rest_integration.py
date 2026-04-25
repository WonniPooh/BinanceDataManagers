"""
Integration test — REST gap-fill of ADAUSDT trades (3 days).

Downloads recent aggTrades via Binance REST API, verifies data is written
to DB with correct structure and reasonable values.

Hits mainnet Binance REST API — NOT for CI.

Usage:
    cd <project_root>
    python -m data_manager.trades_manager.tests.test_trades_rest_integration
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from data_manager.trades_manager.trades_rest_downloader import fill_gap_for_symbol
from data_manager.trades_manager.trades_db_manager import AggTradeDB
from data_manager.binance_rate_limiter import bnx_limiter

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL = "ADAUSDT"
LOOKBACK_DAYS = 3
DB_ROOT = str(_project_root / "logs" / "test_trades_rest_integration")


def _cleanup():
    p = Path(DB_ROOT)
    if p.exists():
        shutil.rmtree(p)


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def test_rest_fill_3days():
    """Download 3 days of ADAUSDT aggTrades via REST and verify."""
    _cleanup()

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - LOOKBACK_DAYS * 86_400_000

    status_queue: asyncio.Queue = asyncio.Queue()

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
        inserted = await fill_gap_for_symbol(
            db_root=DB_ROOT,
            symbol=SYMBOL,
            start_ms=start_ms,
            end_ms=now_ms,
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
    logging.info("REST fill done in %.1fs: %d trades inserted", elapsed, inserted)

    # ── Assertions ────────────────────────────────────────────────────
    assert inserted > 0, "No trades inserted — REST download produced nothing"

    # Open DB and verify
    db_path = Path(DB_ROOT) / SYMBOL / "trades.db"
    assert db_path.exists(), f"DB file not created at {db_path}"

    db = AggTradeDB(str(db_path))
    try:
        count = db.conn.execute("SELECT COUNT(*) FROM agg_trade").fetchone()[0]
        assert count == inserted, f"DB count {count} != inserted {inserted}"

        # Verify time range
        bounds = db.conn.execute(
            "SELECT MIN(trade_ts_ms), MAX(trade_ts_ms) FROM agg_trade"
        ).fetchone()
        min_ts, max_ts = bounds
        assert min_ts is not None and max_ts is not None

        # Oldest trade should be near start_ms (within 1 batch window)
        assert min_ts <= start_ms + 60_000, (
            f"Oldest trade {min_ts} too far from start {start_ms}")

        # Newest trade should be near now
        assert max_ts >= now_ms - 60_000, (
            f"Newest trade {max_ts} too far from now {now_ms}")

        logging.info("Trade range: %d -> %d (%.1f days)",
                      min_ts, max_ts, (max_ts - min_ts) / 86_400_000)

        # Verify data sanity
        sample = db.conn.execute(
            "SELECT agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num "
            "FROM agg_trade ORDER BY trade_ts_ms DESC LIMIT 5"
        ).fetchall()
        for row in sample:
            trade_id, ts, price, qty, is_maker, num = row
            assert trade_id > 0, f"Invalid trade_id: {trade_id}"
            assert price > 0, f"Invalid price: {price}"
            assert qty > 0, f"Invalid qty: {qty}"
            assert is_maker in (0, 1), f"Invalid is_buyer_maker: {is_maker}"
            assert num >= 1, f"Invalid trades_num: {num}"

        logging.info("Data sanity check passed — %d trades, sample verified", count)
    finally:
        db.close()

    logging.info("ALL CHECKS PASSED — %d trades in %.1fs", inserted, elapsed)


def main():
    _setup_logging()
    try:
        asyncio.run(test_rest_fill_3days())
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_trades_rest_integration: PASSED")


if __name__ == "__main__":
    main()
