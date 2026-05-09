"""
Integration test — KlineStream receives live candles from Binance WS.

Connects to mainnet ``wss://fstream.binance.com/ws/btcusdt@kline_1m``,
waits for at least one closed candle, verifies it's written to DB.

Takes up to ~75 seconds (must wait for a candle to close).
Hits mainnet Binance WS — NOT for CI.

Usage:
    cd <project_root>
    python -m BinanceDataManagers.klines_manager.tests.test_klines_ws_integration
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

from BinanceDataManagers.klines_manager.klines_ws_stream import KlineStream
from BinanceDataManagers.klines_manager.klines_db_manager import CandleDB

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL = "BTCUSDT"
DB_ROOT = _project_root / "logs" / "test_klines_ws_integration"
DB_PATH = DB_ROOT / SYMBOL / f"{SYMBOL}_1m.db"
# Max wait for a closed candle: 75s (worst case: connect right after a close)
MAX_WAIT_S = 75


def _cleanup():
    if DB_ROOT.exists():
        shutil.rmtree(DB_ROOT)


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def test_ws_receives_candle():
    """Connect via WS, receive at least one closed candle, verify in DB."""
    _cleanup()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    db = CandleDB(str(DB_PATH))
    candle_received = asyncio.Event()
    received_candles: list[dict] = []

    async def _on_candle(row: dict) -> None:
        received_candles.append(row)
        logging.info("Received closed candle: open_time=%d", row["open_time"])
        candle_received.set()

    stream = KlineStream(SYMBOL, db, on_candle=_on_candle)

    try:
        # ── Phase 1: Start in buffer mode, verify buffering ───────────
        stream.start()
        logging.info("WS stream started in buffer mode, waiting for connection...")

        # Wait for connection
        for _ in range(20):
            if stream.connected:
                break
            await asyncio.sleep(0.5)
        assert stream.connected, "Failed to connect to Binance WS within 10s"
        logging.info("Connected to Binance WS")

        assert not stream.live, "Should start in buffer mode"

        # Wait for first closed candle
        logging.info("Waiting up to %ds for a closed candle...", MAX_WAIT_S)
        try:
            await asyncio.wait_for(candle_received.wait(), timeout=MAX_WAIT_S)
        except asyncio.TimeoutError:
            raise AssertionError(
                f"No closed candle received within {MAX_WAIT_S}s")

        assert len(received_candles) >= 1, "Expected at least 1 candle"
        assert stream.buffer_size >= 1, "Buffer should have at least 1 candle"

        # DB should be empty — candle was buffered, not written
        count_before = db.conn.execute("SELECT COUNT(*) FROM candle").fetchone()[0]
        assert count_before == 0, f"DB should be empty before flush, got {count_before}"
        logging.info("Buffer mode verified: %d candle(s) buffered, 0 in DB",
                      stream.buffer_size)

        # ── Phase 2: Flush and verify candles in DB ───────────────────
        flushed = stream.flush_and_go_live()
        assert flushed >= 1, f"Expected flush >= 1, got {flushed}"
        assert stream.live, "Should be in live mode after flush"

        count_after = db.conn.execute("SELECT COUNT(*) FROM candle").fetchone()[0]
        assert count_after >= 1, f"DB should have >= 1 candle after flush, got {count_after}"
        logging.info("Flushed %d candle(s) to DB, DB count: %d", flushed, count_after)

        # Verify the candle data is reasonable
        row = db.conn.execute(
            "SELECT open_time_ms, open, high, low, close, volume "
            "FROM candle ORDER BY open_time_ms DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        open_time_ms, open_p, high_p, low_p, close_p, volume = row
        now_ms = int(time.time() * 1000)
        assert now_ms - open_time_ms < 120_000, (
            f"Candle too old: {now_ms - open_time_ms}ms ago")
        assert high_p >= low_p, f"high {high_p} < low {low_p}"
        assert high_p >= open_p, f"high {high_p} < open {open_p}"
        assert high_p >= close_p, f"high {high_p} < close {close_p}"
        assert volume > 0, f"Volume should be > 0, got {volume}"
        logging.info("Candle data verified: open_time=%d open=%.2f close=%.2f vol=%.2f",
                      open_time_ms, open_p, close_p, volume)

        # ── Phase 3: Wait for one more candle in live mode ────────────
        candle_received.clear()
        logging.info("Waiting for one more candle in live mode...")
        try:
            await asyncio.wait_for(candle_received.wait(), timeout=MAX_WAIT_S)
        except asyncio.TimeoutError:
            raise AssertionError(
                f"No live candle received within {MAX_WAIT_S}s")

        count_live = db.conn.execute("SELECT COUNT(*) FROM candle").fetchone()[0]
        assert count_live > count_after, (
            f"DB count should have grown: {count_live} <= {count_after}")
        logging.info("Live mode verified: DB count grew from %d to %d",
                      count_after, count_live)

    finally:
        await stream.stop()
        db.close()

    logging.info("ALL CHECKS PASSED — %d candles total, buffer + live modes verified",
                  stream.candles_written)


def main():
    _setup_logging()
    try:
        asyncio.run(test_ws_receives_candle())
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_klines_ws_integration: PASSED")


if __name__ == "__main__":
    main()
