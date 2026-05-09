"""
Integration test — TradesWSStream receives live aggTrades from Binance WS.

Connects to mainnet ``wss://fstream.binance.com/stream``, subscribes to
BTCUSDT aggTrades, waits for trades to arrive and flush to DB, verifies
data integrity.

Takes up to ~20 seconds (must wait for at least one flush cycle).
Hits mainnet Binance WS — NOT for CI.

Usage:
    cd <project_root>
    python -m BinanceDataManagers.trades_manager.tests.test_trades_ws_integration
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

from BinanceDataManagers.trades_manager.trades_ws_stream import TradesWSStream
from BinanceDataManagers.trades_manager.trades_db_manager import AggTradeDB

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL = "BTCUSDT"
DB_ROOT = _project_root / "logs" / "test_trades_ws_integration"
# Wait long enough for connection + at least one 10s flush cycle
MAX_WAIT_S = 25


def _cleanup():
    if DB_ROOT.exists():
        shutil.rmtree(DB_ROOT)


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def test_ws_receives_trades():
    """Subscribe to BTCUSDT, receive trades, verify flush to DB."""
    _cleanup()

    now_ms = int(time.time() * 1000)
    until_ms = now_ms + 60_000  # 1 minute subscription

    stream = TradesWSStream(db_root=str(DB_ROOT))
    stream.start()

    try:
        # ── Phase 1: Subscribe and wait for trades ────────────────────
        await stream.subscribe(SYMBOL, until_ms=until_ms)
        logging.info("Subscribed to %s, waiting for connection...", SYMBOL)

        assert SYMBOL in stream.active_symbols, (
            f"{SYMBOL} not in active symbols: {stream.active_symbols}")
        assert stream.connection_count >= 1, "No connections created"

        # Wait for some trades to arrive
        logging.info("Waiting up to %ds for trades to arrive...", MAX_WAIT_S)
        deadline = time.monotonic() + MAX_WAIT_S
        while time.monotonic() < deadline:
            if stream.trades_received > 0:
                break
            await asyncio.sleep(0.5)

        assert stream.trades_received > 0, (
            f"No trades received within {MAX_WAIT_S}s")
        logging.info("Received %d trades so far", stream.trades_received)

        # ── Phase 2: Wait for flush and verify DB ─────────────────────
        # The flush timer is 10s — wait for at least one cycle
        logging.info("Waiting for flush cycle...")
        pre_flush = stream.trades_flushed
        flush_deadline = time.monotonic() + 15
        while time.monotonic() < flush_deadline:
            if stream.trades_flushed > pre_flush:
                break
            await asyncio.sleep(1.0)

        assert stream.trades_flushed > 0, (
            f"No trades flushed after waiting — received={stream.trades_received}")
        logging.info("Flushed %d trades to DB", stream.trades_flushed)

        # Verify DB has data
        db_path = DB_ROOT / SYMBOL / "trades.db"
        assert db_path.exists(), f"DB file not created at {db_path}"

        db = AggTradeDB(str(db_path))
        try:
            count = db.conn.execute(
                "SELECT COUNT(*) FROM agg_trade"
            ).fetchone()[0]
            assert count > 0, f"DB has 0 trades after flush"
            logging.info("DB contains %d trades", count)

            # Verify data sanity
            row = db.conn.execute(
                "SELECT agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num "
                "FROM agg_trade ORDER BY trade_ts_ms DESC LIMIT 1"
            ).fetchone()
            assert row is not None
            trade_id, ts, price, qty, is_maker, num = row
            assert trade_id > 0, f"Invalid trade_id: {trade_id}"
            assert ts > now_ms - 120_000, f"Trade too old: {ts}"
            assert price > 0, f"Invalid price: {price}"
            assert qty > 0, f"Invalid qty: {qty}"
            assert is_maker in (0, 1), f"Invalid is_buyer_maker: {is_maker}"
            assert num >= 1, f"Invalid trades_num: {num}"
            logging.info("Latest trade: id=%d ts=%d price=%.2f qty=%.4f",
                          trade_id, ts, price, qty)
        finally:
            db.close()

        # ── Phase 3: Test multi-symbol (add ETHUSDT) ──────────────────
        await stream.subscribe("ETHUSDT", until_ms=until_ms)
        assert "ETHUSDT" in stream.active_symbols
        logging.info("Added ETHUSDT, active: %s", stream.active_symbols)

        # Wait a bit for ETH trades
        await asyncio.sleep(5)
        eth_buf = stream.buffer_size("ETHUSDT")
        logging.info("ETHUSDT buffer: %d trades", eth_buf)

        # ── Phase 4: Test unsubscribe ─────────────────────────────────
        await stream.unsubscribe("ETHUSDT")
        assert "ETHUSDT" not in stream.active_symbols, (
            "ETHUSDT should be unsubscribed")
        logging.info("Unsubscribed ETHUSDT, active: %s", stream.active_symbols)

        # ── Phase 5: Test extend ──────────────────────────────────────
        new_until = now_ms + 120_000
        await stream.extend(SYMBOL, until_ms=new_until)
        logging.info("Extended %s subscription", SYMBOL)

    finally:
        await stream.stop()

    logging.info("ALL CHECKS PASSED — received=%d, flushed=%d",
                  stream.trades_received, stream.trades_flushed)


def main():
    _setup_logging()
    try:
        asyncio.run(test_ws_receives_trades())
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_trades_ws_integration: PASSED")


if __name__ == "__main__":
    main()
