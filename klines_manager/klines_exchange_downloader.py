"""
Klines REST gap-filler — downloads recent candles from the Binance Futures
REST API to fill the gap between archived data and now.

Walks **backwards from now** so the most recent data is available first.

Endpoint: GET /fapi/v1/klines
  Weight: 5 per call  |  Max: 1500 candles per call (25 h at 1m)

Progress reporting:
    Pass an ``asyncio.Queue`` to receive per-symbol progress dicts::

        {"symbol": "ADAUSDT", "source": "api", "phase": "loading",
         "pct": 60.0, "detail": "4320/7200 candles"}
        {"symbol": "ADAUSDT", "source": "api", "phase": "done", "pct": 100.0}
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

from .klines_db_manager import CandleDB

logger = logging.getLogger("klines_exchange_downloader")

UTC = timezone.utc
INTERVAL = "1m"
KLINES_WEIGHT   = 5        # API weight per /fapi/v1/klines call
MAX_LIMIT       = 1500     # max candles per call
CANDLE_MS       = 60_000   # 1m candle duration in ms
FAPI_BASE       = "https://fapi.binance.com"
FAPI_TESTNET    = "https://testnet.binancefuture.com"


def _db_path_for_symbol(db_root: str | Path, symbol: str) -> Path:
    d = Path(db_root) / symbol
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{symbol}_{INTERVAL}.db"


async def fill_gap(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
    db: CandleDB,
    rate_limiter: Any | None = None,
    status_queue: asyncio.Queue | None = None,
    testnet: bool = False,
) -> int:
    """Fetch klines from REST API for [start_ms, end_ms) and insert into *db*.

    Walks **backwards** from *end_ms* so newest candles arrive first.
    Returns total number of rows inserted.

    Args:
        session:       aiohttp session
        symbol:        e.g. "ADAUSDT"
        start_ms:      inclusive start (ms epoch)
        end_ms:        exclusive end (ms epoch)
        db:            CandleDB instance (already open)
        rate_limiter:  object with ``acquire(weight)`` / ``record_used_weight(int)``
                       / ``check_backpressure()`` — e.g. ``bnx_limiter``
        status_queue:  optional asyncio.Queue for progress dicts
        testnet:       use testnet URL
    """
    symbol = symbol.upper()
    base = FAPI_TESTNET if testnet else FAPI_BASE
    url = f"{base}/fapi/v1/klines"

    total_expected = max(1, (end_ms - start_ms) // CANDLE_MS)
    total_inserted = 0
    cursor = end_ms  # walk backwards

    def _emit(phase: str, pct: float, detail: str = "") -> None:
        if status_queue is None:
            return
        msg: Dict[str, Any] = {
            "symbol": symbol, "source": "api", "phase": phase,
            "pct": round(pct, 1),
        }
        if detail:
            msg["detail"] = detail
        status_queue.put_nowait(msg)

    while cursor > start_ms:
        # Window for this batch — walk backwards
        batch_start = max(start_ms, cursor - MAX_LIMIT * CANDLE_MS)

        if rate_limiter is not None:
            await rate_limiter.acquire(KLINES_WEIGHT)

        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": batch_start,
            "endTime": cursor - 1,  # inclusive on Binance side
            "limit": MAX_LIMIT,
        }

        try:
            async with session.get(
                url, params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if rate_limiter is not None:
                    raw_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    if raw_weight:
                        rate_limiter.record_used_weight(int(raw_weight))

                resp.raise_for_status()
                data = await resp.json()
        except Exception:
            logger.error("[%s] REST klines failed (start=%d)", symbol, batch_start, exc_info=True)
            break

        if not data:
            logger.debug("[%s] No data for window %d–%d", symbol, batch_start, cursor)
            break

        rows = [_kline_array_to_row(k) for k in data]
        db.insert_rows(rows)
        total_inserted += len(rows)

        fetched_so_far = total_inserted
        pct = min(99.9, fetched_so_far / total_expected * 100.0)
        _emit("loading", pct, f"{fetched_so_far}/{total_expected} candles")

        logger.debug("[%s] Fetched %d candles (%d–%d), total %d",
                     symbol, len(rows), batch_start, cursor, total_inserted)

        # Move cursor backwards
        oldest_fetched = min(k[0] for k in data)  # open_time is index 0
        cursor = oldest_fetched

        if rate_limiter is not None:
            await rate_limiter.check_backpressure()

    _emit("done", 100.0)
    logger.info("[%s] REST gap-fill done — %d candles inserted", symbol, total_inserted)
    return total_inserted


async def fill_gap_for_symbol(
    db_root: str | Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    rate_limiter: Any | None = None,
    status_queue: asyncio.Queue | None = None,
    testnet: bool = False,
) -> int:
    """Convenience wrapper — opens its own session and DB."""
    symbol = symbol.upper()
    db_path = _db_path_for_symbol(db_root, symbol)
    db = CandleDB(str(db_path))

    try:
        async with aiohttp.ClientSession() as session:
            return await fill_gap(
                session, symbol, start_ms, end_ms, db,
                rate_limiter=rate_limiter,
                status_queue=status_queue,
                testnet=testnet,
            )
    finally:
        db.close()


def _kline_array_to_row(k: list) -> dict:
    """Convert Binance kline array response to CandleDB row dict.

    Binance returns: [open_time, open, high, low, close, volume,
                      close_time, quote_volume, count,
                      taker_buy_volume, taker_buy_quote_volume, ignore]
    """
    return {
        "open_time":              int(k[0]),
        "open":                   float(k[1]),
        "high":                   float(k[2]),
        "low":                    float(k[3]),
        "close":                  float(k[4]),
        "volume":                 float(k[5]),
        "quote_volume":           float(k[7]),
        "count":                  int(k[8]),
        "taker_buy_volume":       float(k[9]),
        "taker_buy_quote_volume": float(k[10]),
    }
