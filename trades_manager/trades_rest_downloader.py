"""
Trades REST gap-filler — downloads recent aggTrades from Binance Futures
REST API to fill the gap between archived data and now.

Walks **backwards from end_ms** so the most recent data is available first.

Endpoint: GET /fapi/v1/aggTrades
  Weight: 20 per call  |  Max: 1000 trades per call

Cancellation: checks ``asyncio.CancelledError`` between batches.

Progress reporting:
    Pass an ``asyncio.Queue`` to receive per-symbol progress dicts::

        {"symbol": "BTCUSDT", "source": "rest", "phase": "loading",
         "pct": 60.0, "detail": "6000/10000 trades"}
        {"symbol": "BTCUSDT", "source": "rest", "phase": "done", "pct": 100.0}
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Dict

import aiohttp

from .trades_db_manager import AggTradeDB

logger = logging.getLogger("trades_rest_downloader")

AGGTRADES_WEIGHT = 20
BATCH_LIMIT = 1000
FAPI_BASE = "https://fapi.binance.com"
FAPI_TESTNET = "https://testnet.binancefuture.com"


def _db_path_for_symbol(db_root: str | Path, symbol: str, filename: str = "trades.db") -> Path:
    d = Path(db_root) / symbol
    d.mkdir(parents=True, exist_ok=True)
    return d / filename


def _aggtrade_to_row(t: dict) -> tuple:
    """Convert Binance REST aggTrade JSON to AggTradeDB row tuple."""
    a = int(t["a"])
    ts = int(t["T"])
    price = float(t["p"])
    qty = float(t["q"])
    is_m = 1 if t["m"] else 0
    f_id = int(t.get("f", a))
    l_id = int(t.get("l", a))
    trades_num = (l_id - f_id + 1) if l_id >= f_id else 1
    return (a, ts, price, qty, is_m, trades_num)


async def fill_gap(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
    db: AggTradeDB,
    rate_limiter: Any | None = None,
    status_queue: asyncio.Queue | None = None,
    testnet: bool = False,
    gap_id: int | None = None,
    gap_db: AggTradeDB | None = None,
) -> int:
    """Fetch aggTrades from REST API for [start_ms, end_ms) and insert into *db*.

    Walks **backwards** from *end_ms* so newest trades arrive first.
    Returns total number of rows inserted.

    If *gap_id* and *gap_db* are provided, the ``rest_gap`` row is updated
    after every batch so that a cancellation leaves an accurate frontier.
    """
    symbol = symbol.upper()
    base = FAPI_TESTNET if testnet else FAPI_BASE
    url = f"{base}/fapi/v1/aggTrades"

    total_inserted = 0
    cursor = end_ms
    batch_count = 0
    rest_start_mono = time.monotonic()
    last_info_log = rest_start_mono

    logger.info("[%s] REST fill_gap START  [%d → %d] (%.1fh)  testnet=%s",
                symbol, start_ms, end_ms,
                (end_ms - start_ms) / 3_600_000, testnet)

    def _emit(phase: str, pct: float, detail: str = "",
               covered_from_ms: int | None = None) -> None:
        if status_queue is None:
            return
        msg: Dict[str, Any] = {
            "symbol": symbol, "source": "rest", "phase": phase,
            "pct": round(pct, 1),
        }
        if detail:
            msg["detail"] = detail
        # covered_to_ms is always end_ms (REST fills backwards from end_ms)
        msg["covered_to_ms"] = end_ms
        if covered_from_ms is not None:
            msg["covered_from_ms"] = covered_from_ms
        status_queue.put_nowait(msg)

    MAX_RETRIES = 3
    error_stopped = False

    while cursor > start_ms:
        if rate_limiter is not None:
            await rate_limiter.acquire(AGGTRADES_WEIGHT)

        params: Dict[str, Any] = {
            "symbol": symbol,
            "endTime": cursor - 1,
            "limit": BATCH_LIMIT,
        }

        data = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if rate_limiter is not None:
                        raw_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                        if raw_weight:
                            rate_limiter.record_used_weight(int(raw_weight))

                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "60"))
                        logger.warning("[%s] REST 429 rate-limited, waiting %ds (attempt %d/%d)",
                                       symbol, retry_after, attempt, MAX_RETRIES)
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status >= 500:
                        text = await resp.text()
                        logger.warning("[%s] REST %d server error (attempt %d/%d): %s",
                                       symbol, resp.status, attempt, MAX_RETRIES, text[:200])
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error("[%s] REST aggTrades failed %d: %s",
                                     symbol, resp.status, text[:200])
                        error_stopped = True
                        break
                    data = await resp.json()
                    break  # success
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("[%s] REST aggTrades request failed (attempt %d/%d)",
                               symbol, attempt, MAX_RETRIES, exc_info=True)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(2 ** attempt)
                else:
                    error_stopped = True

        if error_stopped:
            break
        if data is None:
            break

        rows = [_aggtrade_to_row(t) for t in data]
        await asyncio.to_thread(db.insert_rows, rows)
        total_inserted += len(rows)

        elapsed_span = end_ms - start_ms
        oldest_ts = min(int(t["T"]) for t in data)
        filled_span = end_ms - oldest_ts
        pct = min(99.9, filled_span / max(1, elapsed_span) * 100.0)
        _emit("loading", pct, f"{total_inserted} trades",
              covered_from_ms=oldest_ts)

        # Update gap frontier so a cancellation leaves an accurate resume point.
        # Called after await-ing to_thread so the same connection is accessed
        # sequentially (event-loop thread only at this point).
        if gap_id is not None and gap_db is not None:
            gap_db.update_gap(gap_id, oldest_ts)

        logger.debug("[%s] Fetched %d trades, total %d",
                     symbol, len(rows), total_inserted)
        batch_count += 1
        now_mono = time.monotonic()
        if now_mono - last_info_log >= 10.0:
            logger.info("[%s] REST progress: %d trades in %d batches  %.1f%%  elapsed=%.0fs",
                        symbol, total_inserted, batch_count, pct, now_mono - rest_start_mono)
            last_info_log = now_mono

        if oldest_ts <= start_ms:
            break
        cursor = oldest_ts

        if rate_limiter is not None:
            await rate_limiter.check_backpressure()

    if error_stopped:
        _emit("error", 0.0, f"stopped after {total_inserted} trades (errors)")
        logger.error("[%s] REST gap-fill stopped early — %d trades inserted before error",
                     symbol, total_inserted)
    else:
        _emit("done", 100.0, f"{total_inserted} trades")
        logger.info("[%s] REST gap-fill done — %d trades inserted", symbol, total_inserted)
    return total_inserted


async def fill_gap_for_symbol(
    db_root: str | Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    db_filename: str = "trades.db",
    rate_limiter: Any | None = None,
    status_queue: asyncio.Queue | None = None,
    testnet: bool = False,
) -> int:
    """Convenience wrapper — opens its own session and DB."""
    symbol = symbol.upper()
    db_path = _db_path_for_symbol(db_root, symbol, db_filename)
    db = AggTradeDB(str(db_path))

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
