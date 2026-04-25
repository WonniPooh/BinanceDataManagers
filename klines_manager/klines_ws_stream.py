"""
Klines WebSocket live stream — subscribes to Binance ``<symbol>@kline_1m``
and inserts closed candles into CandleDB in real-time.

The stream buffers closed candles until ``flush_and_go_live()`` is called
(which the orchestrator does after initial REST + archive load completes).
After flushing, new candles are written directly to the DB as they close.

Usage::

    stream = KlineStream("ADAUSDT", db, on_candle=my_callback)
    stream.start()            # spawns background task
    ...
    stream.flush_and_go_live()  # after initial load finishes
    ...
    await stream.stop()

Binance Futures WS: ``wss://fstream.binance.com/ws/<symbol>@kline_1m``
Updates every 250ms; only closed candles (``k.x == true``) are persisted.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Coroutine, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .klines_db_manager import CandleDB

logger = logging.getLogger("klines_ws_stream")

WS_BASE = "wss://fstream.binance.com/ws"
WS_TESTNET = "wss://stream.binancefuture.com/ws"
INTERVAL = "1m"


def _kline_event_to_row(k: dict) -> dict:
    """Convert a Binance WS kline ``k`` sub-object into a CandleDB row dict."""
    return {
        "open_time":              int(k["t"]),
        "open":                   float(k["o"]),
        "high":                   float(k["h"]),
        "low":                    float(k["l"]),
        "close":                  float(k["c"]),
        "volume":                 float(k["v"]),
        "quote_volume":           float(k["q"]),
        "count":                  int(k["n"]),
        "taker_buy_volume":       float(k["V"]),
        "taker_buy_quote_volume": float(k["Q"]),
    }


class KlineStream:
    """Persistent WS connection that streams 1m candles into a CandleDB.

    Args:
        symbol:     Trading pair, e.g. ``"ADAUSDT"``
        db:         Open ``CandleDB`` instance — caller owns lifetime
        on_candle:  Optional async callback invoked with each closed candle row dict
        testnet:    Use testnet WS endpoint
        reconnect_interval: Seconds to wait before reconnecting after disconnect
    """

    def __init__(
        self,
        symbol: str,
        db: CandleDB,
        on_candle: Optional[Callable[[dict], Coroutine[Any, Any, None]]] = None,
        testnet: bool = False,
        reconnect_interval: float = 5.0,
    ):
        self._symbol = symbol.upper()
        self._db = db
        self._on_candle = on_candle
        self._testnet = testnet
        self._reconnect_interval = reconnect_interval

        base = WS_TESTNET if testnet else WS_BASE
        self._url = f"{base}/{self._symbol.lower()}@kline_{INTERVAL}"

        self._task: asyncio.Task | None = None
        self._connected = False
        self._live = False           # False = buffering, True = direct-to-DB
        self._buffer: list[dict] = []
        self._candles_written = 0
        self._ws: Any = None

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def live(self) -> bool:
        return self._live

    @property
    def candles_written(self) -> int:
        return self._candles_written

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)

    def start(self) -> None:
        """Spawn the background connection loop."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._connect_loop(),
            name=f"kline-ws-{self._symbol}",
        )

    async def stop(self) -> None:
        """Cancel the background task and close the connection."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    def flush_and_go_live(self) -> int:
        """Write all buffered candles to DB and switch to live mode.

        Returns the number of buffered candles flushed.
        """
        flushed = 0
        if self._buffer:
            self._db.insert_rows(self._buffer)
            flushed = len(self._buffer)
            self._candles_written += flushed
            logger.info("[%s] Flushed %d buffered candles", self._symbol, flushed)
            self._buffer.clear()
        self._live = True
        logger.info("[%s] Switched to live mode", self._symbol)
        return flushed

    # ── Private ───────────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        while True:
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    logger.info("[%s] WS connected to %s", self._symbol, self._url)
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            logger.warning("[%s] Bad JSON: %s", self._symbol, raw[:200])
                            continue
                        await self._handle_event(msg)
            except ConnectionClosed:
                logger.info("[%s] WS connection closed", self._symbol)
            except asyncio.CancelledError:
                return
            except OSError as exc:
                logger.warning("[%s] WS connection error: %s", self._symbol, exc)
            except Exception:
                logger.error("[%s] WS unexpected error", self._symbol, exc_info=True)
            finally:
                self._connected = False
                self._ws = None
            await asyncio.sleep(self._reconnect_interval)

    async def _handle_event(self, msg: dict) -> None:
        """Process a kline WS event. Only closed candles are persisted."""
        if msg.get("e") != "kline":
            return

        k = msg.get("k")
        if not k:
            return

        # Only persist closed candles
        if not k.get("x"):
            return

        row = _kline_event_to_row(k)

        if self._live:
            self._db.insert_one(row)
            self._candles_written += 1
            logger.debug("[%s] Wrote candle %d", self._symbol, row["open_time"])
        else:
            self._buffer.append(row)
            logger.debug("[%s] Buffered candle %d (buffer=%d)",
                         self._symbol, row["open_time"], len(self._buffer))

        if self._on_candle:
            try:
                await self._on_candle(row)
            except Exception:
                logger.error("[%s] on_candle callback failed", self._symbol, exc_info=True)
