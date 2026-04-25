"""
Trades WebSocket live stream — multi-symbol aggTrade feed.

Manages 1+ WS connections to Binance combined stream endpoint, max 10
symbols per connection.  Trades are buffered per-symbol and flushed to
the corresponding AggTradeDB every ``FLUSH_INTERVAL_S`` seconds (default 10).

Each subscription has an expiry timestamp (``until_ms``).  Expired
subscriptions are automatically cleaned up.

Usage::

    stream = TradesWSStream(db_root="db_files")
    stream.start()

    stream.subscribe("BTCUSDT", until_ms=now_ms + 3600_000)  # 1 hour
    stream.subscribe("ETHUSDT", until_ms=now_ms + 7200_000)
    stream.extend("BTCUSDT", until_ms=now_ms + 10800_000)    # extend to 3h
    stream.unsubscribe("ETHUSDT")                              # immediate

    await stream.stop()

Binance Futures combined WS:
  ``wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade``
Dynamic sub/unsub via ``{"method": "SUBSCRIBE", "params": [...], "id": N}``
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .trades_db_manager import AggTradeDB

logger = logging.getLogger("trades_ws_stream")

WS_BASE = "wss://fstream.binance.com/stream"
WS_TESTNET = "wss://stream.binancefuture.com/stream"

MAX_SYMBOLS_PER_CONN = 10
FLUSH_INTERVAL_S = 10.0
EXPIRY_CHECK_S = 1800.0  # 30 minutes


MAX_BUFFER_PER_SYMBOL = 500_000  # ~500K trades safety cap


def _aggtrade_event_to_row(t: dict) -> tuple:
    """Convert Binance WS aggTrade event to AggTradeDB row tuple."""
    a = int(t["a"])
    ts = int(t["T"])
    price = float(t["p"])
    qty = float(t["q"])
    is_m = 1 if t["m"] else 0
    f_id = int(t.get("f", a))
    l_id = int(t.get("l", a))
    trades_num = (l_id - f_id + 1) if l_id >= f_id else 1
    return (a, ts, price, qty, is_m, trades_num)


# Minimum gap (seconds) between last pre-disconnect trade and first
# post-reconnect trade that triggers automatic rest_gap registration.
_RECONNECT_GAP_THRESHOLD_S = 10


class _Connection:
    """A single WS connection handling up to MAX_SYMBOLS_PER_CONN symbols."""

    def __init__(
        self,
        conn_id: int,
        on_trade: Callable[[str, tuple], None],
        testnet: bool = False,
        reconnect_interval: float = 5.0,
        on_reconnect: Callable[[set[str]], None] | None = None,
    ):
        self._id = conn_id
        self._on_trade = on_trade
        self._testnet = testnet
        self._reconnect_interval = reconnect_interval
        self._on_reconnect = on_reconnect
        self._ever_connected = False

        self._symbols: set[str] = set()
        self._ws: Any = None
        self._task: asyncio.Task | None = None
        self._connected = False
        self._msg_id = 0

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def symbol_count(self) -> int:
        return len(self._symbols)

    @property
    def symbols(self) -> set[str]:
        return set(self._symbols)

    def has_room(self) -> bool:
        return len(self._symbols) < MAX_SYMBOLS_PER_CONN

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._connect_loop(),
            name=f"trades-ws-conn-{self._id}",
        )

    async def stop(self) -> None:
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

    async def add_symbol(self, symbol: str) -> None:
        """Subscribe to a symbol on this connection."""
        sym = symbol.upper()
        if sym in self._symbols:
            return
        self._symbols.add(sym)
        if self._ws and self._connected:
            await self._send_subscribe([sym])

    async def remove_symbol(self, symbol: str) -> None:
        """Unsubscribe a symbol from this connection."""
        sym = symbol.upper()
        if sym not in self._symbols:
            return
        self._symbols.discard(sym)
        if self._ws and self._connected:
            await self._send_unsubscribe([sym])

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def _send_subscribe(self, symbols: list[str]) -> None:
        streams = [f"{s.lower()}@aggTrade" for s in symbols]
        msg = {"method": "SUBSCRIBE", "params": streams, "id": self._next_id()}
        try:
            await self._ws.send(json.dumps(msg))
            logger.debug("[conn-%d] SUBSCRIBE %s", self._id, streams)
        except Exception:
            logger.warning("[conn-%d] Failed to send SUBSCRIBE", self._id, exc_info=True)

    async def _send_unsubscribe(self, symbols: list[str]) -> None:
        streams = [f"{s.lower()}@aggTrade" for s in symbols]
        msg = {"method": "UNSUBSCRIBE", "params": streams, "id": self._next_id()}
        try:
            await self._ws.send(json.dumps(msg))
            logger.debug("[conn-%d] UNSUBSCRIBE %s", self._id, streams)
        except Exception:
            logger.warning("[conn-%d] Failed to send UNSUBSCRIBE", self._id, exc_info=True)

    async def _connect_loop(self) -> None:
        base = WS_TESTNET if self._testnet else WS_BASE
        while True:
            # Need at least one symbol to form a valid combined stream URL
            while not self._symbols:
                await asyncio.sleep(0.5)

            try:
                # Include current symbols in URL for reliable initial subscription
                streams = "/".join(f"{s.lower()}@aggTrade" for s in self._symbols)
                url = f"{base}?streams={streams}"

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    is_reconnect = self._ever_connected
                    self._ever_connected = True
                    logger.info("[conn-%d] WS %s (%d streams)",
                                self._id,
                                "reconnected" if is_reconnect else "connected",
                                len(self._symbols))
                    if is_reconnect and self._on_reconnect:
                        self._on_reconnect(set(self._symbols))

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        self._handle_message(msg)
            except ConnectionClosed:
                logger.info("[conn-%d] WS connection closed", self._id)
            except asyncio.CancelledError:
                return
            except OSError as exc:
                logger.warning("[conn-%d] WS connection error: %s", self._id, exc)
            except Exception:
                logger.error("[conn-%d] WS unexpected error", self._id, exc_info=True)
            finally:
                self._connected = False
                self._ws = None
            await asyncio.sleep(self._reconnect_interval)

    def _handle_message(self, msg: dict) -> None:
        """Route a combined-stream message to the trade handler."""
        # Combined stream format: {"stream": "btcusdt@aggTrade", "data": {...}}
        data = msg.get("data")
        if not data:
            return
        if data.get("e") != "aggTrade":
            return

        symbol = data.get("s", "").upper()
        if symbol not in self._symbols:
            return

        try:
            row = _aggtrade_event_to_row(data)
            self._on_trade(symbol, row)
        except Exception:
            logger.warning("[conn-%d] Failed to parse aggTrade for %s",
                           self._id, symbol, exc_info=True)


class TradesWSStream:
    """Multi-symbol aggTrade WS manager with connection pooling.

    Args:
        db_root:    Root directory for per-symbol trade DBs
        on_trade:   Optional async callback ``(symbol, row_tuple)`` for each trade
        testnet:    Use testnet WS endpoint
    """

    def __init__(
        self,
        db_root: str | Path = "db_files",
        on_trade: Optional[Callable[[str, tuple], Coroutine[Any, Any, None]]] = None,
        testnet: bool = False,
        on_reconnect_gap: Optional[Callable[[str, int, int], None]] = None,
    ):
        self._db_root = Path(db_root)
        self._on_trade_cb = on_trade
        self._testnet = testnet
        self._on_reconnect_gap_cb = on_reconnect_gap

        self._connections: list[_Connection] = []
        self._conn_counter = 0
        # symbol -> connection index
        self._symbol_conn: dict[str, _Connection] = {}
        # symbol -> expiry timestamp (ms)
        self._subscriptions: dict[str, int] = {}
        # symbol -> list of buffered row tuples
        self._buffers: dict[str, list[tuple]] = {}
        # symbol -> open AggTradeDB
        self._dbs: dict[str, AggTradeDB] = {}

        self._flush_task: asyncio.Task | None = None
        self._expiry_task: asyncio.Task | None = None
        self._started = False
        self._trades_received = 0
        self._trades_flushed = 0

        # First-trade synchronisation: allows callers to wait until the
        # first live trade arrives for a freshly subscribed symbol.
        self._first_trade_ts: dict[str, int] = {}
        self._first_trade_events: dict[str, asyncio.Event] = {}

        # Reconnect gap detection: last trade ts per symbol (ms)
        self._last_trade_ts: dict[str, int] = {}
        # Symbols that need gap detection on their next trade
        self._pending_gap_check: dict[str, int] = {}  # symbol -> pre_disconnect_ts

    @property
    def active_symbols(self) -> list[str]:
        return sorted(self._subscriptions.keys())

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    @property
    def trades_received(self) -> int:
        return self._trades_received

    @property
    def trades_flushed(self) -> int:
        return self._trades_flushed

    def buffer_size(self, symbol: str | None = None) -> int:
        if symbol:
            return len(self._buffers.get(symbol.upper(), []))
        return sum(len(b) for b in self._buffers.values())

    def start(self) -> None:
        """Start flush and expiry timers."""
        if self._started:
            return
        self._started = True
        self._flush_task = asyncio.create_task(
            self._flush_loop(), name="trades-ws-flush",
        )
        self._expiry_task = asyncio.create_task(
            self._expiry_loop(), name="trades-ws-expiry",
        )

    async def stop(self) -> None:
        """Stop all connections and timers, flush remaining buffers."""
        self._started = False
        for task in (self._flush_task, self._expiry_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Wait briefly for any in-flight worker-thread DB writes spawned by
        # the flush loop to finish, so _flush_all doesn't race with them.
        # asyncio.to_thread futures are tracked on the loop; a short sleep
        # yields control so they can complete.
        await asyncio.sleep(0.05)

        # Final flush
        self._flush_all()

        # Stop all connections
        for conn in self._connections:
            await conn.stop()
        self._connections.clear()
        self._symbol_conn.clear()
        self._subscriptions.clear()
        self._buffers.clear()

        # Close all DBs
        for db in self._dbs.values():
            db.close()
        self._dbs.clear()

    async def subscribe(self, symbol: str, until_ms: int) -> None:
        """Subscribe to aggTrade stream for *symbol* until *until_ms*.

        If already subscribed, updates the expiry.
        """
        sym = symbol.upper()
        self._subscriptions[sym] = until_ms

        if sym not in self._buffers:
            self._buffers[sym] = []

        if sym in self._symbol_conn:
            # Already on a connection, nothing to do
            return

        # Find a connection with room or create a new one
        conn = self._find_or_create_connection()
        self._symbol_conn[sym] = conn
        await conn.add_symbol(sym)
        logger.info("[%s] Subscribed (until %d, conn-%d)", sym, until_ms, conn._id)

    async def extend(self, symbol: str, until_ms: int) -> None:
        """Extend the subscription expiry for *symbol*."""
        sym = symbol.upper()
        if sym not in self._subscriptions:
            logger.warning("[%s] Cannot extend — not subscribed", sym)
            return
        old = self._subscriptions[sym]
        if until_ms <= old:
            logger.debug("[%s] extend ignored — new %d <= current %d", sym, until_ms, old)
            return
        self._subscriptions[sym] = until_ms
        logger.info("[%s] Extended until %d (was %d)", sym, until_ms, old)

    async def unsubscribe(self, symbol: str) -> None:
        """Immediately unsubscribe *symbol*, flush remaining buffer."""
        sym = symbol.upper()
        if sym not in self._subscriptions:
            return

        # Flush remaining buffer for this symbol
        self._flush_symbol(sym)

        # Remove from connection
        conn = self._symbol_conn.pop(sym, None)
        if conn:
            await conn.remove_symbol(sym)
            # If connection is now empty, stop and remove it.
            # Guard against concurrent unsubscribes already removing this conn.
            if conn.symbol_count == 0 and conn in self._connections:
                await conn.stop()
                self._connections.remove(conn)
                logger.info("[conn-%d] Closed (empty)", conn._id)

        self._subscriptions.pop(sym, None)
        self._buffers.pop(sym, None)
        self._first_trade_ts.pop(sym, None)
        self._first_trade_events.pop(sym, None)
        self._last_trade_ts.pop(sym, None)
        self._pending_gap_check.pop(sym, None)
        self._close_db(sym)
        logger.info("[%s] Unsubscribed", sym)

    # ── Internal ──────────────────────────────────────────────────────────

    def _on_reconnect(self, symbols: set[str]) -> None:
        """Called by _Connection when WS reconnects after a drop."""
        for sym in symbols:
            last_ts = self._last_trade_ts.get(sym)
            if last_ts is not None:
                self._pending_gap_check[sym] = last_ts
                logger.info("[%s] WS reconnected — will check for gap "
                            "(last_trade_ts=%d)", sym, last_ts)

    def _record_reconnect_gap(self, symbol: str, pre_ts: int, post_ts: int) -> None:
        """Record a rest_gap row for the reconnection gap and notify owner."""
        gap_ms = post_ts - pre_ts
        if gap_ms < _RECONNECT_GAP_THRESHOLD_S * 1000:
            return
        try:
            db = self._get_db(symbol)
            db.open_gap(pre_ts, post_ts)
            logger.warning(
                "[%s] Reconnect gap recorded: %d → %d (%.1fs)",
                symbol, pre_ts, post_ts, gap_ms / 1000,
            )
        except Exception:
            logger.error("[%s] Failed to record reconnect gap",
                         symbol, exc_info=True)
        if self._on_reconnect_gap_cb is not None:
            try:
                self._on_reconnect_gap_cb(symbol, pre_ts, post_ts)
            except Exception:
                logger.error("[%s] on_reconnect_gap callback failed",
                             symbol, exc_info=True)

    def _on_trade(self, symbol: str, row: tuple) -> None:
        """Called by _Connection for each incoming trade."""
        buf = self._buffers.get(symbol)
        if buf is None:
            return
        if len(buf) >= MAX_BUFFER_PER_SYMBOL:
            logger.warning("[%s] Buffer at cap (%d) — dropping trade",
                           symbol, MAX_BUFFER_PER_SYMBOL)
            return
        buf.append(row)
        self._trades_received += 1

        ts = int(row[1])  # trade_ts_ms

        # Check for reconnection gap
        pre_ts = self._pending_gap_check.pop(symbol, None)
        if pre_ts is not None:
            self._record_reconnect_gap(symbol, pre_ts, ts)

        self._last_trade_ts[symbol] = ts

        # Record first-trade timestamp and unblock any waiter.
        if symbol not in self._first_trade_ts:
            self._first_trade_ts[symbol] = ts
            evt = self._first_trade_events.get(symbol)
            if evt:
                evt.set()
            logger.info("[%s] First WS trade arrived  ts=%d", symbol, ts)

    async def wait_for_first_trade(self, symbol: str, timeout: float = 30.0) -> int | None:
        """Block until the first WS trade for *symbol*, return its ts (ms).

        Returns ``None`` on timeout.  Safe to call before ``subscribe()``
        has actually run — the event is created lazily.
        """
        sym = symbol.upper()
        # Already have a first trade — return immediately.
        ts = self._first_trade_ts.get(sym)
        if ts is not None:
            return ts
        # Create event lazily (subscribe task may not have run yet).
        if sym not in self._first_trade_events:
            self._first_trade_events[sym] = asyncio.Event()
        evt = self._first_trade_events[sym]
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("[%s] Timed out waiting %.0fs for first WS trade", sym, timeout)
            return None
        return self._first_trade_ts.get(sym)

    def _find_or_create_connection(self) -> _Connection:
        """Return an existing connection with room, or create a new one."""
        for conn in self._connections:
            if conn.has_room():
                return conn
        # Create new connection
        self._conn_counter += 1
        conn = _Connection(
            self._conn_counter,
            on_trade=self._on_trade,
            testnet=self._testnet,
            on_reconnect=self._on_reconnect,
        )
        conn.start()
        self._connections.append(conn)
        logger.info("[conn-%d] Created (total connections: %d)",
                     conn._id, len(self._connections))
        return conn

    def _get_db(self, symbol: str) -> AggTradeDB:
        """Get or open the AggTradeDB for a symbol."""
        if symbol not in self._dbs:
            sym_dir = self._db_root / symbol
            sym_dir.mkdir(parents=True, exist_ok=True)
            db_path = sym_dir / "trades.db"
            self._dbs[symbol] = AggTradeDB(str(db_path))
        return self._dbs[symbol]

    def _close_db(self, symbol: str) -> None:
        db = self._dbs.pop(symbol, None)
        if db:
            db.close()

    def _flush_symbol(self, symbol: str) -> int:
        """Flush buffer for a single symbol (sync, for shutdown paths). Returns rows written."""
        buf = self._buffers.get(symbol)
        if not buf:
            return 0
        db = self._get_db(symbol)
        count = len(buf)
        db.insert_rows(buf)
        self._trades_flushed += count
        buf.clear()
        return count

    def _flush_all(self) -> None:
        """Flush all symbol buffers (sync, for shutdown paths)."""
        for sym in list(self._buffers.keys()):
            try:
                n = self._flush_symbol(sym)
                if n > 0:
                    logger.debug("[%s] Flushed %d trades", sym, n)
            except Exception:
                logger.error("[%s] _flush_all: failed to flush", sym,
                             exc_info=True)

    async def flush_symbol_async(self, symbol: str) -> int:
        """Flush buffer for *symbol* without blocking the event loop."""
        buf = self._buffers.get(symbol)
        if not buf:
            return 0
        # Swap buffer in the event-loop thread (safe, no race with _on_trade)
        self._buffers[symbol] = []
        db = self._get_db(symbol)
        try:
            await asyncio.to_thread(db.insert_rows, buf)
        except Exception:
            # Restore unflushed trades so they aren't lost.
            self._buffers[symbol] = buf + self._buffers[symbol]
            logger.error("[%s] flush_symbol_async: DB write failed, "
                         "%d trades returned to buffer", symbol, len(buf),
                         exc_info=True)
            raise
        written = len(buf)
        self._trades_flushed += written
        logger.debug("[%s] Force-flushed %d trades", symbol, written)
        return written

    @staticmethod
    def _write_flushed(work: list[tuple[str, AggTradeDB, list[tuple]]]) -> tuple[int, list[tuple[str, list[tuple]]]]:
        """Write flushed buffers to DB. Runs in a worker thread.

        Returns ``(total_written, failed)`` where *failed* is a list of
        ``(symbol, rows)`` that could not be written.
        """
        total = 0
        failed: list[tuple[str, list[tuple]]] = []
        for sym, db, rows in work:
            try:
                db.insert_rows(rows)
                total += len(rows)
                if rows:
                    logger.debug("[%s] Flushed %d trades", sym, len(rows))
            except Exception:
                logger.error("[%s] _write_flushed: DB write failed for %d rows",
                             sym, len(rows), exc_info=True)
                failed.append((sym, rows))
        return total, failed

    async def _flush_loop(self) -> None:
        """Periodic flush every FLUSH_INTERVAL_S seconds.

        Snapshots buffers in the event-loop thread (no race with _on_trade),
        then writes to DB in a worker thread so the event loop stays responsive.
        """
        flush_count = 0
        while True:
            try:
                await asyncio.sleep(FLUSH_INTERVAL_S)
                # Snapshot & swap buffers in the event-loop thread
                work: list[tuple[str, AggTradeDB, list[tuple]]] = []
                for sym in list(self._buffers.keys()):
                    buf = self._buffers[sym]
                    if not buf:
                        continue
                    # Swap: _on_trade will append to the fresh list
                    self._buffers[sym] = []
                    work.append((sym, self._get_db(sym), buf))

                if work:
                    count, failed = await asyncio.to_thread(self._write_flushed, work)
                    self._trades_flushed += count
                    # Restore failed buffers so trades aren't lost.
                    for sym, rows in failed:
                        self._buffers[sym] = rows + self._buffers.get(sym, [])

                flush_count += 1
                # Log a summary every ~5 minutes (30 flushes at 10s interval)
                if flush_count % 10 == 0:
                    logger.info(
                        "WS trades alive — received=%d flushed=%d symbols=%d",
                        self._trades_received, self._trades_flushed,
                        len(self._subscriptions),
                    )
            except asyncio.CancelledError:
                return
            except Exception:
                logger.error("Flush loop error", exc_info=True)

    async def _expiry_loop(self) -> None:
        """Periodic sweep of expired subscriptions."""
        while True:
            try:
                await asyncio.sleep(EXPIRY_CHECK_S)
                now_ms = int(time.time() * 1000)
                expired = [
                    sym for sym, expiry in self._subscriptions.items()
                    if now_ms >= expiry
                ]
                for sym in expired:
                    logger.info("[%s] Subscription expired", sym)
                    await self.unsubscribe(sym)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.error("Expiry loop error", exc_info=True)
