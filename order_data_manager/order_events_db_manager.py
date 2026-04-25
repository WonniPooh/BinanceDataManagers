"""
SQLite manager for per-symbol order events.

Schema stores one row per order event (NEW, TRADE, CANCELED, etc.) as
received from either UserDataWS ORDER_TRADE_UPDATE or allOrders REST sync.
Rows are upserted by (order_id, execution_type, transaction_time_ms) so
both sources are idempotent.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS order_event (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id                 INTEGER NOT NULL,
    symbol                   TEXT    NOT NULL,
    client_order_id          TEXT    NOT NULL DEFAULT '',
    side                     TEXT    NOT NULL,
    order_type               TEXT    NOT NULL,
    execution_type           TEXT    NOT NULL,
    order_status             TEXT    NOT NULL,
    order_price              REAL    NOT NULL DEFAULT 0,
    stop_price               REAL    NOT NULL DEFAULT 0,
    order_qty                REAL    NOT NULL DEFAULT 0,
    last_fill_price          REAL    NOT NULL DEFAULT 0,
    last_fill_qty            REAL    NOT NULL DEFAULT 0,
    filled_qty_accumulated   REAL    NOT NULL DEFAULT 0,
    avg_price                REAL    NOT NULL DEFAULT 0,
    commission               REAL    NOT NULL DEFAULT 0,
    commission_asset         TEXT    NOT NULL DEFAULT '',
    realized_pnl             REAL    NOT NULL DEFAULT 0,
    trade_id                 INTEGER NOT NULL DEFAULT 0,
    event_time_ms            INTEGER NOT NULL DEFAULT 0,
    transaction_time_ms      INTEGER NOT NULL,
    position_side            TEXT    NOT NULL DEFAULT 'BOTH',
    is_maker                 INTEGER NOT NULL DEFAULT 0,
    is_reduce_only           INTEGER NOT NULL DEFAULT 0,
    time_in_force            TEXT    NOT NULL DEFAULT 'GTC',
    UNIQUE(order_id, execution_type, transaction_time_ms)
);

CREATE INDEX IF NOT EXISTS idx_order_event_order_id
    ON order_event(order_id);
CREATE INDEX IF NOT EXISTS idx_order_event_symbol_ts
    ON order_event(symbol, transaction_time_ms);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_amendment (
    amendment_id      INTEGER PRIMARY KEY,
    order_id          INTEGER NOT NULL,
    symbol            TEXT    NOT NULL,
    client_order_id   TEXT    NOT NULL DEFAULT '',
    time_ms           INTEGER NOT NULL,
    price_before      REAL    NOT NULL DEFAULT 0,
    price_after       REAL    NOT NULL DEFAULT 0,
    qty_before        REAL    NOT NULL DEFAULT 0,
    qty_after         REAL    NOT NULL DEFAULT 0,
    amendment_count   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_order_amendment_order_id
    ON order_amendment(order_id);
CREATE INDEX IF NOT EXISTS idx_order_amendment_symbol_ts
    ON order_amendment(symbol, time_ms);
"""


_ROW_DEFAULTS: Dict[str, Any] = {
    "client_order_id": "", "stop_price": 0, "last_fill_price": 0,
    "last_fill_qty": 0, "filled_qty_accumulated": 0, "avg_price": 0,
    "commission": 0, "commission_asset": "", "realized_pnl": 0,
    "trade_id": 0, "event_time_ms": 0, "position_side": "BOTH",
    "is_maker": 0, "is_reduce_only": 0, "time_in_force": "GTC",
}


class OrderEventDB:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(_CREATE_SQL)
        self.conn.commit()
        self._ensure_amendment_dedup_index()

    def _ensure_amendment_dedup_index(self) -> None:
        """Create UNIQUE(order_id, time_ms) index, deduplicating existing data if needed."""
        # Check if index already exists — skip all work if so
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_amendment_dedup'"
        ).fetchone()
        if exists:
            return
        # Defensively dedup before creating — avoids needing to catch
        # the ambiguous IntegrityError/OperationalError from CREATE INDEX
        self.conn.execute(
            "DELETE FROM order_amendment WHERE rowid NOT IN ("
            "  SELECT MAX(rowid) FROM order_amendment "
            "  GROUP BY order_id, time_ms"
            ")"
        )
        self.conn.execute(
            "CREATE UNIQUE INDEX idx_amendment_dedup "
            "ON order_amendment(order_id, time_ms)"
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ── write ────────────────────────────────────────────────────────────────

    def insert_one(self, row: Dict[str, Any]) -> None:
        self._upsert([row])

    def insert_rows(self, rows: List[Dict[str, Any]]) -> None:
        if rows:
            self._upsert(rows)

    def _upsert(self, rows: List[Dict[str, Any]]) -> None:
        filled = [{**_ROW_DEFAULTS, **r} for r in rows]
        self.conn.executemany(
            """
            INSERT INTO order_event (
                order_id, symbol, client_order_id, side, order_type,
                execution_type, order_status, order_price, stop_price,
                order_qty, last_fill_price, last_fill_qty,
                filled_qty_accumulated, avg_price, commission,
                commission_asset, realized_pnl, trade_id,
                event_time_ms, transaction_time_ms, position_side,
                is_maker, is_reduce_only, time_in_force
            ) VALUES (
                :order_id, :symbol, :client_order_id, :side, :order_type,
                :execution_type, :order_status, :order_price, :stop_price,
                :order_qty, :last_fill_price, :last_fill_qty,
                :filled_qty_accumulated, :avg_price, :commission,
                :commission_asset, :realized_pnl, :trade_id,
                :event_time_ms, :transaction_time_ms, :position_side,
                :is_maker, :is_reduce_only, :time_in_force
            )
            ON CONFLICT(order_id, execution_type, transaction_time_ms)
            DO UPDATE SET
                order_status           = excluded.order_status,
                filled_qty_accumulated = excluded.filled_qty_accumulated,
                avg_price              = excluded.avg_price,
                last_fill_price        = excluded.last_fill_price,
                last_fill_qty          = excluded.last_fill_qty,
                commission             = excluded.commission,
                realized_pnl           = excluded.realized_pnl
            """,
            filled,
        )
        self.conn.commit()

    # ── read ─────────────────────────────────────────────────────────────────

    def get_latest_transaction_time(self) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(transaction_time_ms) FROM order_event"
        ).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_orders_in_window(
        self, start_ms: int, end_ms: int
    ) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM order_event "
            "WHERE transaction_time_ms >= ? AND transaction_time_ms <= ? "
            "ORDER BY transaction_time_ms",
            (start_ms, end_ms),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_events_by_symbol(
        self,
        symbol: str,
        start_time_ms: int = 0,
        end_time_ms: Optional[int] = None,
        limit: int = 10_000,
    ) -> List[Dict[str, Any]]:
        if end_time_ms is not None:
            cur = self.conn.execute(
                "SELECT * FROM order_event "
                "WHERE symbol = ? AND transaction_time_ms >= ? "
                "AND transaction_time_ms <= ? "
                "ORDER BY transaction_time_ms LIMIT ?",
                (symbol, start_time_ms, end_time_ms, limit),
            )
        else:
            cur = self.conn.execute(
                "SELECT * FROM order_event "
                "WHERE symbol = ? AND transaction_time_ms >= ? "
                "ORDER BY transaction_time_ms LIMIT ?",
                (symbol, start_time_ms, limit),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_order_lifecycle(self, order_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM order_event WHERE order_id = ? "
            "ORDER BY transaction_time_ms",
            (order_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """Return the latest event row for each order whose final status is non-terminal."""
        cur = self.conn.execute(
            "SELECT e.* FROM order_event e "
            "INNER JOIN ( "
            "  SELECT order_id, MAX(transaction_time_ms) AS max_ts "
            "  FROM order_event WHERE symbol = ? "
            "  GROUP BY order_id "
            ") latest ON e.order_id = latest.order_id "
            "  AND e.transaction_time_ms = latest.max_ts "
            "WHERE e.symbol = ? AND e.order_status NOT IN "
            "('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED') "
            "ORDER BY e.transaction_time_ms",
            (symbol, symbol),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── amendment write ──────────────────────────────────────────────────────

    def insert_amendment(self, row: Dict[str, Any]) -> None:
        self.insert_amendment_rows([row])

    def insert_amendment_rows(self, rows: List[Dict[str, Any]]) -> None:
        if rows:
            self.conn.executemany(
                """
                INSERT INTO order_amendment (
                    amendment_id, order_id, symbol, client_order_id, time_ms,
                    price_before, price_after, qty_before, qty_after,
                    amendment_count
                ) VALUES (
                    :amendment_id, :order_id, :symbol, :client_order_id,
                    :time_ms, :price_before, :price_after, :qty_before,
                    :qty_after, :amendment_count
                )
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
            self.conn.commit()

    # ── amendment read ───────────────────────────────────────────────────────

    def get_amendments_by_order(self, order_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM order_amendment WHERE order_id = ? "
            "ORDER BY time_ms",
            (order_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_latest_amendment_prices(self, symbol: str) -> Dict[int, tuple]:
        """Return {order_id: (price_after, qty_after)} for the latest amendment per order."""
        cur = self.conn.execute(
            "SELECT a.order_id, a.price_after, a.qty_after "
            "FROM order_amendment a "
            "INNER JOIN ("
            "  SELECT order_id, MAX(time_ms) AS max_ts "
            "  FROM order_amendment WHERE symbol = ? "
            "  GROUP BY order_id"
            ") b ON a.order_id = b.order_id AND a.time_ms = b.max_ts "
            "WHERE a.symbol = ?",
            (symbol, symbol),
        )
        return {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    def get_amendments_by_symbol(
        self,
        symbol: str,
        start_time_ms: int = 0,
        end_time_ms: Optional[int] = None,
        limit: int = 50_000,
    ) -> List[Dict[str, Any]]:
        if end_time_ms is not None:
            cur = self.conn.execute(
                "SELECT * FROM order_amendment "
                "WHERE symbol = ? AND time_ms >= ? AND time_ms <= ? "
                "ORDER BY time_ms LIMIT ?",
                (symbol, start_time_ms, end_time_ms, limit),
            )
        else:
            cur = self.conn.execute(
                "SELECT * FROM order_amendment "
                "WHERE symbol = ? AND time_ms >= ? "
                "ORDER BY time_ms LIMIT ?",
                (symbol, start_time_ms, limit),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── metadata ─────────────────────────────────────────────────────────────

    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()
