"""
SQLite manager for per-symbol user trades.

Stores fills as received from userTrades REST or ORDER_TRADE_UPDATE WS events.
Rows are upserted by trade_id (globally unique per Binance).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS user_trade (
    trade_id          INTEGER PRIMARY KEY,
    order_id          INTEGER NOT NULL,
    symbol            TEXT    NOT NULL,
    side              TEXT    NOT NULL,
    price             REAL    NOT NULL,
    qty               REAL    NOT NULL,
    quote_qty         REAL    NOT NULL DEFAULT 0,
    commission        REAL    NOT NULL DEFAULT 0,
    commission_asset  TEXT    NOT NULL DEFAULT '',
    realized_pnl      REAL    NOT NULL DEFAULT 0,
    is_maker          INTEGER NOT NULL DEFAULT 0,
    is_buyer          INTEGER NOT NULL DEFAULT 0,
    position_side     TEXT    NOT NULL DEFAULT 'BOTH',
    trade_time_ms     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_trade_order_id
    ON user_trade(order_id);
CREATE INDEX IF NOT EXISTS idx_user_trade_symbol_ts
    ON user_trade(symbol, trade_time_ms);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


_TRADE_DEFAULTS: Dict[str, Any] = {
    "quote_qty": 0, "commission": 0, "commission_asset": "",
    "realized_pnl": 0, "is_maker": 0, "is_buyer": 0,
    "position_side": "BOTH",
}


class UserTradeDB:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(_CREATE_SQL)
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
        filled = [{**_TRADE_DEFAULTS, **r} for r in rows]
        self.conn.executemany(
            """
            INSERT INTO user_trade (
                trade_id, order_id, symbol, side, price, qty, quote_qty,
                commission, commission_asset, realized_pnl,
                is_maker, is_buyer, position_side, trade_time_ms
            ) VALUES (
                :trade_id, :order_id, :symbol, :side, :price, :qty, :quote_qty,
                :commission, :commission_asset, :realized_pnl,
                :is_maker, :is_buyer, :position_side, :trade_time_ms
            )
            ON CONFLICT(trade_id) DO UPDATE SET
                realized_pnl     = excluded.realized_pnl,
                commission       = excluded.commission,
                commission_asset = excluded.commission_asset
            """,
            filled,
        )
        self.conn.commit()

    # ── read ─────────────────────────────────────────────────────────────────

    def get_latest_trade_time(self) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(trade_time_ms) FROM user_trade"
        ).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_trades_in_window(
        self, start_ms: int, end_ms: int
    ) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM user_trade "
            "WHERE trade_time_ms >= ? AND trade_time_ms <= ? "
            "ORDER BY trade_time_ms",
            (start_ms, end_ms),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_trades_by_symbol(
        self,
        symbol: str,
        start_time_ms: int = 0,
        end_time_ms: Optional[int] = None,
        limit: int = 10_000,
    ) -> List[Dict[str, Any]]:
        if end_time_ms is not None:
            cur = self.conn.execute(
                "SELECT * FROM user_trade "
                "WHERE symbol = ? AND trade_time_ms >= ? "
                "AND trade_time_ms <= ? "
                "ORDER BY trade_time_ms LIMIT ?",
                (symbol, start_time_ms, end_time_ms, limit),
            )
        else:
            cur = self.conn.execute(
                "SELECT * FROM user_trade "
                "WHERE symbol = ? AND trade_time_ms >= ? "
                "ORDER BY trade_time_ms LIMIT ?",
                (symbol, start_time_ms, limit),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_trades_by_order(self, order_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM user_trade WHERE order_id = ? "
            "ORDER BY trade_time_ms",
            (order_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_total_pnl(
        self,
        symbol: str,
        start_time_ms: int = 0,
        end_time_ms: Optional[int] = None,
    ) -> float:
        if end_time_ms is not None:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) FROM user_trade "
                "WHERE symbol = ? AND trade_time_ms >= ? "
                "AND trade_time_ms <= ?",
                (symbol, start_time_ms, end_time_ms),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) FROM user_trade "
                "WHERE symbol = ? AND trade_time_ms >= ?",
                (symbol, start_time_ms),
            ).fetchone()
        return float(row[0]) if row else 0.0

    def get_total_commission(
        self,
        symbol: str,
        start_time_ms: int = 0,
    ) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(commission), 0) FROM user_trade "
            "WHERE symbol = ? AND trade_time_ms >= ?",
            (symbol, start_time_ms),
        ).fetchone()
        return float(row[0]) if row else 0.0

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
