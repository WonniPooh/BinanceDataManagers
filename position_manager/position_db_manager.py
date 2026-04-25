"""
Position DB Manager — SQLite storage for reconstructed closed positions.

Each symbol gets a positions.db in db_files/<SYMBOL>/.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS position (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    quantity REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    pnl_pct REAL NOT NULL,
    fee_total REAL NOT NULL,
    entry_time_ms INTEGER NOT NULL,
    exit_time_ms INTEGER NOT NULL,
    entry_order_ids TEXT NOT NULL,
    exit_order_ids TEXT NOT NULL,
    duration_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pos_symbol_time ON position(symbol, exit_time_ms);
CREATE INDEX IF NOT EXISTS idx_pos_exit_time ON position(exit_time_ms);
"""


class PositionDB:
    """SQLite store for closed positions."""

    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def insert_position(self, pos: dict) -> int:
        """Insert a closed position record. Returns the row id."""
        cur = self._conn.execute(
            """INSERT INTO position
               (symbol, side, entry_price, exit_price, quantity,
                realized_pnl, pnl_pct, fee_total,
                entry_time_ms, exit_time_ms,
                entry_order_ids, exit_order_ids, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pos["symbol"], pos["side"],
                pos["entry_price"], pos["exit_price"], pos["quantity"],
                pos["realized_pnl"], pos["pnl_pct"], pos["fee_total"],
                pos["entry_time_ms"], pos["exit_time_ms"],
                pos["entry_order_ids"], pos["exit_order_ids"],
                pos["duration_ms"],
            ),
        )
        self._conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def get_positions(
        self,
        symbol: str | None = None,
        since_ms: int | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Fetch closed positions, ordered by exit_time_ms descending."""
        clauses = []
        params: list = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if since_ms is not None:
            clauses.append("exit_time_ms >= ?")
            params.append(since_ms)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT * FROM position {where} ORDER BY exit_time_ms DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def get_last_exit_time(self, symbol: str) -> int | None:
        """Get the latest exit_time_ms for a symbol, or None if no positions."""
        row = self._conn.execute(
            "SELECT MAX(exit_time_ms) as t FROM position WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        return row["t"] if row and row["t"] is not None else None

    def count(self, symbol: str | None = None) -> int:
        if symbol:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM position WHERE symbol = ?", (symbol,)
            ).fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) FROM position").fetchone()
        return row[0]
