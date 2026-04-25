# aggtrade_db.py
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, Union

AggRow = Union[
    Tuple[int, int, float, float, int, int],  # (agg_trade_id, trade_ts, price, qty, is_buyer_maker, trades_num)
    dict,                                     # keys: agg_trade_id, trade_ts, price, qty, is_buyer_maker, trades_num
]

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
-- Adjust if you like; improves read-heavy scans.
PRAGMA mmap_size = 268435456;  -- 256 MiB

CREATE TABLE IF NOT EXISTS agg_trade (
  agg_trade_id    INTEGER PRIMARY KEY,  -- Binance aggTradeId (per symbol DB)
  trade_ts_ms     INTEGER NOT NULL,     -- milliseconds since epoch
  price           REAL    NOT NULL,
  qty             REAL    NOT NULL,
  is_buyer_maker  INTEGER NOT NULL,     -- 0 or 1
  trades_num      INTEGER NOT NULL      -- >= 1
);

CREATE INDEX IF NOT EXISTS idx_time ON agg_trade(trade_ts_ms);
CREATE INDEX IF NOT EXISTS idx_side_time ON agg_trade(is_buyer_maker, trade_ts_ms);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Tracks in-flight and interrupted backwards REST fills.
-- Unfilled range for a row = [gap_start_ms, frontier_ms).
-- Rows are deleted on successful completion; they persist on cancellation.
CREATE TABLE IF NOT EXISTS rest_gap (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  gap_start_ms  INTEGER NOT NULL,  -- absolute oldest timestamp desired
  frontier_ms   INTEGER NOT NULL   -- oldest ts reached by REST so far
);
"""

UPSERT = """
INSERT INTO agg_trade(agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(agg_trade_id) DO NOTHING;
"""

class AggTradeDB:
    def __init__(self, db_path: str):
        self.path = Path(db_path)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.last_present_timestamp = None  # type: int | None
        self._init_schema()
        result = self.conn.execute(
            "SELECT MAX(trade_ts_ms) FROM agg_trade;"
        ).fetchone()
        self.last_present_timestamp = result[0] if result and result[0] is not None else None

    def _init_schema(self):
        self.conn.executescript(DDL)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    @staticmethod
    def _normalize_ms(ts: int) -> int:
        """
        Accept ms or µs; return ms.
        13-digit => ms; 16-digit => µs -> ms.
        """
        # Fast magnitude check without string casts
        if ts >= 10_000_000_000_000:   # >= 1e13 → could be µs from 2025+ dumps
            # If it's already ms but very large future time, //1000 still works
            # but we only do it for clearly 14–16-digit ranges.
            if ts >= 100_000_000_000_000:  # >= 1e14 → definitely µs
                return ts // 1000
            # Ambiguous 1e13–1e14 range—treat as ms (2024 data & earlier).
        return ts

    @staticmethod
    def _coerce_row(row: AggRow) -> Tuple[int, int, float, float, int, int]:
        if isinstance(row, dict):
            agg_id = int(row["agg_trade_id"])
            ts     = int(row["trade_ts"])
            price  = float(row["price"])
            qty    = float(row["qty"])
            mflag  = 1 if row["is_buyer_maker"] else 0
            tnum   = int(row.get("trades_num", 1))
        else:
            agg_id, ts, price, qty, mflag, tnum = row
            agg_id = int(agg_id); ts = int(ts)
            price = float(price); qty = float(qty)
            mflag = int(mflag); tnum = int(tnum)

        # Guardrails
        ts_ms = AggTradeDB._normalize_ms(ts)
        if tnum < 1:
            tnum = 1
        if mflag not in (0, 1):
            mflag = 1 if mflag else 0

        return (agg_id, ts_ms, price, qty, mflag, tnum)

    def insert_rows(self, rows: Iterable[AggRow], batch_size: int = 10_000) -> int:
        """
        Insert many rows idempotently. Returns number of *attempted* inserts.
        (Duplicates are ignored via ON CONFLICT.)
        """
        cur = self.conn.cursor()
        count = 0
        buf = []
        for r in rows:
            buf.append(self._coerce_row(r))
            count += 1
            if len(buf) >= batch_size:
                with self.conn:
                    cur.executemany(UPSERT, buf)
                buf.clear()
        if buf:
            with self.conn:
                cur.executemany(UPSERT, buf)
        # Update cached timestamp so long-lived instances stay current.
        if count > 0:
            result = self.conn.execute(
                "SELECT MAX(trade_ts_ms) FROM agg_trade;"
            ).fetchone()
            self.last_present_timestamp = result[0] if result and result[0] is not None else self.last_present_timestamp
        return count

    def insert_one(self, row: AggRow) -> None:
        with self.conn:
            self.conn.execute(UPSERT, self._coerce_row(row))

    # Handy extras (optional)
    def set_meta(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                (key, value),
            )

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?;", (key,)).fetchone()
        return row[0] if row else default

    # ── REST gap tracking ─────────────────────────────────────────────────────

    def open_gap(self, gap_start_ms: int, frontier_ms: int) -> int:
        """Dedup-insert a new gap row. Returns the new row id.

        Deletes any existing row with the same ``gap_start_ms`` so a repeated
        call for the same window supersedes the previous run.
        """
        with self.conn:
            self.conn.execute(
                "DELETE FROM rest_gap WHERE gap_start_ms = ?;", (gap_start_ms,)
            )
            cur = self.conn.execute(
                "INSERT INTO rest_gap(gap_start_ms, frontier_ms) VALUES(?, ?);",
                (gap_start_ms, frontier_ms),
            )
        return cur.lastrowid

    def update_gap(self, gap_id: int, frontier_ms: int) -> None:
        """Advance the frontier of an in-flight gap row."""
        with self.conn:
            self.conn.execute(
                "UPDATE rest_gap SET frontier_ms = ? WHERE id = ?;",
                (frontier_ms, gap_id),
            )

    def close_gap(self, gap_id: int) -> None:
        """Delete a gap row on successful fill completion."""
        with self.conn:
            self.conn.execute("DELETE FROM rest_gap WHERE id = ?;", (gap_id,))

    def list_gaps(self) -> list[tuple[int, int, int]]:
        """Return all gap rows as (id, gap_start_ms, frontier_ms) ascending."""
        rows = self.conn.execute(
            "SELECT id, gap_start_ms, frontier_ms FROM rest_gap ORDER BY gap_start_ms ASC;"
        ).fetchall()
        return [(int(r[0]), int(r[1]), int(r[2])) for r in rows]

    # ── Query helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row: tuple) -> dict:
        """Convert a raw SQLite row tuple to a keyed dict."""
        agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num = row
        return {
            "agg_trade_id":   int(agg_trade_id),
            "trade_ts_ms":    int(trade_ts_ms),
            "price":          float(price),
            "qty":            float(qty),
            "is_buyer_maker": bool(is_buyer_maker),
            "trades_num":     int(trades_num),
        }

    def get_trades_before(
        self,
        end_time_ms: int,
        limit: int = 1000,
    ) -> list[dict]:
        """Return up to `limit` trades with trade_ts_ms < end_time_ms, sorted ascending."""
        rows = self.conn.execute(
            """
            SELECT agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num
            FROM agg_trade
            WHERE trade_ts_ms < ?
            ORDER BY trade_ts_ms DESC, agg_trade_id DESC
            LIMIT ?
            """,
            (end_time_ms, limit),
        ).fetchall()
        # Reverse so result is ascending by time
        return [self._row_to_dict(r) for r in reversed(rows)]

    def get_trades_in_range(
        self,
        from_ms: int,
        to_ms: int,
        limit: int = 1000,
    ) -> list[dict]:
        """Return up to `limit` trades in [from_ms, to_ms), sorted ascending by time."""
        rows = self.conn.execute(
            """
            SELECT agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num
            FROM agg_trade
            WHERE trade_ts_ms >= ? AND trade_ts_ms < ?
            ORDER BY trade_ts_ms ASC, agg_trade_id ASC
            LIMIT ?
            """,
            (from_ms, to_ms, limit),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_trades_in_range_desc(
        self,
        from_ms: int,
        to_ms: int,
        limit: int = 1000,
    ) -> list[dict]:
        """Return up to `limit` trades in (from_ms, to_ms] newest-first, sorted descending."""
        rows = self.conn.execute(
            """
            SELECT agg_trade_id, trade_ts_ms, price, qty, is_buyer_maker, trades_num
            FROM agg_trade
            WHERE trade_ts_ms > ? AND trade_ts_ms <= ?
            ORDER BY trade_ts_ms DESC, agg_trade_id DESC
            LIMIT ?
            """,
            (from_ms, to_ms, limit),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]
