# ohlcv_db.py
from __future__ import annotations
import csv
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, Union, TextIO

# (open_time_ms, open, high, low, close, volume, quote_volume, trades_count, taker_buy_volume, taker_buy_quote_volume)
CandleRow = Union[
    Tuple[int, float, float, float, float, float, float, int, float, float],
    dict,
]

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 268435456;  -- 256 MiB

CREATE TABLE IF NOT EXISTS candle (
  open_time_ms              INTEGER PRIMARY KEY,  -- ms since epoch
  open                      REAL    NOT NULL,
  high                      REAL    NOT NULL,
  low                       REAL    NOT NULL,
  close                     REAL    NOT NULL,
  volume                    REAL    NOT NULL,
  quote_volume              REAL    NOT NULL,
  trades_count              INTEGER NOT NULL,
  taker_buy_volume          REAL    NOT NULL,
  taker_buy_quote_volume    REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

UPSERT = """
INSERT INTO candle(
  open_time_ms, open, high, low, close,
  volume, quote_volume, trades_count,
  taker_buy_volume, taker_buy_quote_volume
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(open_time_ms) DO NOTHING;
"""

class CandleDB:
    def __init__(self, db_path: str):
        self.path = Path(db_path)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(DDL)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    @staticmethod
    def _normalize_ms(ts: int) -> int:
        """Accept ms or µs; return ms."""
        if ts >= 100_000_000_000_000:  # >= 1e14 → definitely µs
            return ts // 1000
        return ts

    @staticmethod
    def _coerce_row(row: CandleRow
                    ) -> Tuple[int, float, float, float, float, float, float, int, float, float]:
        if isinstance(row, dict):
            ot   = int(row["open_time"])
            o    = float(row["open"]); h = float(row["high"])
            l    = float(row["low"]);  c = float(row["close"])
            v    = float(row["volume"])
            qv   = float(row["quote_volume"])
            cnt  = int(row.get("count") or row.get("trades_count") or 0)
            tbv  = float(row["taker_buy_volume"])
            tbqv = float(row["taker_buy_quote_volume"])
        else:
            (ot, o, h, l, c, v, qv, cnt, tbv, tbqv) = row
            ot = int(ot); o = float(o); h = float(h); l = float(l); c = float(c)
            v = float(v); qv = float(qv); cnt = int(cnt); tbv = float(tbv); tbqv = float(tbqv)

        ot_ms = CandleDB._normalize_ms(ot)
        return (ot_ms, o, h, l, c, v, qv, cnt, tbv, tbqv)

    def insert_rows(self, rows: Iterable[CandleRow], batch_size: int = 50_000) -> int:
        """Insert many rows idempotently. Returns number of *attempted* inserts."""
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
        return count

    def insert_one(self, row: CandleRow) -> None:
        with self.conn:
            self.conn.execute(UPSERT, self._coerce_row(row))

    # --- CSV helpers ---------------------------------------------------------
    @staticmethod
    def parse_binance_kline_csv(fp: TextIO) -> Iterable[CandleRow]:
        """
        Expects header:
        open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
        Ignores 'close_time' and final 'ignore'.
        """
        reader = csv.DictReader(fp)
        for row in reader:
            yield {
                "open_time": int(row["open_time"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                # close_time -> ignored
                "quote_volume": float(row["quote_volume"]),
                "count": int(row["count"]),
                "taker_buy_volume": float(row["taker_buy_volume"]),
                "taker_buy_quote_volume": float(row["taker_buy_quote_volume"]),
                # ignore -> ignored
            }

    def load_csv_file(self, csv_path: str | Path, batch_size: int = 50_000) -> int:
        with open(csv_path, "r", newline="") as fp:
            return self.insert_rows(self.parse_binance_kline_csv(fp), batch_size=batch_size)

    # Meta (optional)
    def set_meta(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                (key, value),
            )

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?;", (key,)).fetchone()
        return row[0] if row else default
