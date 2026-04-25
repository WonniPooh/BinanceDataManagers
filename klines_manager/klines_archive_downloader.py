"""
Binance Futures archived kline downloader.

Downloads historical klines from Binance's public S3 bucket (monthly + daily
ZIP files) and inserts them into per-symbol SQLite databases via CandleDB.

Progress reporting:
    Pass an ``asyncio.Queue`` as ``status_queue`` to ``process_symbol`` or
    ``download_klines`` to receive per-symbol progress dicts::

        {"symbol": "BTCUSDT", "phase": "download", "pct": 45.0, "detail": "12/27 files"}
        {"symbol": "BTCUSDT", "phase": "insert",   "pct": 80.0, "detail": "20/25 files"}
        {"symbol": "BTCUSDT", "phase": "done",      "pct": 100.0}
        {"symbol": "BTCUSDT", "phase": "error",     "pct": 0.0,  "detail": "..."}

Logging:
    Uses ``logging.getLogger("klines_archived_downloader")``.
    When run as ``__main__`` a console handler is attached automatically so
    output looks the same as the old ``print()`` behaviour.
"""
from __future__ import annotations

import asyncio
import aiohttp
import csv
import io
import logging
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, Any, Iterable, List, Optional, Sequence

import xml.etree.ElementTree as ET

from .klines_db_manager import CandleDB

logger = logging.getLogger("klines_archived_downloader")

# ── Config ────────────────────────────────────────────────────────────────────

INTERVAL = "1m"
UTC = timezone.utc

S3_LIST_BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DATA_BASE    = "https://data.binance.vision"

UM_DAILY_PREFIX_KL   = "data/futures/um/daily/klines"
UM_MONTHLY_PREFIX_KL = "data/futures/um/monthly/klines"


# ── Date helpers ──────────────────────────────────────────────────────────────

def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)


def _next_month(dt: datetime) -> datetime:
    y, m = dt.year, dt.month
    return datetime(y + (m // 12), (m % 12) + 1, 1, tzinfo=UTC)


def _iter_days(start: datetime, end_incl: datetime):
    d = start
    while d <= end_incl:
        yield d
        d += timedelta(days=1)


def _iter_months(start: datetime, end_incl: datetime):
    m = _month_start(start)
    endm = _month_start(end_incl)
    while m <= endm:
        yield m
        m = _next_month(m)


# ── S3 listing ────────────────────────────────────────────────────────────────

async def _s3_list(prefix: str, session: aiohttp.ClientSession) -> dict:
    """List S3 objects under *prefix*.  Returns ``{"prefixes": [...], "keys": [...]}``.

    Handles v1 (Marker) and v2 (ContinuationToken) pagination.
    """
    prefixes: List[str] = []
    keys: List[str] = []

    params = {"delimiter": "/", "prefix": prefix}
    marker: Optional[str] = None
    continuation: Optional[str] = None

    while True:
        q = params.copy()
        if marker:
            q["marker"] = marker
        if continuation:
            q["continuation-token"] = continuation

        async with session.get(
            S3_LIST_BASE, params=q,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            resp.raise_for_status()
            xml_text = await resp.text()

        root = ET.fromstring(xml_text)
        m = re.match(r"\{(.*)\}", root.tag)
        ns = m.group(1) if m else "http://s3.amazonaws.com/doc/2006-03-01/"

        def tag(name: str) -> str:
            return f"{{{ns}}}{name}"

        for cp in root.findall(tag("CommonPrefixes")):
            pfx = cp.find(tag("Prefix"))
            if pfx is not None and pfx.text:
                prefixes.append(pfx.text)

        for ct in root.findall(tag("Contents")):
            k = ct.find(tag("Key"))
            if k is not None and k.text:
                keys.append(k.text)

        is_trunc_el = root.find(tag("IsTruncated"))
        trunc = is_trunc_el is not None and is_trunc_el.text == "true"

        next_marker = root.find(tag("NextMarker"))
        next_cont   = root.find(tag("NextContinuationToken"))

        if trunc:
            marker       = next_marker.text if next_marker is not None else None
            continuation = next_cont.text   if next_cont   is not None else None
            if not marker and not continuation:
                break
        else:
            break

    return {"prefixes": prefixes, "keys": keys}


# ── Work plan ─────────────────────────────────────────────────────────────────

@dataclass
class WorkPlan:
    monthly_keys: List[str]
    daily_keys:   List[str]

    @property
    def total(self) -> int:
        return len(self.monthly_keys) + len(self.daily_keys)

    @property
    def all_keys(self) -> List[str]:
        return self.monthly_keys + self.daily_keys


def _plan_for_symbol(
    symbol: str, start_dt: datetime, end_dt: datetime,
    available_monthly: Sequence[str], available_daily: Sequence[str],
) -> WorkPlan:
    avail_m = set(available_monthly)
    avail_d = set(available_daily)
    monthly: List[str] = []
    daily:   List[str] = []

    current_month = _month_start(datetime.now(tz=UTC))

    for m in _iter_months(start_dt, end_dt):
        if m < current_month:
            key = (f"{UM_MONTHLY_PREFIX_KL}/{symbol}/{INTERVAL}/"
                   f"{symbol}-{INTERVAL}-{m.strftime('%Y-%m')}.zip")
            if key in avail_m:
                monthly.append(key)

    covered_months = {re.search(r"(\d{4}-\d{2})", k).group(1) for k in monthly}
    for d in _iter_days(start_dt, end_dt):
        ym = d.strftime("%Y-%m")
        if ym in covered_months:
            continue
        k = (f"{UM_DAILY_PREFIX_KL}/{symbol}/{INTERVAL}/"
             f"{symbol}-{INTERVAL}-{d.strftime('%Y-%m-%d')}.zip")
        if k in avail_d:
            daily.append(k)

    return WorkPlan(monthly, daily)


# ── CSV / ZIP parsing ────────────────────────────────────────────────────────

def _parse_kline_csv_bytes(raw: bytes) -> Iterable[Dict[str, Any]]:
    """Parse Binance kline CSV bytes (with or without header) into row dicts."""
    text = raw.decode("utf-8")
    sio = io.StringIO(text)

    peek = sio.read(256)
    sio.seek(0)
    has_header = "open_time" in peek

    if has_header:
        for row in csv.DictReader(sio):
            try:
                yield {
                    "open_time":               int(row["open_time"]),
                    "open":                    float(row["open"]),
                    "high":                    float(row["high"]),
                    "low":                     float(row["low"]),
                    "close":                   float(row["close"]),
                    "volume":                  float(row["volume"]),
                    "quote_volume":            float(row["quote_volume"]),
                    "count":                   int(row["count"]),
                    "taker_buy_volume":        float(row["taker_buy_volume"]),
                    "taker_buy_quote_volume":  float(row["taker_buy_quote_volume"]),
                }
            except Exception:
                logger.warning("Bad kline row (header): %s", row, exc_info=True)
        return

    for row in csv.reader(sio):
        if not row or len(row) < 12:
            continue
        try:
            yield {
                "open_time":               int(row[0]),
                "open":                    float(row[1]),
                "high":                    float(row[2]),
                "low":                     float(row[3]),
                "close":                   float(row[4]),
                "volume":                  float(row[5]),
                "quote_volume":            float(row[7]),
                "count":                   int(row[8]),
                "taker_buy_volume":        float(row[9]),
                "taker_buy_quote_volume":  float(row[10]),
            }
        except Exception:
            logger.warning("Bad kline row (no header): %s", row, exc_info=True)


def _extract_csv_from_zip(zbytes: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                with zf.open(name) as f:
                    return f.read()
    raise RuntimeError("No CSV found in ZIP")


# ── HTTP fetch with retries ──────────────────────────────────────────────────

async def _fetch_url(
    session: aiohttp.ClientSession, url: str, io_sem: asyncio.Semaphore,
) -> Optional[bytes]:
    for attempt in range(4):
        try:
            async with io_sem:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=600),
                ) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    return await resp.read()
        except Exception:
            if attempt == 3:
                raise
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


# ── DB path helper ────────────────────────────────────────────────────────────

def db_path_for_symbol(root: str, symbol: str) -> Path:
    d = Path(root) / symbol
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{symbol}_{INTERVAL}.db"


# ── process_symbol sub‑steps ──────────────────────────────────────────────────

def _ingest_csv_files(db: CandleDB, files: List[Path], symbol: str) -> int:
    """Insert sorted CSV files into DB, delete each after success. Returns total rows."""
    total = 0
    for fp in sorted(files):
        try:
            raw = fp.read_bytes()
            rows = _parse_kline_csv_bytes(raw)
            inserted = db.insert_rows(rows)
            total += inserted
            logger.info("[%s %s] Inserted %s: %d rows", symbol, INTERVAL, fp.name, inserted)
            fp.unlink()
        except Exception:
            logger.error("[%s %s] Failed to process %s", symbol, INTERVAL, fp, exc_info=True)
    return total


def _resolve_start_time(db_path: Path, start_dt: datetime, symbol: str) -> tuple[datetime, bool]:
    """Decide effective start and whether the DB must be re-created.

    Returns ``(effective_start_dt, should_drop_db)``.
    """
    if not db_path.exists():
        return start_dt, False

    db = CandleDB(str(db_path))
    try:
        result = db.conn.execute(
            "SELECT MIN(open_time_ms), MAX(open_time_ms) FROM candle;"
        ).fetchone()
        min_ts, max_ts = result if result else (None, None)
    finally:
        db.close()

    if min_ts is None or max_ts is None:
        return start_dt, False

    existing_start = datetime.fromtimestamp((min_ts - 60_000) / 1000.0, tz=UTC)
    existing_end   = datetime.fromtimestamp(max_ts / 1000.0, tz=UTC)

    logger.info("[%s %s] Existing data: %s to %s",
                symbol, INTERVAL,
                existing_start.strftime("%Y-%m-%d"),
                existing_end.strftime("%Y-%m-%d"))

    if start_dt < existing_start:
        logger.info("[%s %s] Requested start %s before existing %s — downloading missing range",
                    symbol, INTERVAL,
                    start_dt.strftime("%Y-%m-%d"),
                    existing_start.strftime("%Y-%m-%d"))
        return start_dt, False

    resume_dt = existing_end + timedelta(milliseconds=1)
    if resume_dt > start_dt:
        logger.info("[%s %s] Resuming from %s", symbol, INTERVAL, resume_dt.strftime("%Y-%m-%d"))
        return resume_dt, False

    return start_dt, False


async def _download_files(
    session: aiohttp.ClientSession,
    keys: List[str],
    temp_dir: Path,
    io_sem: asyncio.Semaphore,
    concurrency: int,
    symbol: str,
    on_progress: Callable[[int, int], None] | None = None,
) -> List[Path]:
    """Download + extract ZIPs to *temp_dir*.  Returns list of extracted CSV paths."""
    sem = asyncio.Semaphore(concurrency)
    total = len(keys)
    done_count = 0
    results: List[Path] = []

    async def _one(key: str) -> Optional[Path]:
        nonlocal done_count
        try:
            async with sem:
                zbytes = await _fetch_url(session, f"{DATA_BASE}/{key}", io_sem)
            if not zbytes:
                return None
            csv_bytes = _extract_csv_from_zip(zbytes)
            date_match = re.search(r'(\d{4}-\d{2}(?:-\d{2})?)', key)
            date_str = date_match.group(1) if date_match else "chunk"
            fp = temp_dir / f"{date_str}.csv"
            fp.write_bytes(csv_bytes)
            return fp
        except Exception:
            logger.error("[%s %s] Download failed: %s", symbol, INTERVAL, key, exc_info=True)
            return None

    tasks = [asyncio.create_task(_one(k)) for k in keys]
    for future in asyncio.as_completed(tasks):
        fp = await future
        done_count += 1
        if fp is not None:
            results.append(fp)
        if on_progress:
            on_progress(done_count, total)
        logger.debug("[%s %s] Downloaded %d/%d", symbol, INTERVAL, done_count, total)

    return results


# ── Main per-symbol entry point ───────────────────────────────────────────────

async def process_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    start_dt: datetime,
    db_root: str | Path,
    io_sem: asyncio.Semaphore,
    per_asset_conc: int = 10,
    status_queue: asyncio.Queue | None = None,
) -> None:
    """Download and ingest archived klines for a single symbol.

    Reports per-symbol progress (0–100 %) to *status_queue* if provided.
    """
    symbol = symbol.upper()
    db_path  = db_path_for_symbol(db_root, symbol)
    temp_dir = Path(db_root) / symbol / f"temp_{INTERVAL}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db: CandleDB | None = None

    def _emit(phase: str, pct: float, detail: str = "") -> None:
        if status_queue is None:
            return
        msg: Dict[str, Any] = {"symbol": symbol, "phase": phase, "pct": round(pct, 1)}
        if detail:
            msg["detail"] = detail
        status_queue.put_nowait(msg)

    try:
        # 1. Resume any leftover temp CSVs from a previous interrupted run
        leftover = sorted(temp_dir.glob("*.csv"))
        if leftover:
            logger.info("[%s %s] Resuming %d leftover temp files", symbol, INTERVAL, len(leftover))
            db = CandleDB(str(db_path))
            _ingest_csv_files(db, leftover, symbol)
            db.close()
            db = None

        # 2. Resolve start time / drop-and-redownload decision
        # 2. Resolve start time: advance past already-downloaded days.
        start_dt, _ = _resolve_start_time(db_path, start_dt, symbol)

        end_dt = datetime.now(tz=UTC)

        # 3. Build work plan
        monthly_list = await _s3_list(f"{UM_MONTHLY_PREFIX_KL}/{symbol}/{INTERVAL}/", session)
        daily_list   = await _s3_list(f"{UM_DAILY_PREFIX_KL}/{symbol}/{INTERVAL}/", session)

        monthly_zips = [k for k in monthly_list["keys"] if k.endswith(".zip")]
        daily_zips   = [k for k in daily_list["keys"]   if k.endswith(".zip")]

        plan = _plan_for_symbol(symbol, start_dt, end_dt, monthly_zips, daily_zips)
        logger.info("[%s %s] Plan: %d monthly + %d daily files",
                    symbol, INTERVAL, len(plan.monthly_keys), len(plan.daily_keys))

        if plan.total == 0:
            logger.info("[%s %s] Already up to date", symbol, INTERVAL)
            _emit("done", 100.0)
            return

        # 4. Download phase (0–50 %)
        def _dl_progress(done: int, total: int) -> None:
            _emit("download", done / total * 50.0, f"{done}/{total} files")

        extracted = await _download_files(
            session, plan.all_keys, temp_dir, io_sem, per_asset_conc,
            symbol, on_progress=_dl_progress,
        )
        logger.info("[%s %s] Downloaded %d files", symbol, INTERVAL, len(extracted))

        # 5. Insert phase (50–100 %)
        _emit("insert", 50.0, f"0/{len(extracted)} files")
        db = CandleDB(str(db_path))
        sorted_files = sorted(extracted)
        total_inserted = 0

        for i, fp in enumerate(sorted_files, 1):
            try:
                raw = fp.read_bytes()
                rows = _parse_kline_csv_bytes(raw)
                total_inserted += db.insert_rows(rows)
                fp.unlink()
            except Exception:
                logger.error("[%s %s] Insert failed: %s", symbol, INTERVAL, fp, exc_info=True)
            _emit("insert", 50.0 + i / len(sorted_files) * 50.0, f"{i}/{len(sorted_files)} files")

        # 6. Cleanup
        try:
            temp_dir.rmdir()
        except OSError:
            pass

        logger.info("[%s %s] Done — %d files, %d rows inserted",
                    symbol, INTERVAL, len(extracted), total_inserted)
        _emit("done", 100.0)

    except Exception:
        logger.error("[%s %s] process_symbol failed", symbol, INTERVAL, exc_info=True)
        _emit("error", 0.0, "process_symbol failed")
    finally:
        if db is not None:
            db.close()


# ── Multi-symbol entry point ─────────────────────────────────────────────────

async def download_klines(
    config: Dict[str, Any],
    status_queue: asyncio.Queue | None = None,
) -> None:
    """Download archived klines for multiple symbols.

    Args:
        config: Dict with keys:
            symbols            — ``{"BTCUSDT": "2025-01-01", ...}``
            db_root            — root dir for DBs (default ``"../db_files"``)
            files_concurrency  — concurrent downloads per symbol (default 10)
            io_semaphore_limit — global I/O semaphore size (default 16)
            connector_limit    — aiohttp connector limit (default 64)
            timeout_seconds    — session-level timeout (default 900)
        status_queue: optional ``asyncio.Queue`` for progress dicts.
    """
    symbols_dict    = config.get("symbols", {})
    db_root         = config.get("db_root", "../db_files")
    concurrency     = config.get("files_concurrency", 10)
    io_sem_limit    = config.get("io_semaphore_limit", 16)
    connector_limit = config.get("connector_limit", 64)
    timeout_seconds = config.get("timeout_seconds", 900)

    if not symbols_dict:
        logger.warning("No symbols provided in config")
        return

    pairs: List[tuple[str, datetime]] = []
    for sym, date_str in symbols_dict.items():
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
            pairs.append((sym.upper(), dt))
        except ValueError:
            logger.warning("Invalid date '%s' for %s — skipping", date_str, sym)

    if not pairs:
        logger.warning("No valid symbols after parsing")
        return

    logger.info("Processing %d symbols", len(pairs))

    conn    = aiohttp.TCPConnector(limit=connector_limit, ssl=False)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    io_sem  = asyncio.Semaphore(io_sem_limit)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        for sym, start in pairs:
            logger.info("Starting %s @ %s from %s", sym, INTERVAL, start.strftime("%Y-%m-%d"))
            try:
                await process_symbol(
                    session, sym, start, db_root, io_sem,
                    per_asset_conc=concurrency, status_queue=status_queue,
                )
            except Exception:
                logger.error("Symbol %s failed", sym, exc_info=True)


# ── Standalone CLI ────────────────────────────────────────────────────────────

def _parse_symbols_file(path: str) -> Dict[str, str]:
    """Parse ``symbols.txt`` — one ``SYMBOL YYYY-MM-DD`` per line."""
    result: Dict[str, str] = {}
    if not os.path.exists(path):
        logger.warning("Symbols file '%s' not found", path)
        return result

    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                logger.warning("Line %d invalid: '%s'", lineno, line)
                continue
            sym, date_str = parts[0].upper(), parts[1]
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                result[sym] = date_str
            except ValueError:
                logger.warning("Line %d bad date '%s' for %s", lineno, date_str, sym)
    return result


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    symbols = _parse_symbols_file("symbols.txt")
    if not symbols:
        logger.error("No valid symbols found in symbols.txt")
        return

    config = {
        "symbols": symbols,
        "db_root": "../db_files",
        "files_concurrency": 10,
        "io_semaphore_limit": 16,
        "connector_limit": 64,
        "timeout_seconds": 900,
    }
    asyncio.run(download_klines(config))


if __name__ == "__main__":
    main()
