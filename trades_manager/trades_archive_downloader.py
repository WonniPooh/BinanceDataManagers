"""
Binance Futures archived aggTrade downloader.

Downloads historical aggTrades from Binance's public S3 bucket (monthly +
daily ZIP files) and inserts them into per-symbol SQLite databases via
AggTradeDB.

Progress reporting:
    Pass an ``asyncio.Queue`` as ``status_queue`` to ``process_symbol`` or
    ``download_trades`` to receive per-symbol progress dicts::

        {"symbol": "BTCUSDT", "source": "archive", "phase": "download", "pct": 45.0, "detail": "12/27 files"}
        {"symbol": "BTCUSDT", "source": "archive", "phase": "insert",   "pct": 80.0, "detail": "20/25 files"}
        {"symbol": "BTCUSDT", "source": "archive", "phase": "done",      "pct": 100.0}
        {"symbol": "BTCUSDT", "source": "archive", "phase": "error",     "pct": 0.0,  "detail": "..."}

Logging:
    Uses ``logging.getLogger("trades_archive_downloader")``.
"""
from __future__ import annotations

import asyncio
import aiohttp
import csv
import io
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import xml.etree.ElementTree as ET

from .trades_db_manager import AggTradeDB

logger = logging.getLogger("trades_archive_downloader")

# ── Config ────────────────────────────────────────────────────────────────────

S3_LIST_BASE = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DATA_BASE    = "https://data.binance.vision"

UM_DAILY_PREFIX   = "data/futures/um/daily/aggTrades"
UM_MONTHLY_PREFIX = "data/futures/um/monthly/aggTrades"
UTC = timezone.utc


# ── Date helpers ──────────────────────────────────────────────────────────────

def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)


def _next_month(dt: datetime) -> datetime:
    y, m = dt.year, dt.month
    return datetime(y + (m // 12), (m % 12) + 1, 1, tzinfo=UTC)


def _iter_days(start: datetime, end_incl: datetime):
    d = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end_incl.replace(hour=0, minute=0, second=0, microsecond=0)
    while d <= end:
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
    range_days = (end_dt - start_dt).days + 1  # inclusive

    # Only use monthly archives for ranges >= 8 days.
    # Short ranges use daily files to avoid downloading entire months.
    if range_days >= 8:
        for m in _iter_months(start_dt, end_dt):
            if m < current_month:
                key = f"{UM_MONTHLY_PREFIX}/{symbol}/{symbol}-aggTrades-{m.strftime('%Y-%m')}.zip"
                if key in avail_m:
                    monthly.append(key)

    covered_months = {re.search(r"(\d{4}-\d{2})", k).group(1) for k in monthly}
    for d in _iter_days(start_dt, end_dt):
        ym = d.strftime("%Y-%m")
        if ym in covered_months:
            continue
        k = f"{UM_DAILY_PREFIX}/{symbol}/{symbol}-aggTrades-{d.strftime('%Y-%m-%d')}.zip"
        if k in avail_d:
            daily.append(k)

    return WorkPlan(monthly, daily)


# ── CSV / ZIP parsing ────────────────────────────────────────────────────────

def _parse_agg_csv_bytes(raw: bytes) -> Iterable[tuple]:
    """Parse Binance aggTrade CSV bytes into row tuples for AggTradeDB."""
    text = raw.decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)  # skip header if present
    for row in reader:
        if not row or len(row) < 7:
            continue
        try:
            a  = int(row[0])         # aggTradeId
            p  = float(row[1])       # price
            q  = float(row[2])       # qty
            f  = int(row[3])         # firstTradeId
            l  = int(row[4])         # lastTradeId
            ts = int(row[5])         # trade time (ms)
            m  = row[6].strip().lower() in ("true", "1")
            trades_num = (l - f + 1) if l >= f else 1
            yield (a, ts, p, q, 1 if m else 0, trades_num)
        except Exception:
            logger.warning("Skipping bad CSV row: %s", row, exc_info=True)


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

def db_path_for_symbol(root: str | Path, symbol: str, db_filename: str = "trades.db") -> Path:
    d = Path(root) / symbol
    d.mkdir(parents=True, exist_ok=True)
    return d / db_filename


# ── process_symbol sub-steps ─────────────────────────────────────────────────

def _ingest_csv_files(db: AggTradeDB, files: List[Path], symbol: str) -> int:
    """Insert sorted CSV files into DB, delete each after success. Returns total rows."""
    total = 0
    for fp in sorted(files):
        try:
            raw = fp.read_bytes()
            rows = _parse_agg_csv_bytes(raw)
            inserted = db.insert_rows(rows)
            total += inserted
            logger.info("[%s] Inserted %s: %d rows", symbol, fp.name, inserted)
            fp.unlink()
        except Exception:
            logger.error("[%s] Failed to process %s", symbol, fp, exc_info=True)
    return total


def _resolve_start_time(
    db_path: Path, start_dt: datetime, symbol: str,
    end_dt: datetime | None = None,
) -> tuple[datetime, bool]:
    """Decide effective start and whether the DB must be re-created.

    Returns ``(effective_start_dt, should_drop_db)``.

    ``end_dt`` is the archive window upper bound.  WS live data written
    beyond this boundary into the same DB must not be used to determine
    the resume point — otherwise the archive phase would incorrectly
    skip days that are still missing (the gap between archived history
    and the live stream).
    """
    if not db_path.exists():
        return start_dt, False

    db = AggTradeDB(str(db_path))
    try:
        # Cap the max lookup to end_dt so WS live data written after the
        # archive boundary doesn't mask unfilled historical gaps.
        if end_dt is not None:
            cap_ms = int(end_dt.timestamp() * 1000) + 24 * 3_600_000  # +1 day buffer
            result = db.conn.execute(
                "SELECT MIN(trade_ts_ms), MAX(trade_ts_ms) FROM agg_trade "
                "WHERE trade_ts_ms <= ?;",
                (cap_ms,),
            ).fetchone()
        else:
            result = db.conn.execute(
                "SELECT MIN(trade_ts_ms), MAX(trade_ts_ms) FROM agg_trade;"
            ).fetchone()
        min_ts, max_ts = result if result else (None, None)
    finally:
        db.close()

    if min_ts is None or max_ts is None:
        return start_dt, False

    existing_start = datetime.fromtimestamp((min_ts - 60_000) / 1000.0, tz=UTC)
    existing_end   = datetime.fromtimestamp(max_ts / 1000.0, tz=UTC)

    logger.info("[%s] Existing data: %s to %s",
                symbol,
                existing_start.strftime("%Y-%m-%d"),
                existing_end.strftime("%Y-%m-%d"))

    if start_dt < existing_start:
        logger.info("[%s] Requested start %s before existing %s — downloading missing range",
                    symbol,
                    start_dt.strftime("%Y-%m-%d"),
                    existing_start.strftime("%Y-%m-%d"))
        return start_dt, False

    resume_dt = existing_end + timedelta(milliseconds=1)
    if resume_dt > start_dt:
        logger.info("[%s] Resuming from %s", symbol, resume_dt.strftime("%Y-%m-%d"))
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
            logger.error("[%s] Download failed: %s", symbol, key, exc_info=True)
            return None

    tasks = [asyncio.create_task(_one(k)) for k in keys]
    try:
        for future in asyncio.as_completed(tasks):
            fp = await future
            done_count += 1
            if fp is not None:
                results.append(fp)
            if on_progress:
                on_progress(done_count, total)
            logger.debug("[%s] Downloaded %d/%d", symbol, done_count, total)
    except (asyncio.CancelledError, Exception):
        # Cancel all pending download tasks so they don't outlive the session.
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    return results


# ── Main per-symbol entry point ───────────────────────────────────────────────

async def process_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    start_dt: datetime,
    db_root: str | Path,
    io_sem: asyncio.Semaphore,
    per_asset_conc: int = 10,
    db_filename: str = "trades.db",
    end_dt: datetime | None = None,
    status_queue: asyncio.Queue | None = None,
) -> None:
    """Download and ingest archived aggTrades for a single symbol.

    Args:
        db_filename:    Target DB file name inside ``<db_root>/<symbol>/``.
                        Use ``"trades.db"`` for common DB or
                        ``"trades_hist_<start>-<end>.db"`` for historical.
        end_dt:         Upper bound (inclusive) for archive files to download.
                        Defaults to ``datetime.now(UTC)``.
        status_queue:   Optional queue for progress reports (0–100 %).
    """
    symbol = symbol.upper()
    db_path  = db_path_for_symbol(db_root, symbol, db_filename)
    temp_dir = Path(db_root) / symbol / "temp_trades"
    temp_dir.mkdir(parents=True, exist_ok=True)
    db: AggTradeDB | None = None

    def _emit(phase: str, pct: float, detail: str = "") -> None:
        if status_queue is None:
            return
        msg: Dict[str, Any] = {
            "symbol": symbol, "source": "archive",
            "phase": phase, "pct": round(pct, 1),
        }
        if detail:
            msg["detail"] = detail
        status_queue.put_nowait(msg)

    try:
        # 1. Resume any leftover temp CSVs from a previous interrupted run
        leftover = sorted(temp_dir.glob("*.csv"))
        if leftover:
            logger.info("[%s] Resuming %d leftover temp files", symbol, len(leftover))
            db = AggTradeDB(str(db_path))
            await asyncio.to_thread(_ingest_csv_files, db, leftover, symbol)
            db.close()
            db = None

        # 2. Resolve start time: advance past already-downloaded days.
        # Pass end_dt so the resolver caps its max-ts query to the archive
        # window — live WS data beyond end_dt must not mask historical gaps.
        start_dt, _ = _resolve_start_time(db_path, start_dt, symbol, end_dt)

        if end_dt is None:
            end_dt = datetime.now(tz=UTC)

        # 3. Build work plan
        monthly_list = await _s3_list(f"{UM_MONTHLY_PREFIX}/{symbol}/", session)
        daily_list   = await _s3_list(f"{UM_DAILY_PREFIX}/{symbol}/", session)

        monthly_zips = [k for k in monthly_list["keys"] if k.endswith(".zip")]
        daily_zips   = [k for k in daily_list["keys"]   if k.endswith(".zip")]

        plan = _plan_for_symbol(symbol, start_dt, end_dt, monthly_zips, daily_zips)
        logger.info("[%s] Plan: %d monthly + %d daily files",
                    symbol, len(plan.monthly_keys), len(plan.daily_keys))

        if plan.total == 0:
            logger.info("[%s] Already up to date", symbol)
            _emit("done", 100.0)
            return

        # 4. Download phase (0–50 %)
        def _dl_progress(done: int, total: int) -> None:
            _emit("download", done / total * 50.0, f"{done}/{total} files")

        extracted = await _download_files(
            session, plan.all_keys, temp_dir, io_sem, per_asset_conc,
            symbol, on_progress=_dl_progress,
        )
        logger.info("[%s] Downloaded %d files", symbol, len(extracted))

        # 5. Insert phase (50–100 %)
        _emit("insert", 50.0, f"0/{len(extracted)} files")
        db = AggTradeDB(str(db_path))
        sorted_files = sorted(extracted)
        total_inserted = 0

        for i, fp in enumerate(sorted_files, 1):
            try:
                raw = fp.read_bytes()
                rows = _parse_agg_csv_bytes(raw)
                total_inserted += await asyncio.to_thread(db.insert_rows, rows)
                fp.unlink()
            except Exception:
                logger.error("[%s] Insert failed: %s", symbol, fp, exc_info=True)
            _emit("insert", 50.0 + i / len(sorted_files) * 50.0,
                  f"{i}/{len(sorted_files)} files")

        # 6. Cleanup
        try:
            temp_dir.rmdir()
        except OSError:
            pass

        logger.info("[%s] Done — %d files, %d rows inserted",
                    symbol, len(extracted), total_inserted)
        _emit("done", 100.0)

    except Exception:
        logger.error("[%s] process_symbol failed", symbol, exc_info=True)
        _emit("error", 0.0, "process_symbol failed")
        raise
    finally:
        if db is not None:
            db.close()


# ── Archive availability probe ──────────────────────────────────────────────────

async def get_last_archive_date(
    session: aiohttp.ClientSession, symbol: str
) -> datetime | None:
    """Probe S3 for the most recent daily archive ZIP available for *symbol*.

    Checks up to 5 days back starting from yesterday UTC.  Uses HEAD requests
    so it is cheap (no download).

    Returns the date (midnight UTC) of the most recent available archive,
    or None if no archive is found within the probe window.
    """
    now = datetime.now(tz=UTC)
    for delta in range(1, 6):
        day = (now - timedelta(days=delta)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC
        )
        key = (
            f"{UM_DAILY_PREFIX}/{symbol}/"
            f"{symbol}-aggTrades-{day.strftime('%Y-%m-%d')}.zip"
        )
        try:
            async with session.head(
                f"{DATA_BASE}/{key}",
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=True,
            ) as resp:
                if resp.status == 200:
                    return day
        except Exception:
            continue
    return None


# ── Multi-symbol entry point ─────────────────────────────────────────────────

async def download_trades(
    config: Dict[str, Any],
    status_queue: asyncio.Queue | None = None,
) -> None:
    """Download archived trades for multiple symbols.

    Args:
        config: dict with keys:
            - ``symbols``:  ``{symbol: start_date_str}`` (``"YYYY-MM-DD"``)
            - ``db_root``:  root directory for DBs (default ``"../db_files"``)
            - ``files_concurrency``:    per-symbol download concurrency (default 10)
            - ``io_semaphore_limit``:   global IO semaphore (default 16)
            - ``connector_limit``:      aiohttp connector limit (default 64)
            - ``timeout_seconds``:      request timeout (default 900)
        status_queue: optional queue for progress dicts.
    """
    symbols_dict = config.get("symbols", {})
    db_root = config.get("db_root", "../db_files")
    files_concurrency = config.get("files_concurrency", 10)
    io_sem_limit = config.get("io_semaphore_limit", 16)
    connector_limit = config.get("connector_limit", 64)
    timeout_seconds = config.get("timeout_seconds", 900)

    if not symbols_dict:
        logger.warning("No symbols provided in config")
        return

    symbols_with_dates: list[tuple[str, datetime]] = []
    for symbol, start_date_str in symbols_dict.items():
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
            symbols_with_dates.append((symbol.upper(), start_date))
        except ValueError:
            logger.warning("Invalid date format '%s' for symbol %s", start_date_str, symbol)

    if not symbols_with_dates:
        logger.warning("No valid symbols after parsing")
        return

    logger.info("Processing %d symbols", len(symbols_with_dates))

    conn = aiohttp.TCPConnector(limit=connector_limit, ssl=False)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    io_sem = asyncio.Semaphore(io_sem_limit)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        for symbol, start_timestamp in symbols_with_dates:
            logger.info("Processing %s from %s", symbol, start_timestamp.strftime("%Y-%m-%d"))
            try:
                await process_symbol(
                    session, symbol, start_timestamp, db_root, io_sem,
                    files_concurrency, status_queue=status_queue,
                )
            except Exception:
                logger.error("Error processing symbol %s", symbol, exc_info=True)
# asyncio.run(download_trades(config))