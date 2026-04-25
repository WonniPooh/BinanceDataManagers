"""
Trades loader — orchestrates archive, REST, and WS components.

Provides a single entry point for loading trades data:
- ``load_recent(symbol, days=7)`` → REST gap-fill into common ``trades.db``
- ``load_historical(symbol, start_dt, end_dt)`` → archive into ``trades_hist_<start>-<end>.db``
  where dates are ``DDMMYY`` (e.g. ``050226``), range is inclusive ``[start, end]``
- ``start_live / extend_live / stop_live`` → WS stream management

REST and archive loads are queued and processed one at a time.  All
operations are cancellable per-symbol.

Progress is reported via an optional ``asyncio.Queue``.

Usage::

    loader = TradesLoader(db_root="db_files", rate_limiter=bnx_limiter)
    loader.start()

    await loader.load_recent("BTCUSDT", days=7)
    db_name = await loader.load_historical("BTCUSDT", start_dt, end_dt)
    loader.start_live("BTCUSDT", until_ms=now_ms + 3600_000)

    loader.cancel("BTCUSDT")
    await loader.stop()
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import aiohttp

from .trades_db_manager import AggTradeDB
from .trades_rest_downloader import fill_gap
from .trades_archive_downloader import (
    get_last_archive_date,
    process_symbol as archive_process_symbol,
)
from .trades_ws_stream import TradesWSStream

logger = logging.getLogger("trades_loader")
UTC = timezone.utc


@dataclass
class _QueueItem:
    """An item in the REST or archive processing queue."""
    symbol: str
    kind: str              # "rest" or "archive"
    # REST fields
    start_ms: int = 0
    end_ms: int = 0
    db_filename: str = "trades.db"
    # Archive fields
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    # Pre-probed last available S3 archive day (avoids a second probe in _do_rest)
    last_archive_day: Optional[datetime] = None
    # Skip archive download phase (targeted gap fills only need REST)
    skip_archive: bool = False
    # Completion future
    future: Optional[asyncio.Future] = None


class TradesLoader:
    """Top-level orchestrator for all trades data loading.

    Args:
        db_root:        Root directory for per-symbol DBs.
        rate_limiter:   Shared Binance rate limiter (``acquire`` / ``check_backpressure``).
        status_queue:   Optional queue for progress dicts.
    """

    def __init__(
        self,
        db_root: str | Path = "db_files",
        rate_limiter: Any | None = None,
        status_queue: asyncio.Queue | None = None,
        testnet: bool = False,  # reserved for future use; ignored
        on_reconnect_gap: Callable[[str, int, int], None] | None = None,
    ):
        self._db_root = Path(db_root)
        self._rate_limiter = rate_limiter
        self._status_queue = status_queue

        self._ws_stream = TradesWSStream(
            db_root=db_root,
            on_reconnect_gap=on_reconnect_gap,
        )

        # Queues
        self._rest_queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        self._archive_queue: asyncio.Queue[_QueueItem] = asyncio.Queue()

        # Priority slot: checked before the regular REST queue.
        # Set by load_recent(priority=True) to preempt the current item.
        self._priority_rest: _QueueItem | None = None

        # Workers
        self._rest_worker: asyncio.Task | None = None
        self._archive_worker: asyncio.Task | None = None

        # Active sub-tasks per symbol (for cancellation).
        # Managed by _process_queue, NOT by _do_rest/_do_archive.
        self._active: dict[str, asyncio.Task] = {}
        self._started = False

        # Cache for last available S3 archive date per symbol: {symbol: (checked_at, date)}
        self._archive_date_cache: dict[str, tuple[float, datetime | None]] = {}

    def set_archive_date_for_test(self, symbol: str, date: datetime | None) -> None:
        """Test hook: pre-seed the archive date cache to avoid network calls."""
        self._archive_date_cache[symbol.upper()] = (time.time(), date)

    @property
    def active_symbols(self) -> list[str]:
        return sorted(self._active.keys())

    @property
    def live_symbols(self) -> list[str]:
        return self._ws_stream.active_symbols

    def start(self) -> None:
        """Start queue workers and WS stream timers."""
        if self._started:
            return
        self._started = True
        self._rest_worker = asyncio.create_task(
            self._process_queue(self._rest_queue, "rest"),
            name="trades-rest-worker",
        )
        self._archive_worker = asyncio.create_task(
            self._process_queue(self._archive_queue, "archive"),
            name="trades-archive-worker",
        )
        self._ws_stream.start()
        logger.info("TradesLoader started")

    async def stop(self) -> None:
        """Stop all workers, WS streams, and cancel in-flight tasks."""
        self._started = False

        # Cancel all active tasks
        for sym, task in list(self._active.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._active.clear()

        # Stop workers
        for worker in (self._rest_worker, self._archive_worker):
            if worker:
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass

        await self._ws_stream.stop()
        logger.info("TradesLoader stopped")

    # ── Archive availability probe (cached) ──────────────────────────

    _ARCHIVE_CACHE_TTL = 30 * 60  # seconds

    @staticmethod
    def _archive_end_dt(last_archive_day: datetime | None) -> datetime:
        """Return the last date fully covered by S3 archives (inclusive).

        If the S3 probe returned a date, that is the answer.  Fallback:
        2 days ago midnight UTC — conservative, accounts for S3 lag.
        """
        if last_archive_day:
            return last_archive_day
        return (
            datetime.now(tz=UTC) - timedelta(days=2)
        ).replace(hour=0, minute=0, second=0, microsecond=0)

    async def _get_last_archive_date(self, symbol: str) -> datetime | None:
        """Return the most recent archive day available on S3 for *symbol* (cached 30 min)."""
        now = time.time()
        entry = self._archive_date_cache.get(symbol)
        if entry and now - entry[0] < self._ARCHIVE_CACHE_TTL:
            return entry[1]
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            result = await get_last_archive_date(session, symbol)
        self._archive_date_cache[symbol] = (now, result)
        if result:
            logger.info("[%s] Last available archive date: %s",
                        symbol, result.strftime("%Y-%m-%d"))
        else:
            logger.warning("[%s] No archive found in 5-day probe window", symbol)
        return result

    # ── Public API: REST gap-fill ────────────────────────────────────

    def get_last_trade_ts(self, symbol: str) -> int | None:
        """Return the latest trade timestamp in the DB, or None if empty/missing."""
        db_path = self._db_root / symbol.upper() / "trades.db"
        if not db_path.exists():
            return None
        db = AggTradeDB(str(db_path))
        try:
            return db.last_present_timestamp
        finally:
            db.close()

    def record_bridge_gap(self, symbol: str, pre_ws_max_ts: int, first_ws_ts: int) -> None:
        """Record a gap between old DB data and the first WS trade.

        Must be called **before** ``flush_live`` so that even if the
        subsequent REST fill is cancelled, the gap row persists and
        will be picked up on the next ``load_recent`` call.
        """
        if pre_ws_max_ts >= first_ws_ts - 1000:
            return  # no meaningful gap (≤1 s)
        symbol = symbol.upper()
        db_path = self._db_root / symbol / "trades.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db = AggTradeDB(str(db_path))
        try:
            db.open_gap(pre_ws_max_ts, first_ws_ts)
            logger.info(
                "[%s] Recorded bridge gap [%d → %d] (%.1fs)",
                symbol, pre_ws_max_ts, first_ws_ts,
                (first_ws_ts - pre_ws_max_ts) / 1000,
            )
        finally:
            db.close()

    @staticmethod
    def _probe_db_for_resume(
        db_path: Path, start_ms: int, end_ms: int,
        archive_boundary_ms: int,
        pre_ws_max_ts: int | None, caller_end_ms: int | None,
        symbol: str, days: int,
    ) -> tuple[str, int, bool]:
        """Probe DB for existing data and determine resume strategy.

        Returns ``(resume_info, start_ms, should_skip)``.
        Runs in a worker thread — no async code.
        """
        resume_info = "fresh"
        if not db_path.exists():
            return resume_info, start_ms, False

        db = AggTradeDB(str(db_path))
        try:
            max_ts = db.last_present_timestamp
            if max_ts and max_ts > archive_boundary_ms:
                row = db.conn.execute(
                    "SELECT MIN(trade_ts_ms) FROM agg_trade;"
                ).fetchone()
                min_ts = row[0] if row and row[0] is not None else None
                if min_ts and min_ts <= start_ms:
                    all_gaps = db.list_gaps()
                    real_gaps = []
                    for gid, gs, gf in all_gaps:
                        if gs >= gf:
                            db.close_gap(gid)
                            continue
                        # Verify actual data continuity, not just presence.
                        # A partially-filled range (interrupted REST/archive)
                        # may have some rows but still contain large gaps.
                        row = db.conn.execute(
                            "SELECT COUNT(*), MAX(delta) FROM ("
                            "  SELECT trade_ts_ms - LAG(trade_ts_ms)"
                            "    OVER (ORDER BY trade_ts_ms) AS delta"
                            "  FROM agg_trade"
                            "  WHERE trade_ts_ms >= ? AND trade_ts_ms < ?"
                            ")",
                            (gs, gf),
                        ).fetchone()
                        cnt = row[0] if row else 0
                        max_delta = row[1] if row and row[1] is not None else None
                        if cnt == 0:
                            # No trades at all — definitely a gap.
                            real_gaps.append((gid, gs, gf))
                        elif max_delta is not None and max_delta > 60_000:
                            # Trades present but data has a hole > 60s — keep gap.
                            logger.info(
                                "[%s] load_recent: gap id=%d [%d→%d] has %d "
                                "trades but max_delta=%.1fs — keeping",
                                symbol, gid, gs, gf, cnt, max_delta / 1000)
                            real_gaps.append((gid, gs, gf))
                        else:
                            # Data is contiguous (no hole > 60s) — safe to close.
                            db.close_gap(gid)
                    closed = len(all_gaps) - len(real_gaps)
                    if closed:
                        logger.info(
                            "[%s] load_recent: closed %d stale rest_gap rows "
                            "(%d real gaps remain)",
                            symbol, closed, len(real_gaps))
                    if not real_gaps:
                        if (pre_ws_max_ts is not None and caller_end_ms is not None
                                and pre_ws_max_ts + 1000 < caller_end_ms):
                            logger.info(
                                "[%s] load_recent: WS bridge gap detected "
                                "(pre_ws_max_ts=%d, caller_end_ms=%d, gap=%.1fs) "
                                "— proceeding with download",
                                symbol, pre_ws_max_ts, caller_end_ms,
                                (caller_end_ms - pre_ws_max_ts) / 1000)
                        else:
                            logger.info(
                                "[%s] load_recent: DB already covers range "
                                "(min_ts=%d <= start_ms=%d, max_ts=%d) — skipping",
                                symbol, min_ts, start_ms, max_ts)
                            return resume_info, start_ms, True
                open_gaps = db.list_gaps()
                resume_info = (f"live-WS data present (max_ts={max_ts}, "
                               f"min_ts={min_ts}, gaps={len(open_gaps)}), "
                               f"keeping full {days}d range")
            elif max_ts and start_ms <= max_ts <= archive_boundary_ms:
                start_ms = max_ts + 1
                resume_info = f"resume from max_ts={max_ts}"
            else:
                resume_info = f"DB exists but max_ts={max_ts}"
        finally:
            db.close()
        return resume_info, start_ms, False

    async def load_recent(
        self, symbol: str, days: int = 7, *, priority: bool = False,
        end_ms: int | None = None,
        pre_ws_max_ts: int | None = None,
        skip_archive: bool = False,
    ) -> int:
        """Queue a REST gap-fill for the last *days* into common ``trades.db``.

        Returns the number of trades inserted (once complete).

        Args:
            priority:  If True, cancel any currently active REST item and
                       process this load next (before whatever is queued).
            end_ms:    If provided, use this as the upper bound instead of
                       ``now + 5s``.  Typically the first WS trade ts so
                       REST covers right up to where live data begins.
            pre_ws_max_ts: The last trade timestamp in the DB *before* WS
                       started.  Used to detect the gap between old data
                       and new WS trades that has no ``rest_gap`` row.
        """
        symbol = symbol.upper()
        now_ms = int(time.time() * 1000)
        # caller_end_ms: the original value supplied by the caller (first WS
        # trade ts or similar).  Used in the WS bridge-gap check so the +1s
        # fetch buffer below does not inflate the perceived gap size.
        caller_end_ms = end_ms
        if end_ms is None:
            # Fallback: small buffer so REST overlaps with WS connect time.
            end_ms = now_ms + 5_000
            caller_end_ms = end_ms
        else:
            end_ms += 1_000
        start_ms = now_ms - days * 86_400_000

        # Targeted gap fill: use pre_ws_max_ts as the exact start, skip
        # archive boundary probing and DB resume logic entirely.
        if skip_archive and pre_ws_max_ts is not None:
            start_ms = pre_ws_max_ts
            logger.info("[%s] load_recent: targeted gap fill  %d → %d (%.1fs)  priority=%s",
                        symbol, start_ms, end_ms,
                        (end_ms - start_ms) / 1000, priority)
            loop = asyncio.get_running_loop()
            future: asyncio.Future[int] = loop.create_future()
            item = _QueueItem(
                symbol=symbol, kind="rest",
                start_ms=start_ms, end_ms=end_ms,
                skip_archive=True,
                future=future,
            )
            if priority:
                self._priority_rest = item
                for sym, task in list(self._active.items()):
                    if not task.done():
                        logger.info("[%s] Preempting active %s for priority gap fill",
                                    symbol, sym)
                        task.cancel()
            else:
                self._rest_queue.put_nowait(item)
            self._emit_queued(symbol, "rest")
            return await future

        # Determine the archive boundary: the start of the day AFTER the last
        # daily archive ZIP that is actually available on S3 for this symbol.
        # Never advance start_ms past this boundary — live WS data flushed to
        # the DB (max_ts ≈ now) must not be mistaken for historical coverage,
        # which would skip the archive phase and leave history unfilled.
        last_archive_day = await self._get_last_archive_date(symbol)
        archive_boundary_ms = int(
            (self._archive_end_dt(last_archive_day) + timedelta(days=1)).timestamp() * 1000
        )

        # Check existing data to resume from (historical fills only).
        # All DB I/O is pushed to a worker thread so the event loop is not blocked.
        db_path = self._db_root / symbol / "trades.db"
        resume_info, start_ms, skip = await asyncio.to_thread(
            self._probe_db_for_resume,
            db_path, start_ms, end_ms, archive_boundary_ms,
            pre_ws_max_ts, caller_end_ms, symbol, days,
        )
        if skip:
            return 0

        logger.info("[%s] load_recent: days=%d  start_ms=%d  end_ms=%d  (%.1fh)  priority=%s  %s",
                    symbol, days, start_ms, end_ms,
                    (end_ms - start_ms) / 3_600_000, priority, resume_info)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[int] = loop.create_future()
        item = _QueueItem(
            symbol=symbol, kind="rest",
            start_ms=start_ms, end_ms=end_ms,
            last_archive_day=last_archive_day,
            skip_archive=skip_archive,
            future=future,
        )

        if priority:
            # Cancel whatever the REST worker is currently processing.
            # The worker will pick up this item from the priority slot next.
            self._priority_rest = item
            for sym, task in list(self._active.items()):
                if not task.done():
                    logger.info("[%s] Preempting active %s for priority load",
                                symbol, sym)
                    task.cancel()
        else:
            self._rest_queue.put_nowait(item)

        self._emit_queued(symbol, "rest")
        return await future

    # ── Public API: Historical archive load ──────────────────────────

    async def load_historical(
        self, symbol: str, start_dt: datetime, end_dt: datetime | None = None,
    ) -> str:
        """Queue an archive download into a separate historical DB.

        Returns the DB filename (e.g. ``trades_hist_050226-100226.db``).
        Date range is inclusive ``[start_dt, end_dt]``, formatted as ``DDMMYY``.
        """
        symbol = symbol.upper()
        if end_dt is None:
            end_dt = datetime.now(tz=UTC)
        # Inclusive [start, end] date-based naming: DDMMYY
        start_tag = start_dt.strftime("%d%m%y")
        end_tag = end_dt.strftime("%d%m%y")
        db_filename = f"trades_hist_{start_tag}-{end_tag}.db"

        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        item = _QueueItem(
            symbol=symbol, kind="archive",
            start_dt=start_dt, end_dt=end_dt,
            db_filename=db_filename,
            future=future,
        )
        self._archive_queue.put_nowait(item)
        self._emit_queued(symbol, "archive")
        return await future

    # ── Public API: WS live stream ───────────────────────────────────

    async def start_live(self, symbol: str, until_ms: int) -> None:
        """Start live WS aggTrade stream for *symbol*."""
        await self._ws_stream.subscribe(symbol, until_ms)

    async def extend_live(self, symbol: str, until_ms: int) -> None:
        """Extend live WS stream for *symbol*."""
        await self._ws_stream.extend(symbol, until_ms)

    async def stop_live(self, symbol: str) -> None:
        """Stop live WS stream for *symbol*."""
        await self._ws_stream.unsubscribe(symbol)

    async def flush_live(self, symbol: str) -> int:
        """Force-flush buffered WS trades for *symbol* to DB."""
        return await self._ws_stream.flush_symbol_async(symbol)

    async def wait_for_first_trade(self, symbol: str, timeout: float = 30.0) -> int | None:
        """Wait for the first live WS trade for *symbol*.  Returns ts (ms) or None."""
        return await self._ws_stream.wait_for_first_trade(symbol, timeout)

    # ── Cancel ───────────────────────────────────────────────────────

    def cancel(self, symbol: str) -> None:
        """Cancel any active load operation for *symbol*."""
        sym = symbol.upper()
        task = self._active.pop(sym, None)
        if task and not task.done():
            task.cancel()
            logger.info("[%s] Cancelled active task", sym)
            self._emit(sym, "cancelled", "cancelled")

    # ── Internal: queue processing ───────────────────────────────────

    async def _process_queue(
        self, queue: asyncio.Queue[_QueueItem], queue_name: str,
    ) -> None:
        """Worker: pull items from *queue* and process sequentially.

        Work is run in a sub-task so that cancelling a single item (via
        ``cancel()`` or priority preemption) does **not** kill the worker
        loop itself.
        """
        while True:
            # ── Pick next item ────────────────────────────────────────
            item: _QueueItem | None = None
            if queue_name == "rest" and self._priority_rest is not None:
                item = self._priority_rest
                self._priority_rest = None
                logger.info("[%s] picked up from priority slot", item.symbol)

            if item is None:
                # Use a short timeout for the REST worker so it can notice
                # a priority item that arrives while we are blocked.
                if queue_name == "rest":
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=0.5)
                    except asyncio.TimeoutError:
                        continue
                    except asyncio.CancelledError:
                        return
                else:
                    try:
                        item = await queue.get()
                    except asyncio.CancelledError:
                        return

            # ── Run in a sub-task ─────────────────────────────────────
            if item.kind == "rest":
                work_task = asyncio.create_task(
                    self._do_rest(item), name=f"rest-{item.symbol}")
            else:
                work_task = asyncio.create_task(
                    self._do_archive(item), name=f"archive-{item.symbol}")

            self._active[item.symbol] = work_task

            try:
                result = await work_task
                if item.kind == "rest":
                    if item.future and not item.future.done():
                        item.future.set_result(result)
                else:
                    if item.future and not item.future.done():
                        item.future.set_result(item.db_filename)
            except asyncio.CancelledError:
                if item.future and not item.future.done():
                    item.future.cancel()
                logger.info("[%s] %s item preempted — worker continues",
                            item.symbol, queue_name)
                # Worker stays alive; next iteration picks up the queue.
            except Exception as exc:
                logger.error("[%s] %s queue item failed",
                             item.symbol, queue_name, exc_info=True)
                if item.future and not item.future.done():
                    item.future.set_exception(exc)
            finally:
                self._active.pop(item.symbol, None)

    async def _do_rest(self, item: _QueueItem) -> int:
        """Execute a recent-data load: REST for recent gap, then archive for bulk days."""
        symbol = item.symbol
        db_path = self._db_root / symbol / item.db_filename
        db_path.parent.mkdir(parents=True, exist_ok=True)

        total_inserted = 0
        # Open one DB handle for the entire operation (gap tracking + REST fill).
        db = AggTradeDB(str(db_path))

        logger.info("[%s] _do_rest START  db=%s  range=[%d → %d] (%.1fh)",
                    symbol, db_path, item.start_ms, item.end_ms,
                    (item.end_ms - item.start_ms) / 3_600_000)

        try:
            # Compute archive boundaries using the last available archive day
            # probed in load_recent (stored on the queue item — no second S3 probe).
            # The archive phase covers [archive_start_dt, archive_end_dt] inclusive;
            # REST starts from the following day so there is no overlap.
            archive_start_dt = datetime.fromtimestamp(item.start_ms / 1000, tz=UTC)
            archive_end_dt = self._archive_end_dt(item.last_archive_day)
            has_archive = archive_end_dt.date() >= archive_start_dt.date()

            # Step 1: REST gap-fill (recent data — fast, user sees chart data
            # within seconds).  Runs BEFORE archives and gap resolution so the
            # UI is not blocked by slower S3 downloads or old-gap recovery.
            if item.skip_archive:
                # Targeted gap fill — use exact range, no archive boundary logic.
                rest_start_ms = item.start_ms
            else:
                # Full load — REST starts from the day after the last archive
                # to avoid overlap with archive data.
                rest_start_ms = (
                    int((archive_end_dt + timedelta(days=1)).timestamp() * 1000)
                    if has_archive else item.start_ms
                )

            if rest_start_ms < item.end_ms:
                logger.info("[%s] REST phase: %d → %d (%.1fh)",
                            symbol, rest_start_ms, item.end_ms,
                            (item.end_ms - rest_start_ms) / 3_600_000)
                gap_id = db.open_gap(rest_start_ms, item.end_ms)
                try:
                    async with aiohttp.ClientSession() as session:
                        total_inserted = await fill_gap(
                            session, symbol,
                            rest_start_ms, item.end_ms,
                            db,
                            rate_limiter=self._rate_limiter,
                            status_queue=self._status_queue,
                            gap_id=gap_id,
                            gap_db=db,
                        )
                    db.close_gap(gap_id)
                    logger.info("[%s] REST phase DONE  inserted=%d", symbol, total_inserted)
                except asyncio.CancelledError:
                    logger.info("[%s] REST phase CANCELLED at gap_id=%d", symbol, gap_id)
                    # Gap row remains with last frontier_ms — resolved on next run.
                    raise
            else:
                logger.info("[%s] REST phase SKIPPED (rest_start_ms >= end_ms)", symbol)

            # Signal that recent data is ready — browser can render the chart
            # while archives fill older history in the background.
            self._emit(symbol, "done", f"rest: {total_inserted} trades")

            # Step 2: Resolve gaps left from previous interrupted runs.
            # Runs AFTER REST so recent data is already visible to the user.
            # Gaps covered by Step 1's range are closed without re-fetching;
            # partially overlapping gaps have their frontier shrunk so only
            # the truly unfilled portion is re-downloaded.
            old_gaps = db.list_gaps()
            if old_gaps:
                remaining = []
                for gid, gs, gf in old_gaps:
                    if gs >= rest_start_ms and gf <= item.end_ms:
                        # Entirely within REST range — already filled.
                        db.close_gap(gid)
                        logger.info("[%s] Closed gap id=%d — covered by REST phase", symbol, gid)
                    elif gs < rest_start_ms and gf > rest_start_ms:
                        # Gap partially overlaps: [gs → gf] but [rest_start_ms → gf]
                        # is already filled.  Shrink frontier to rest_start_ms so
                        # only [gs → rest_start_ms] remains.
                        db.update_gap(gid, rest_start_ms)
                        logger.info("[%s] Shrunk gap id=%d frontier %s → %s (REST covers the rest)",
                                    symbol, gid,
                                    datetime.fromtimestamp(gf / 1000, tz=UTC).strftime("%H:%M"),
                                    datetime.fromtimestamp(rest_start_ms / 1000, tz=UTC).strftime("%H:%M"))
                        remaining.append((gid, gs, rest_start_ms))
                    else:
                        remaining.append((gid, gs, gf))
                if remaining:
                    logger.info("[%s] Resolving %d gaps from previous runs", symbol, len(remaining))
                    await self._resolve_gaps(db, symbol)

            # Step 3: Archive phase (historical bulk — slow, fills older data
            # in the background after the user already has recent chart data).
            if has_archive and not item.skip_archive:
                logger.info("[%s] Archive phase: %s → %s",
                            symbol, archive_start_dt.date(), archive_end_dt.date())
                self._emit(symbol, "archive-phase",
                           "downloading daily archives...")
                io_sem = asyncio.Semaphore(16)
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=64, ssl=False),
                    timeout=aiohttp.ClientTimeout(total=900),
                ) as session:
                    await archive_process_symbol(
                        session, symbol, archive_start_dt,
                        str(self._db_root), io_sem,
                        db_filename=item.db_filename,
                        end_dt=archive_end_dt,
                        status_queue=self._status_queue,
                    )
                logger.info("[%s] Archive phase DONE", symbol)
            else:
                logger.info("[%s] Archive phase SKIPPED (end %s <= start %s)",
                            symbol, archive_end_dt.date(), archive_start_dt.date())

            logger.info("[%s] _do_rest DONE  total=%d", symbol, total_inserted)
            return total_inserted
        except asyncio.CancelledError:
            logger.info("[%s] _do_rest CANCELLED", symbol)
            raise
        except Exception:
            logger.error("[%s] _do_rest ERROR", symbol, exc_info=True)
            raise
        finally:
            db.close()

    async def _resolve_gaps(self, db: AggTradeDB, symbol: str) -> None:
        """Fill any gaps left by previous interrupted REST runs.

        Uses archives for old ranges, REST for recent ones.  See
        docs/features/trades-gap-tracking.md for the full algorithm.
        """
        gaps = db.list_gaps()
        if not gaps:
            return

        archive_cutoff_ms = int(time.time() * 1000) - 86_400_000  # yesterday

        for gap_id, gap_start_ms, frontier_ms in gaps:
            if frontier_ms <= gap_start_ms:
                # Degenerate row — nothing to fill.
                db.close_gap(gap_id)
                continue

            gap_start_str = datetime.fromtimestamp(
                gap_start_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M")
            frontier_str = datetime.fromtimestamp(
                frontier_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M")
            logger.info("[%s] Resolving gap id=%d [%s → %s]",
                        symbol, gap_id, gap_start_str, frontier_str)

            if frontier_ms <= archive_cutoff_ms:
                # Case A: entire unfilled range is archive-eligible.
                archive_start_dt = datetime.fromtimestamp(gap_start_ms / 1000, tz=UTC)
                archive_end_dt = datetime.fromtimestamp(frontier_ms / 1000, tz=UTC)
                self._emit(symbol, "gap-archive",
                           f"archived gap {archive_start_dt.date()}–{archive_end_dt.date()}")
                io_sem = asyncio.Semaphore(16)
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=64, ssl=False),
                    timeout=aiohttp.ClientTimeout(total=900),
                ) as session:
                    await archive_process_symbol(
                        session, symbol, archive_start_dt,
                        str(self._db_root), io_sem,
                        db_filename="trades.db",
                        end_dt=archive_end_dt,
                        status_queue=self._status_queue,
                    )
                db.close_gap(gap_id)

            elif gap_start_ms < archive_cutoff_ms:
                # Case B: split — archive covers the old portion, REST the tail.
                archive_start_dt = datetime.fromtimestamp(gap_start_ms / 1000, tz=UTC)
                archive_end_dt = datetime.fromtimestamp(archive_cutoff_ms / 1000, tz=UTC)
                self._emit(symbol, "gap-archive", "filling archived portion of gap")
                io_sem = asyncio.Semaphore(16)
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=64, ssl=False),
                    timeout=aiohttp.ClientTimeout(total=900),
                ) as session:
                    await archive_process_symbol(
                        session, symbol, archive_start_dt,
                        str(self._db_root), io_sem,
                        db_filename="trades.db",
                        end_dt=archive_end_dt,
                        status_queue=self._status_queue,
                    )
                # Mark archive portion done so a subsequent cancellation of
                # the REST phase leaves the frontier inside the REST-only zone.
                db.update_gap(gap_id, archive_cutoff_ms)

                self._emit(symbol, "gap-rest", "filling recent portion of gap")
                async with aiohttp.ClientSession() as session:
                    await fill_gap(
                        session, symbol,
                        archive_cutoff_ms, frontier_ms,
                        db,
                        rate_limiter=self._rate_limiter,
                        status_queue=self._status_queue,
                        gap_id=gap_id,
                        gap_db=db,
                    )
                db.close_gap(gap_id)

            else:
                # Case C: entirely within the last day — REST only.
                self._emit(symbol, "gap-rest", "filling recent gap with REST")
                async with aiohttp.ClientSession() as session:
                    await fill_gap(
                        session, symbol,
                        gap_start_ms, frontier_ms,
                        db,
                        rate_limiter=self._rate_limiter,
                        status_queue=self._status_queue,
                        gap_id=gap_id,
                        gap_db=db,
                    )
                db.close_gap(gap_id)

    async def _do_archive(self, item: _QueueItem) -> None:
        """Execute an archive download item."""
        symbol = item.symbol
        # NOTE: _active is managed by _process_queue.

        io_sem = asyncio.Semaphore(16)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=64, ssl=False),
            timeout=aiohttp.ClientTimeout(total=900),
        ) as session:
            await archive_process_symbol(
                session, symbol, item.start_dt,
                str(self._db_root), io_sem,
                db_filename=item.db_filename,
                end_dt=item.end_dt,
                status_queue=self._status_queue,
            )
        self._emit(symbol, "done", f"archive: {item.db_filename}")

    # ── Progress helpers ─────────────────────────────────────────────

    def _emit(self, symbol: str, phase: str, detail: str = "") -> None:
        if self._status_queue is None:
            return
        msg: Dict[str, Any] = {
            "symbol": symbol, "source": "loader",
            "phase": phase,
        }
        if detail:
            msg["detail"] = detail
        self._status_queue.put_nowait(msg)

    def _emit_queued(self, symbol: str, queue_name: str) -> None:
        if self._status_queue is None:
            return
        self._status_queue.put_nowait({
            "symbol": symbol, "source": "loader",
            "phase": "queued", "queue": queue_name,
        })
