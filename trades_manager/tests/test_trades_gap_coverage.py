"""
Unit tests for load_recent() coverage-check / gap-cleanup logic.

Tests verify that load_recent() correctly:
  1. Skips redundant downloads when DB covers full range (no gaps)
  2. Closes stale gap rows when data has since been filled by archives/WS
  3. Keeps real gap rows (unfilled ranges with zero trades in DB)
  4. Handles degenerate gap rows (gs >= gf)
  5. Does NOT skip when min_ts > start_ms (data starts too late)
  6. Does NOT skip when max_ts is old (not recent live data)
  7. Mix of stale + real gaps — stale closed, real kept, download proceeds
  8. Empty DB (no trades, max_ts=None) — download proceeds
  9. DB doesn't exist — download proceeds (fresh)
 10. Multiple stale gaps with data in each — all closed

No network — uses real SQLite DB in logs/ temp dir.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
import time
from pathlib import Path

# Ensure imports resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from trades_manager.trades_db_manager import AggTradeDB
from trades_manager.trades_loader import TradesLoader

TEST_DB_ROOT = "logs/test_trades_gap_coverage"
SYMBOL = "TESTUSDT"

# Archive date seeded into the loader cache so tests never make network calls.
# Matches the fallback: 2 days ago midnight UTC.
def _test_archive_day() -> "datetime":
    from datetime import datetime, timedelta, timezone
    return (
        datetime.now(tz=timezone.utc) - timedelta(days=2)
    ).replace(hour=0, minute=0, second=0, microsecond=0)


def _setup():
    if os.path.exists(TEST_DB_ROOT):
        shutil.rmtree(TEST_DB_ROOT)
    os.makedirs(TEST_DB_ROOT, exist_ok=True)


def _teardown():
    if os.path.exists(TEST_DB_ROOT):
        shutil.rmtree(TEST_DB_ROOT)


def _db_path() -> Path:
    p = Path(TEST_DB_ROOT) / SYMBOL
    p.mkdir(parents=True, exist_ok=True)
    return p / "trades.db"


def _insert_trades(db: AggTradeDB, start_ms: int, end_ms: int,
                   interval_ms: int = 60_000, start_id: int = 1) -> int:
    """Insert evenly-spaced fake trades from start_ms to end_ms inclusive."""
    rows = []
    ts = start_ms
    tid = start_id
    while ts <= end_ms:
        rows.append((tid, ts, 100.0, 1.0, 0, 1))
        ts += interval_ms
        tid += 1
    db.insert_rows(rows)
    return len(rows)


def _run(coro):
    """Run an async function in a fresh event loop."""
    return asyncio.run(coro)


async def _load_recent_with_loader(days: int = 7, end_ms: int | None = None,
                                   pre_ws_max_ts: int | None = None) -> int:
    """Create loader, call load_recent — no workers started (no network)."""
    loader = TradesLoader(db_root=TEST_DB_ROOT, rate_limiter=None,
                          status_queue=None)
    loader.set_archive_date_for_test(SYMBOL, _test_archive_day())
    return await loader.load_recent(SYMBOL, days=days, end_ms=end_ms,
                                    pre_ws_max_ts=pre_ws_max_ts)


async def _load_recent_with_timeout(days: int = 7, end_ms: int | None = None,
                                    timeout: float = 2.0,
                                    pre_ws_max_ts: int | None = None) -> int | None:
    """Call load_recent with no workers — times out on queue put (no network).

    Returns int if load_recent completes quickly (skip path), or None if
    it times out waiting on the queue (download-would-proceed path).
    """
    loader = TradesLoader(db_root=TEST_DB_ROOT, rate_limiter=None,
                          status_queue=None)
    loader.set_archive_date_for_test(SYMBOL, _test_archive_day())
    try:
        return await asyncio.wait_for(
            loader.load_recent(SYMBOL, days=days, end_ms=end_ms,
                               pre_ws_max_ts=pre_ws_max_ts),
            timeout=timeout,
        )
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None


# ── Helpers for time anchoring ────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


# ══════════════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════════════


def test_full_coverage_no_gaps():
    """DB has data spanning full range, no gap rows → skip (return 0)."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=3_600_000)
        db.close()

        result = _run(_load_recent_with_loader(days=7, end_ms=end_ms))
        assert result == 0, f"Expected 0 (skip), got {result}"
        print("  [OK] test_full_coverage_no_gaps")
    finally:
        _teardown()


def test_full_coverage_stale_gaps_all_filled():
    """DB has full data, 3 stale gap rows whose ranges contain trades → all closed, skip."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=300_000)
        db.open_gap(now - 86_400_000, now - 43_200_000)
        db.open_gap(now - 2 * 86_400_000, now - 86_400_000)
        db.open_gap(now - 3 * 86_400_000, now - 2 * 86_400_000)
        assert len(db.list_gaps()) == 3
        db.close()

        result = _run(_load_recent_with_loader(days=7, end_ms=end_ms))
        assert result == 0, f"Expected 0 (skip), got {result}"

        db2 = AggTradeDB(str(_db_path()))
        assert len(db2.list_gaps()) == 0, f"Expected 0 gaps after cleanup, got {len(db2.list_gaps())}"
        db2.close()
        print("  [OK] test_full_coverage_stale_gaps_all_filled")
    finally:
        _teardown()


def test_real_gap_not_closed():
    """DB has full min/max range but a gap row pointing at a truly empty hole → kept, download proceeds."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        hole_start = now - 3 * 86_400_000
        hole_end = now - 2 * 86_400_000

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, hole_start - 60_000,
                       interval_ms=3_600_000, start_id=1)
        _insert_trades(db, hole_end + 60_000, end_ms,
                       interval_ms=3_600_000, start_id=100_000)
        db.open_gap(hole_start, hole_end)
        db.close()

        result = _run(_load_recent_with_timeout(days=7, end_ms=end_ms))
        assert result is None or result != 0, "Real gap should NOT be skipped"

        db2 = AggTradeDB(str(_db_path()))
        gaps = db2.list_gaps()
        db2.close()
        assert len(gaps) == 1, f"Real gap row should persist, got {len(gaps)} gaps"
        print("  [OK] test_real_gap_not_closed")
    finally:
        _teardown()


def test_degenerate_gap_closed():
    """Gap row with gs >= gf (fully walked) → closed as degenerate, skip."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=3_600_000)
        ts = now - 86_400_000
        db.open_gap(ts, ts)                      # gs == gf
        db.open_gap(now - 50_000, now - 100_000)  # gs > gf
        db.close()

        result = _run(_load_recent_with_loader(days=7, end_ms=end_ms))
        assert result == 0, f"Expected 0 (skip), got {result}"

        db2 = AggTradeDB(str(_db_path()))
        assert len(db2.list_gaps()) == 0, f"Degenerate gaps should be closed"
        db2.close()
        print("  [OK] test_degenerate_gap_closed")
    finally:
        _teardown()


def test_min_ts_too_late_no_skip():
    """DB has recent data but min_ts > start_ms → download proceeds (don't skip)."""
    _setup()
    try:
        now = _now_ms()
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        late_start = now - 3 * 86_400_000
        _insert_trades(db, late_start, end_ms, interval_ms=3_600_000)
        db.close()

        result = _run(_load_recent_with_timeout(days=7, end_ms=end_ms))
        assert result is None or result != 0, "min_ts > start_ms should not skip"
        print("  [OK] test_min_ts_too_late_no_skip")
    finally:
        _teardown()


def test_max_ts_old_no_skip():
    """DB has data but max_ts is old (< archive_boundary) → resume path, not coverage skip."""
    _setup()
    try:
        now = _now_ms()
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        old_start = now - 5 * 86_400_000
        old_end = now - 3 * 86_400_000
        _insert_trades(db, old_start, old_end, interval_ms=3_600_000)
        db.close()

        result = _run(_load_recent_with_timeout(days=7, end_ms=end_ms))
        assert result is None or result != 0, "Old max_ts should not trigger coverage skip"
        print("  [OK] test_max_ts_old_no_skip")
    finally:
        _teardown()


def test_mixed_stale_and_real_gaps():
    """Mix: 2 stale gaps (data present) + 1 real gap (empty hole) → stale closed, real kept."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        hole_start = now - 4 * 86_400_000
        hole_end = now - 3 * 86_400_000

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, hole_start - 60_000,
                       interval_ms=3_600_000, start_id=1)
        _insert_trades(db, hole_end + 60_000, end_ms,
                       interval_ms=3_600_000, start_id=100_000)
        db.open_gap(now - 2 * 86_400_000, now - 86_400_000)  # stale
        db.open_gap(now - 86_400_000, now - 43_200_000)       # stale
        db.open_gap(hole_start, hole_end)                     # real
        assert len(db.list_gaps()) == 3
        db.close()

        result = _run(_load_recent_with_timeout(days=7, end_ms=end_ms))
        assert result is None or result != 0, "Real gap exists — should not skip"

        db2 = AggTradeDB(str(_db_path()))
        gaps_after = db2.list_gaps()
        db2.close()
        assert len(gaps_after) == 1, f"Expected 1 real gap remaining, got {len(gaps_after)}"
        _, gs, gf = gaps_after[0]
        assert gs == hole_start, f"Remaining gap start should be {hole_start}, got {gs}"
        assert gf == hole_end, f"Remaining gap frontier should be {hole_end}, got {gf}"
        print("  [OK] test_mixed_stale_and_real_gaps")
    finally:
        _teardown()


def test_empty_db_no_skip():
    """DB exists but has zero trades → download proceeds."""
    _setup()
    try:
        now = _now_ms()
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        db.close()

        result = _run(_load_recent_with_timeout(days=7, end_ms=end_ms))
        assert result is None or result != 0, "Empty DB should not skip"
        print("  [OK] test_empty_db_no_skip")
    finally:
        _teardown()


def test_no_db_no_skip():
    """DB file doesn't exist → download proceeds (fresh)."""
    _setup()
    try:
        now = _now_ms()
        end_ms = now

        result = _run(_load_recent_with_timeout(days=7, end_ms=end_ms))
        assert result is None or result != 0, "Missing DB should not skip"
        print("  [OK] test_no_db_no_skip")
    finally:
        _teardown()


def test_multiple_stale_gaps_data_in_each():
    """4 stale gap rows each covering different filled ranges → all closed, skip."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=60_000)
        for i in range(4):
            gs = now - (i + 1) * 86_400_000
            gf = gs + 43_200_000
            db.open_gap(gs, gf)
        assert len(db.list_gaps()) == 4
        db.close()

        result = _run(_load_recent_with_loader(days=7, end_ms=end_ms))
        assert result == 0, f"Expected 0 (skip), got {result}"

        db2 = AggTradeDB(str(_db_path()))
        assert len(db2.list_gaps()) == 0, "All stale gaps should be closed"
        db2.close()
        print("  [OK] test_multiple_stale_gaps_data_in_each")
    finally:
        _teardown()


def test_gap_exactly_at_boundary():
    """Gap row [gs, gf) where gs == gf-1ms — single-ms range with data → stale."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=3_600_000)
        gs = now - 86_400_000
        gf = gs + 3_600_000
        db.open_gap(gs, gf)
        db.close()

        result = _run(_load_recent_with_loader(days=7, end_ms=end_ms))
        assert result == 0, f"Expected 0 (skip), got {result}"

        db2 = AggTradeDB(str(_db_path()))
        assert len(db2.list_gaps()) == 0
        db2.close()
        print("  [OK] test_gap_exactly_at_boundary")
    finally:
        _teardown()


def test_ws_bridge_gap_detected():
    """Full coverage by min/max, no gap rows, but pre_ws_max_ts << end_ms → download proceeds."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        # Old data up to 12h ago
        old_edge = now - 12 * 3_600_000
        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, old_edge, interval_ms=3_600_000, start_id=1)
        # WS trades from "now - 5min" to "now" (simulating WS flush)
        _insert_trades(db, now - 300_000, now, interval_ms=60_000, start_id=200_000)
        db.close()

        # pre_ws_max_ts = old_edge (the DB edge before WS started)
        result = _run(_load_recent_with_timeout(
            days=7, end_ms=end_ms, pre_ws_max_ts=old_edge))
        assert result is None or result != 0, \
            "WS bridge gap should trigger download, not skip"
        print("  [OK] test_ws_bridge_gap_detected")
    finally:
        _teardown()


def test_ws_bridge_no_gap_when_fresh():
    """pre_ws_max_ts is close to end_ms (< 1s gap) → no bridge gap, skip."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=3_600_000)
        db.close()

        # pre_ws_max_ts is basically at end_ms (within 1s)
        result = _run(_load_recent_with_loader(
            days=7, end_ms=end_ms, pre_ws_max_ts=end_ms - 500))
        assert result == 0, f"No bridge gap — should skip, got {result}"
        print("  [OK] test_ws_bridge_no_gap_when_fresh")
    finally:
        _teardown()


def test_ws_bridge_no_gap_without_pre_ws():
    """pre_ws_max_ts=None (not provided) → old behavior, skip when full coverage."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        end_ms = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, end_ms, interval_ms=3_600_000)
        db.close()

        result = _run(_load_recent_with_loader(days=7, end_ms=end_ms))
        assert result == 0, f"No pre_ws_max_ts — should skip, got {result}"
        print("  [OK] test_ws_bridge_no_gap_without_pre_ws")
    finally:
        _teardown()


# ════════════════════════════════════════════════════════════════════════
# _archive_end_dt and archive boundary tests
# ════════════════════════════════════════════════════════════════════════


def test_archive_end_dt_uses_probe_result():
    """_archive_end_dt returns the probed date when S3 returned one."""
    from datetime import datetime, timezone
    day = datetime(2026, 4, 19, tzinfo=timezone.utc)
    result = TradesLoader._archive_end_dt(day)
    assert result == day, f"Expected {day}, got {result}"
    print("  [OK] test_archive_end_dt_uses_probe_result")


def test_archive_end_dt_fallback_is_2_days_ago():
    """_archive_end_dt fallback is start of 2 days ago midnight UTC."""
    import time
    from datetime import datetime, timedelta, timezone
    result = TradesLoader._archive_end_dt(None)
    expected = (
        datetime.now(tz=timezone.utc) - timedelta(days=2)
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    # Allow 1s tolerance for test execution time
    diff = abs((result - expected).total_seconds())
    assert diff < 1, f"Fallback off by {diff}s"
    assert result.hour == 0 and result.minute == 0 and result.second == 0, \
        "Fallback must be midnight"
    print("  [OK] test_archive_end_dt_fallback_is_2_days_ago")


def test_archive_boundary_ms_is_start_of_day_after_archive():
    """archive_boundary_ms = midnight of (last_archive_day + 1) so the day after
    the last archive is NOT included in the archive phase."""
    from datetime import datetime, timedelta, timezone
    day = datetime(2026, 4, 19, tzinfo=timezone.utc)  # archive covers up to end of Apr 19
    expected_boundary = datetime(2026, 4, 20, tzinfo=timezone.utc)  # start of Apr 20
    boundary_ms = int(
        (TradesLoader._archive_end_dt(day) + timedelta(days=1)).timestamp() * 1000
    )
    assert boundary_ms == int(expected_boundary.timestamp() * 1000), \
        f"Boundary ms mismatch"
    print("  [OK] test_archive_boundary_ms_is_start_of_day_after_archive")


def test_archive_end_dt_seeded_via_test_hook():
    """set_archive_date_for_test pre-seeds the cache so load_recent never calls S3."""
    from datetime import datetime, timezone
    day = datetime(2026, 4, 18, tzinfo=timezone.utc)
    loader = TradesLoader(db_root=TEST_DB_ROOT)
    loader.set_archive_date_for_test(SYMBOL, day)
    cached = loader._archive_date_cache.get(SYMBOL.upper())
    assert cached is not None
    _, cached_day = cached
    assert cached_day == day
    print("  [OK] test_archive_end_dt_seeded_via_test_hook")


# ════════════════════════════════════════════════════════════════════════
# record_bridge_gap tests
# ════════════════════════════════════════════════════════════════════════


def test_record_bridge_gap_creates_gap_row():
    """record_bridge_gap writes a rest_gap row [pre_ws_max_ts, first_ws_ts)."""
    _setup()
    try:
        now = _now_ms()
        pre_ws = now - 3_600_000  # 1 hour ago
        first_ws = now

        # Create DB with some old data so it exists
        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, pre_ws - 86_400_000, pre_ws, interval_ms=3_600_000)
        db.close()

        loader = TradesLoader(db_root=TEST_DB_ROOT)
        loader.record_bridge_gap(SYMBOL, pre_ws, first_ws)

        db2 = AggTradeDB(str(_db_path()))
        gaps = db2.list_gaps()
        db2.close()
        assert len(gaps) == 1, f"Expected 1 gap row, got {len(gaps)}"
        _, gs, gf = gaps[0]
        assert gs == pre_ws, f"gap_start_ms: expected {pre_ws}, got {gs}"
        assert gf == first_ws, f"frontier_ms: expected {first_ws}, got {gf}"
        print("  [OK] test_record_bridge_gap_creates_gap_row")
    finally:
        _teardown()


def test_record_bridge_gap_skips_tiny_gap():
    """record_bridge_gap does nothing if gap ≤ 1 second."""
    _setup()
    try:
        now = _now_ms()
        pre_ws = now - 500  # 0.5 s gap
        first_ws = now

        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, pre_ws - 86_400_000, pre_ws, interval_ms=3_600_000)
        db.close()

        loader = TradesLoader(db_root=TEST_DB_ROOT)
        loader.record_bridge_gap(SYMBOL, pre_ws, first_ws)

        db2 = AggTradeDB(str(_db_path()))
        gaps = db2.list_gaps()
        db2.close()
        assert len(gaps) == 0, f"Expected 0 gaps for tiny gap, got {len(gaps)}"
        print("  [OK] test_record_bridge_gap_skips_tiny_gap")
    finally:
        _teardown()


def test_bridge_gap_survives_and_prevents_skip():
    """Bridge gap row from record_bridge_gap causes load_recent to NOT skip."""
    _setup()
    try:
        now = _now_ms()
        start_ms = now - 7 * 86_400_000
        pre_ws = now - 3_600_000
        first_ws = now - 300_000  # 5 min ago

        # Old historical data + WS data (simulating first run flush)
        db = AggTradeDB(str(_db_path()))
        _insert_trades(db, start_ms - 1000, pre_ws, interval_ms=3_600_000, start_id=1)
        _insert_trades(db, first_ws, now, interval_ms=60_000, start_id=200_000)
        db.close()

        # Record bridge gap (what first run should have done)
        loader = TradesLoader(db_root=TEST_DB_ROOT)
        loader.set_archive_date_for_test(SYMBOL, _test_archive_day())
        loader.record_bridge_gap(SYMBOL, pre_ws, first_ws)

        # Second run: pre_ws_max_ts is now the WS max (simulating polluted value)
        result = _run(_load_recent_with_timeout(
            days=7, end_ms=now, pre_ws_max_ts=now))
        assert result is None or result != 0, \
            "Bridge gap row should prevent skip, got return 0"
        print("  [OK] test_bridge_gap_survives_and_prevents_skip")
    finally:
        _teardown()


# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("[test_trades_gap_coverage]")
    test_full_coverage_no_gaps()
    test_full_coverage_stale_gaps_all_filled()
    test_real_gap_not_closed()
    test_degenerate_gap_closed()
    test_min_ts_too_late_no_skip()
    test_max_ts_old_no_skip()
    test_mixed_stale_and_real_gaps()
    test_empty_db_no_skip()
    test_no_db_no_skip()
    test_multiple_stale_gaps_data_in_each()
    test_gap_exactly_at_boundary()
    test_ws_bridge_gap_detected()
    test_ws_bridge_no_gap_when_fresh()
    test_ws_bridge_no_gap_without_pre_ws()
    test_archive_end_dt_uses_probe_result()
    test_archive_end_dt_fallback_is_2_days_ago()
    test_archive_boundary_ms_is_start_of_day_after_archive()
    test_archive_end_dt_seeded_via_test_hook()
    test_record_bridge_gap_creates_gap_row()
    test_record_bridge_gap_skips_tiny_gap()
    test_bridge_gap_survives_and_prevents_skip()
    print("All tests passed.")
