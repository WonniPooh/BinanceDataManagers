"""
Integration test — trades_cleanup module.

Creates test DBs with old and recent data, runs cleanup functions, verifies
correct data is pruned. No network access — purely local DB operations.

Usage:
    cd <project_root>
    python -m BinanceDataManagers.trades_manager.tests.test_trades_cleanup_integration
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from BinanceDataManagers.trades_manager.trades_db_manager import AggTradeDB
from BinanceDataManagers.trades_manager.trades_cleanup import (
    cleanup_common_db,
    cleanup_stale_historical_dbs,
    cleanup_all_symbols,
)

# ── Config ────────────────────────────────────────────────────────────────────

DB_ROOT = _project_root / "logs" / "test_trades_cleanup_integration"
SYMBOL = "TESTCOIN"


def _cleanup():
    if DB_ROOT.exists():
        shutil.rmtree(DB_ROOT)


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _create_test_db(db_path: Path, rows: list[tuple]) -> None:
    """Create an AggTradeDB and insert test rows."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = AggTradeDB(str(db_path))
    db.insert_rows(rows)
    db.close()


def test_cleanup_common_db():
    """Verify common DB cleanup prunes rows older than max_age_days."""
    logging.info("[TEST] test_cleanup_common_db")

    db_path = DB_ROOT / SYMBOL / "trades.db"
    now_ms = int(time.time() * 1000)

    # Create rows: 5 old (10 days ago) + 5 recent (1 day ago)
    old_ts = now_ms - 10 * 86_400_000
    recent_ts = now_ms - 1 * 86_400_000
    rows = []
    for i in range(5):
        rows.append((100 + i, old_ts + i * 1000, 1.0, 1.0, 0, 1))
    for i in range(5):
        rows.append((200 + i, recent_ts + i * 1000, 2.0, 2.0, 1, 1))

    _create_test_db(db_path, rows)

    # Verify all 10 rows present
    db = AggTradeDB(str(db_path))
    count = db.conn.execute("SELECT COUNT(*) FROM agg_trade").fetchone()[0]
    db.close()
    assert count == 10, f"Expected 10 rows, got {count}"

    # Run cleanup with 7-day max age
    deleted = cleanup_common_db(str(DB_ROOT), SYMBOL, max_age_days=7)
    assert deleted == 5, f"Expected 5 deleted, got {deleted}"

    # Only recent rows should remain
    db = AggTradeDB(str(db_path))
    remaining = db.conn.execute("SELECT COUNT(*) FROM agg_trade").fetchone()[0]
    db.close()
    assert remaining == 5, f"Expected 5 remaining, got {remaining}"

    logging.info("  [PASS] Pruned 5 old rows, kept 5 recent")


def test_cleanup_stale_historical_dbs():
    """Verify stale historical DB files are removed based on mtime."""
    logging.info("[TEST] test_cleanup_stale_historical_dbs")

    sym_dir = DB_ROOT / SYMBOL
    sym_dir.mkdir(parents=True, exist_ok=True)

    # Create 2 historical DB files
    fresh_db = sym_dir / "trades_hist_fresh.db"
    stale_db = sym_dir / "trades_hist_stale.db"

    _create_test_db(fresh_db, [(1, int(time.time() * 1000), 1.0, 1.0, 0, 1)])
    _create_test_db(stale_db, [(2, int(time.time() * 1000), 2.0, 2.0, 1, 1)])

    # Set stale DB mtime to 5 days ago
    stale_mtime = time.time() - 5 * 86_400
    os.utime(stale_db, (stale_mtime, stale_mtime))

    assert fresh_db.exists()
    assert stale_db.exists()

    # Run cleanup with 3-day unused threshold
    removed = cleanup_stale_historical_dbs(str(DB_ROOT), SYMBOL, unused_days=3)
    assert len(removed) == 1, f"Expected 1 removed, got {len(removed)}"
    assert "trades_hist_stale.db" in removed[0]

    assert fresh_db.exists(), "Fresh DB should not be removed"
    assert not stale_db.exists(), "Stale DB should be removed"

    logging.info("  [PASS] Removed 1 stale DB, kept 1 fresh")


def test_cleanup_all_symbols():
    """Verify cleanup_all_symbols sweeps all symbol dirs."""
    logging.info("[TEST] test_cleanup_all_symbols")

    # Create old data for 2 symbols
    now_ms = int(time.time() * 1000)
    old_ts = now_ms - 10 * 86_400_000

    for sym in ("SYM1", "SYM2"):
        db_path = DB_ROOT / sym / "trades.db"
        _create_test_db(db_path, [
            (1, old_ts, 1.0, 1.0, 0, 1),
            (2, now_ms - 86_400_000, 2.0, 2.0, 1, 1),
        ])

    results = cleanup_all_symbols(str(DB_ROOT), max_age_days=7, unused_days=3)
    total_pruned = sum(r["common_pruned"] for r in results.values())
    assert total_pruned >= 2, (
        f"Expected at least 2 old rows pruned across symbols, got {total_pruned}")

    logging.info("  [PASS] cleanup_all_symbols pruned %d rows across %d symbols",
                  total_pruned, len(results))


def main():
    _setup_logging()
    _cleanup()
    try:
        test_cleanup_common_db()
        test_cleanup_stale_historical_dbs()
        test_cleanup_all_symbols()
    except AssertionError as e:
        logging.error("TEST FAILED: %s", e)
        sys.exit(1)
    except Exception:
        logging.error("TEST ERROR", exc_info=True)
        sys.exit(2)
    finally:
        _cleanup()

    logging.info("test_trades_cleanup_integration: ALL PASSED")


if __name__ == "__main__":
    main()
