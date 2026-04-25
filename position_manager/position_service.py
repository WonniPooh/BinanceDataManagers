"""
Position Service — orchestrates position reconstruction and live tracking.

Used by:
  - collector: on startup to gap-fill, and on each TRADE event for live updates
  - chart-ui-server: to query closed positions via REST API

Thread-safety: the service is safe to call from a single asyncio event loop.
DB writes are synchronous but fast (single inserts with WAL mode).
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Callable

from position_db_manager import PositionDB
from position_tracker import reconstruct_positions, _RunningPosition, _Fill

logger = logging.getLogger("position_service")


class PositionService:
    """Manages position DB and live position tracking for one symbol."""

    def __init__(self, db_root: str, symbol: str) -> None:
        self._symbol = symbol
        db_path = os.path.join(db_root, symbol, "positions.db")
        self._db = PositionDB(db_path)
        self._running: _RunningPosition = _RunningPosition()

    def close(self) -> None:
        self._db.close()

    @property
    def db(self) -> PositionDB:
        return self._db

    def reconstruct_from_trades(self, trades: list[dict]) -> list[dict]:
        """Reconstruct positions from user_trade rows and persist them.

        Args:
            trades: user_trade dicts sorted by trade_time_ms ascending.

        Returns:
            List of newly inserted position dicts.
        """
        positions = reconstruct_positions(trades)
        for pos in positions:
            self._db.insert_position(pos)
        if positions:
            logger.info("[%s] Reconstructed %d positions (last exit: %d)",
                        self._symbol, len(positions),
                        positions[-1]["exit_time_ms"])
        return positions

    def get_last_exit_time(self) -> int | None:
        """Get the latest exit_time_ms from DB."""
        return self._db.get_last_exit_time(self._symbol)

    def get_positions(
        self,
        since_ms: int | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Fetch closed positions from DB."""
        return self._db.get_positions(
            symbol=self._symbol, since_ms=since_ms, limit=limit
        )
