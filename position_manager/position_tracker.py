"""
Position Tracker — reconstructs closed positions from user_trade rows.

One-way mode (position_side=BOTH): BUY opens/adds to LONG, SELL opens/adds to SHORT.
When accumulated qty hits zero → position is closed. Partial closes emit
proportional closed-position records.

Algorithm:
  1. Process trades in trade_time_ms order
  2. Track running position: qty (signed: +LONG / -SHORT), entry fills
  3. When a trade reduces the position → close proportional entry fills (FIFO)
  4. When position hits zero → emit completed position record
  5. If position flips sign → close current, open new in opposite direction
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("position_tracker")


@dataclass
class _Fill:
    """A single entry fill in the position."""
    price: float
    qty: float  # always positive
    order_id: int
    trade_time_ms: int
    commission: float
    commission_asset: str


@dataclass
class _RunningPosition:
    """Tracks the current open position for a symbol."""
    symbol: str = ""
    side: str = ""  # "LONG" or "SHORT" or ""
    qty: float = 0.0  # always positive remaining qty
    entry_fills: list[_Fill] = field(default_factory=list)
    entry_order_ids: set[int] = field(default_factory=set)
    exit_order_ids: set[int] = field(default_factory=set)
    exit_fills: list[_Fill] = field(default_factory=list)
    fee_total: float = 0.0
    realized_pnl: float = 0.0


def reconstruct_positions(trades: list[dict]) -> list[dict]:
    """Reconstruct closed positions from a list of user_trade dicts.

    Args:
        trades: List of user_trade rows, sorted by trade_time_ms ascending.

    Returns:
        List of closed position dicts, each with:
            symbol, side, entry_price, exit_price, quantity,
            realized_pnl, pnl_pct, fee_total,
            entry_time_ms, exit_time_ms,
            entry_order_ids (JSON), exit_order_ids (JSON), duration_ms
    """
    if not trades:
        return []

    closed: list[dict] = []
    pos = _RunningPosition()

    for trade in trades:
        symbol = trade["symbol"]
        side = trade["side"]  # BUY or SELL
        price = float(trade["price"])
        qty = float(trade["qty"])
        order_id = trade["order_id"]
        trade_time = trade["trade_time_ms"]
        commission = float(trade.get("commission", 0))
        commission_asset = trade.get("commission_asset", "")
        trade_pnl = float(trade.get("realized_pnl", 0))

        # Determine trade direction: BUY = +1 (long), SELL = -1 (short)
        is_buy = side == "BUY"

        if pos.qty == 0:
            # No open position — this trade opens a new one
            pos.symbol = symbol
            pos.side = "LONG" if is_buy else "SHORT"
            pos.qty = qty
            pos.entry_fills = [_Fill(price, qty, order_id, trade_time, commission, commission_asset)]
            pos.entry_order_ids = {order_id}
            pos.exit_order_ids = set()
            pos.exit_fills = []
            pos.fee_total = commission
            pos.realized_pnl = 0.0
            continue

        # Position exists — check if this trade adds to or reduces position
        same_direction = (pos.side == "LONG" and is_buy) or (pos.side == "SHORT" and not is_buy)

        if same_direction:
            # Adding to position
            pos.qty += qty
            pos.entry_fills.append(_Fill(price, qty, order_id, trade_time, commission, commission_asset))
            pos.entry_order_ids.add(order_id)
            pos.fee_total += commission
        else:
            # Reducing/closing/flipping position
            pos.fee_total += commission
            pos.exit_order_ids.add(order_id)
            pos.realized_pnl += trade_pnl

            if qty < pos.qty - 1e-8:
                # Partial close
                pos.exit_fills.append(_Fill(price, qty, order_id, trade_time, commission, commission_asset))
                pos.qty -= qty
                # Consume entry fills FIFO for the closed portion
                _consume_entry_fills_fifo(pos.entry_fills, qty)
            elif abs(qty - pos.qty) < 1e-8:
                # Full close — position goes to zero
                pos.exit_fills.append(_Fill(price, qty, order_id, trade_time, commission, commission_asset))
                closed.append(_emit_position(pos))
                pos = _RunningPosition()
            else:
                # Flip: close current + open opposite with remainder
                close_qty = pos.qty
                flip_qty = qty - close_qty
                pos.exit_fills.append(_Fill(price, close_qty, order_id, trade_time, commission, commission_asset))
                closed.append(_emit_position(pos))

                # Open new position in opposite direction
                flip_side = "SHORT" if pos.side == "LONG" else "LONG"
                pos = _RunningPosition()
                pos.symbol = symbol
                pos.side = flip_side
                pos.qty = flip_qty
                pos.entry_fills = [_Fill(price, flip_qty, order_id, trade_time, 0, commission_asset)]
                pos.entry_order_ids = {order_id}
                pos.fee_total = 0.0
                pos.realized_pnl = 0.0

    return closed


def _consume_entry_fills_fifo(fills: list[_Fill], qty_to_consume: float) -> None:
    """Remove qty from entry fills in FIFO order (for partial close tracking)."""
    remaining = qty_to_consume
    while remaining > 1e-10 and fills:
        if fills[0].qty <= remaining + 1e-10:
            remaining -= fills[0].qty
            fills.pop(0)
        else:
            fills[0] = _Fill(
                fills[0].price,
                fills[0].qty - remaining,
                fills[0].order_id,
                fills[0].trade_time_ms,
                fills[0].commission,
                fills[0].commission_asset,
            )
            remaining = 0


def _emit_position(pos: _RunningPosition) -> dict:
    """Convert a closed RunningPosition to a position dict."""
    entry_price = _weighted_avg_price(pos.entry_fills)
    exit_price = _weighted_avg_price(pos.exit_fills)
    total_qty = sum(f.qty for f in pos.exit_fills)

    # PnL calculation
    if pos.realized_pnl != 0:
        # Use Binance-reported PnL (more accurate, includes funding adjustments)
        pnl = pos.realized_pnl
    else:
        # Fallback: calculate from prices
        if pos.side == "LONG":
            pnl = (exit_price - entry_price) * total_qty
        else:
            pnl = (entry_price - exit_price) * total_qty

    entry_notional = entry_price * total_qty
    pnl_pct = (pnl / entry_notional * 100) if entry_notional > 0 else 0.0

    entry_time = pos.entry_fills[0].trade_time_ms if pos.entry_fills else 0
    exit_time = pos.exit_fills[-1].trade_time_ms if pos.exit_fills else 0

    return {
        "symbol": pos.symbol,
        "side": pos.side,
        "entry_price": round(entry_price, 8),
        "exit_price": round(exit_price, 8),
        "quantity": round(total_qty, 8),
        "realized_pnl": round(pnl, 8),
        "pnl_pct": round(pnl_pct, 4),
        "fee_total": round(pos.fee_total, 8),
        "entry_time_ms": entry_time,
        "exit_time_ms": exit_time,
        "entry_order_ids": json.dumps(sorted(pos.entry_order_ids)),
        "exit_order_ids": json.dumps(sorted(pos.exit_order_ids)),
        "duration_ms": exit_time - entry_time,
    }


def _weighted_avg_price(fills: list[_Fill]) -> float:
    """Calculate quantity-weighted average price."""
    total_cost = sum(f.price * f.qty for f in fills)
    total_qty = sum(f.qty for f in fills)
    return total_cost / total_qty if total_qty > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# Incremental tracker — for live WS-driven position tracking
# ═══════════════════════════════════════════════════════════════════════════════


class LivePositionTracker:
    """Tracks running positions per symbol incrementally.

    Usage:
        tracker = LivePositionTracker()
        tracker.warm("BTCUSDT", historical_trades)   # replay history
        closed = tracker.on_trade(live_trade_dict)    # returns closed pos or None
    """

    def __init__(self) -> None:
        self._positions: dict[str, _RunningPosition] = {}

    def warm(self, symbol: str, trades: list[dict]) -> None:
        """Replay historical trades to establish running position state.

        Closed positions are discarded — only the final running state matters.
        """
        pos = _RunningPosition()
        for trade in trades:
            pos = self._apply_trade(pos, trade)[1]
        if pos.qty > 0:
            self._positions[symbol] = pos
        else:
            self._positions.pop(symbol, None)
        logger.info("[%s] Warmed position tracker: %s qty=%.6f",
                    symbol, pos.side or "flat", pos.qty)

    def on_trade(self, trade: dict) -> list[dict]:
        """Process a single live trade. Returns list of closed positions (0, 1, or 2 on flip)."""
        symbol = trade["symbol"]
        pos = self._positions.get(symbol, _RunningPosition())
        closed, pos = self._apply_trade(pos, trade)
        if pos.qty > 0:
            self._positions[symbol] = pos
        else:
            self._positions.pop(symbol, None)
        return closed

    @staticmethod
    def _apply_trade(pos: _RunningPosition, trade: dict) -> tuple[list[dict], _RunningPosition]:
        """Apply one trade to a running position. Returns (closed_positions, new_running_pos)."""
        symbol = trade["symbol"]
        side = trade["side"]
        price = float(trade["price"])
        qty = float(trade["qty"])
        order_id = trade["order_id"]
        trade_time = trade["trade_time_ms"]
        commission = float(trade.get("commission", 0))
        commission_asset = trade.get("commission_asset", "")
        trade_pnl = float(trade.get("realized_pnl", 0))

        is_buy = side == "BUY"
        closed: list[dict] = []

        if pos.qty == 0:
            # Open new position
            pos.symbol = symbol
            pos.side = "LONG" if is_buy else "SHORT"
            pos.qty = qty
            pos.entry_fills = [_Fill(price, qty, order_id, trade_time, commission, commission_asset)]
            pos.entry_order_ids = {order_id}
            pos.exit_order_ids = set()
            pos.exit_fills = []
            pos.fee_total = commission
            pos.realized_pnl = 0.0
            return closed, pos

        same_direction = (pos.side == "LONG" and is_buy) or (pos.side == "SHORT" and not is_buy)

        if same_direction:
            pos.qty += qty
            pos.entry_fills.append(_Fill(price, qty, order_id, trade_time, commission, commission_asset))
            pos.entry_order_ids.add(order_id)
            pos.fee_total += commission
        else:
            pos.fee_total += commission
            pos.exit_order_ids.add(order_id)
            pos.realized_pnl += trade_pnl

            if qty < pos.qty - 1e-8:
                pos.exit_fills.append(_Fill(price, qty, order_id, trade_time, commission, commission_asset))
                pos.qty -= qty
                _consume_entry_fills_fifo(pos.entry_fills, qty)
            elif abs(qty - pos.qty) < 1e-8:
                pos.exit_fills.append(_Fill(price, qty, order_id, trade_time, commission, commission_asset))
                closed.append(_emit_position(pos))
                pos = _RunningPosition()
            else:
                close_qty = pos.qty
                flip_qty = qty - close_qty
                pos.exit_fills.append(_Fill(price, close_qty, order_id, trade_time, commission, commission_asset))
                closed.append(_emit_position(pos))
                flip_side = "SHORT" if pos.side == "LONG" else "LONG"
                pos = _RunningPosition()
                pos.symbol = symbol
                pos.side = flip_side
                pos.qty = flip_qty
                pos.entry_fills = [_Fill(price, flip_qty, order_id, trade_time, 0, commission_asset)]
                pos.entry_order_ids = {order_id}
                pos.fee_total = 0.0
                pos.realized_pnl = 0.0

        return closed, pos
