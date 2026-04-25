"""
User trades manager — paginated sync of user trade history to SQLite.

Required client interface (BinanceFuturesClient or compatible):

  get_user_trades(symbol, start_time, end_time, limit) -> list[dict]
    Keys used: id, orderId, symbol, side, price, qty, quoteQty,
               commission, commissionAsset, realizedPnl,
               maker, buyer, positionSide, time
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from .user_trades_db_manager import UserTradeDB

logger = logging.getLogger("data_manager.user_trades_manager")

_QUERY_WINDOW_MS = 6 * 24 * 60 * 60 * 1000  # 6 days in ms


async def sync_trades(
    client, symbol: str, start_time: int, end_time: int, db: UserTradeDB
) -> int:
    total = 0
    window_start = start_time
    limit = 1000
    while window_start < end_time:
        window_end = min(window_start + _QUERY_WINDOW_MS, end_time)
        last_trade_id = None

        while True:
            try:
                if last_trade_id is not None:
                    trades = await client.get_user_trades(
                        symbol=symbol,
                        from_id=last_trade_id + 1,
                        limit=limit,
                    )
                    # fromId has no time bound — stop when we pass window_end
                    trades = [t for t in trades if int(t.get("time", 0)) <= window_end]
                else:
                    trades = await client.get_user_trades(
                        symbol=symbol,
                        start_time=window_start if window_start > 0 else None,
                        end_time=window_end,
                        limit=limit,
                    )
            except Exception as exc:
                logger.error("[%s] userTrades failed: %s", symbol, exc)
                return total
            if trades:
                rows = [_trade_to_row(t) for t in trades]
                db.insert_rows(rows)
                total += len(rows)
                last_trade_id = max(int(t["id"]) for t in trades)
            if len(trades) < limit:
                break

        window_start = window_end + 1
    return total


def _trade_to_row(t: Dict[str, Any]) -> dict:
    return {
        "trade_id":         int(t["id"]),
        "order_id":         int(t["orderId"]),
        "symbol":           t["symbol"],
        "side":             t.get("side", "BUY"),
        "price":            float(t["price"]),
        "qty":              float(t["qty"]),
        "quote_qty":        float(t.get("quoteQty", 0)),
        "commission":       float(t.get("commission", 0)),
        "commission_asset": t.get("commissionAsset", ""),
        "realized_pnl":     float(t.get("realizedPnl", 0)),
        "is_maker":         1 if t.get("maker") else 0,
        "is_buyer":         1 if t.get("buyer") else 0,
        "position_side":    t.get("positionSide", "BOTH"),
        "trade_time_ms":    int(t.get("time", 0)),
    }


def ws_event_to_trade_row(parsed: Dict[str, Any]) -> dict:
    """Map a parsed UserDataWS ORDER_TRADE_UPDATE dict (short Binance keys)
    to a UserTradeDB row dict.

    Only meaningful when executionType (parsed["x"]) == "TRADE".
    The WS event carries fill details that map directly to user_trade columns.
    """
    return {
        "trade_id":         parsed["t"],
        "order_id":         parsed["i"],
        "symbol":           parsed["s"],
        "side":             parsed.get("S", "BUY"),
        "price":            float(parsed.get("L", 0)),   # lastFilledPrice
        "qty":              float(parsed.get("l", 0)),   # lastFilledQty
        "quote_qty":        float(parsed.get("L", 0)) * float(parsed.get("l", 0)),
        "commission":       float(parsed.get("n", 0)),
        "commission_asset": parsed.get("N", ""),
        "realized_pnl":     float(parsed.get("rp", 0)),
        "is_maker":         1 if parsed.get("m") else 0,
        "is_buyer":         1 if parsed.get("S") == "BUY" else 0,
        "position_side":    parsed.get("ps", "BOTH"),
        "trade_time_ms":    parsed.get("T", 0),
    }
