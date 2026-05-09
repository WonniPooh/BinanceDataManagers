"""
Order data manager — paginated sync of order history and amendments to SQLite.

Required client interface (BinanceFuturesClient or compatible):

  get_orders(symbol, start_time, end_time, order_id, limit) -> list[dict]
    Keys used: orderId, symbol, clientOrderId, side, origType, type, status,
               price, stopPrice, origQty, executedQty, avgPrice,
               time, updateTime, positionSide, reduceOnly, timeInForce

  get_order_amendments(symbol, order_id, limit) -> list[dict]
    Keys used: amendmentId, orderId, symbol, clientOrderId, time,
               amendment.price.before, amendment.price.after,
               amendment.origQty.before, amendment.origQty.after,
               amendment.count
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from .order_events_db_manager import OrderEventDB

logger = logging.getLogger("BinanceDataManagers.order_data_manager")

_QUERY_WINDOW_MS = 6 * 24 * 60 * 60 * 1000  # 6 days in ms


async def sync_orders(
    client, symbol: str, start_time: int, end_time: int, db: OrderEventDB
) -> int:
    total = 0
    window_start = start_time
    limit = 1000
    while window_start < end_time:
        window_end = min(window_start + _QUERY_WINDOW_MS, end_time)
        last_order_id = None

        while True:
            try:
                orders = await client.get_orders(
                    symbol=symbol,
                    start_time=window_start if last_order_id is None and window_start > 0 else None,
                    end_time=window_end,
                    order_id=last_order_id + 1 if last_order_id is not None else None,
                    limit=limit,
                )
            except Exception as exc:
                logger.error("[%s] allOrders failed: %s", symbol, exc)
                return total
            if orders:
                rows = [_live_order_to_row(o) for o in orders]
                db.insert_rows(rows)
                total += len(rows)
                last_order_id = max(int(o["orderId"]) for o in orders)
            if len(orders) < limit:
                break

        window_start = window_end + 1
    return total


async def sync_amendments_for_order(
    client, symbol: str, order_id: int, db: OrderEventDB,
) -> int:
    """Fetch and store all amendments for a single order from REST."""
    try:
        amendments = await client.get_order_amendments(symbol=symbol, order_id=order_id, limit=100)
    except Exception as exc:
        logger.warning("[%s] orderAmendment REST fetch failed for order %d: %s", symbol, order_id, exc)
        return 0
    if amendments:
        rows = [_amendment_to_row(a) for a in amendments]
        db.insert_amendment_rows(rows)
        return len(rows)
    return 0


async def sync_amendments(
    client, symbol: str, start_time: int,
    order_db: OrderEventDB,
) -> int:
    # Fetch amendments for ALL LIMIT orders in the sync window.
    # Amendments are cheap (weight 1) and we can't know which orders were
    # amended if WS was down when it happened — just check them all.
    candidate_ids = _find_all_limit_order_ids_in_window(order_db, start_time)
    if not candidate_ids:
        return 0

    total = 0
    for order_id in candidate_ids:
        try:
            amendments = await client.get_order_amendments(
                symbol=symbol, order_id=order_id, limit=100,
            )
        except Exception as exc:
            logger.warning("[%s] orderAmendment failed for order %d: %s", symbol, order_id, exc)
            continue
        if amendments:
            rows = [_amendment_to_row(a) for a in amendments]
            order_db.insert_amendment_rows(rows)
            total += len(rows)
    return total


def _find_all_limit_order_ids_in_window(db: OrderEventDB, start_time: int) -> List[int]:
    """All LIMIT orders in the sync window, regardless of terminal status."""
    rows = db.conn.execute(
        "SELECT DISTINCT order_id FROM order_event "
        "WHERE order_type = 'LIMIT' AND transaction_time_ms >= ?",
        (start_time,),
    ).fetchall()
    return [r[0] for r in rows]


def _live_order_to_row(o: Dict[str, Any]) -> dict:
    status = o.get("status", "NEW")
    exec_type_map = {
        "NEW": "NEW", "PARTIALLY_FILLED": "TRADE", "FILLED": "TRADE",
        "CANCELED": "CANCELED", "REJECTED": "NEW",
        "EXPIRED": "EXPIRED", "EXPIRED_IN_MATCH": "EXPIRED",
    }
    return {
        "order_id":               int(o["orderId"]),
        "symbol":                 o["symbol"],
        "client_order_id":        o.get("clientOrderId", ""),
        "side":                   o.get("side", "BUY"),
        "order_type":             o.get("origType", o.get("type", "LIMIT")),
        "execution_type":         exec_type_map.get(status, "NEW"),
        "order_status":           status,
        "order_price":            float(o.get("price", 0)),
        "stop_price":             float(o.get("stopPrice", 0)),
        "order_qty":              float(o.get("origQty", 0)),
        "last_fill_price":        0.0,
        "last_fill_qty":          0.0,
        "filled_qty_accumulated": float(o.get("executedQty", 0)),
        "avg_price":              float(o.get("avgPrice", 0)),
        "commission":             0.0,
        "commission_asset":       "",
        "realized_pnl":           0.0,
        "trade_id":               0,
        "event_time_ms":          int(o.get("time", 0)),
        "transaction_time_ms":    int(o.get("updateTime", o.get("time", 0))),
        "position_side":          o.get("positionSide", "BOTH"),
        "is_maker":               0,  # not available in allOrders response; fill-level only
        "is_reduce_only":         1 if o.get("reduceOnly") else 0,
        "time_in_force":          o.get("timeInForce", "GTC"),
    }


def _amendment_to_row(a: Dict[str, Any]) -> dict:
    amendment  = a.get("amendment", {})
    price_info = amendment.get("price", {})
    qty_info   = amendment.get("origQty", {})
    return {
        "amendment_id":    int(a["amendmentId"]),
        "order_id":        int(a["orderId"]),
        "symbol":          a["symbol"],
        "client_order_id": a.get("clientOrderId", ""),
        "time_ms":         int(a["time"]),
        "price_before":    float(price_info.get("before", 0)),
        "price_after":     float(price_info.get("after", 0)),
        "qty_before":      float(qty_info.get("before", 0)),
        "qty_after":       float(qty_info.get("after", 0)),
        "amendment_count": int(amendment.get("count", 0)),
    }


def ws_order_event_to_row(parsed: Dict[str, Any]) -> dict:
    """Map a parsed UserDataWS ORDER_TRADE_UPDATE dict (short Binance keys)
    to an OrderEventDB row dict (long DB column names).

    The parsed dict uses single-letter keys from the Binance WS format:
        s=symbol, i=orderId, x=executionType, X=orderStatus, etc.
    """
    return {
        "order_id":               parsed["i"],
        "symbol":                 parsed["s"],
        "client_order_id":        parsed.get("c", ""),
        "side":                   parsed.get("S", "BUY"),
        "order_type":             parsed.get("o", "LIMIT"),
        "execution_type":         parsed.get("x", "NEW"),
        "order_status":           parsed.get("X", "NEW"),
        "order_price":            float(parsed.get("p", 0)),
        "stop_price":             float(parsed.get("sp", 0)),
        "order_qty":              float(parsed.get("q", 0)),
        "last_fill_price":        float(parsed.get("L", 0)),
        "last_fill_qty":          float(parsed.get("l", 0)),
        "filled_qty_accumulated": float(parsed.get("z", 0)),
        "avg_price":              float(parsed.get("ap", 0)),
        "commission":             float(parsed.get("n", 0)),
        "commission_asset":       parsed.get("N", ""),
        "realized_pnl":           float(parsed.get("rp", 0)),
        "trade_id":               parsed.get("t", 0),
        "event_time_ms":          parsed.get("E", 0),
        "transaction_time_ms":    parsed.get("T", 0),
        "position_side":          parsed.get("ps", "BOTH"),
        "is_maker":               1 if parsed.get("m") else 0,
        "is_reduce_only":         1 if parsed.get("R") else 0,
        "time_in_force":          parsed.get("f", "GTC"),
    }
