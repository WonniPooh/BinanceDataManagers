"""
Shared Binance IP rate-limit — fixed-window tracker.

Binance Futures hard limit: 2400 weight/minute per IP (rolling window).
aggTrades endpoint costs 20 weight each.

We use a fixed 20-second window capped at 750 weight (= 2250/min, ≈ 94% of
the hard limit), leaving headroom for other processes sharing the IP.  When
the window fills, acquire() sleeps until the window resets — no busy-loops,
no arbitrary back-off durations.

As a safety backstop, check_backpressure() reads the used weight reported by
Binance (supplied by the caller from the X-MBX-USED-WEIGHT-1M header) and
sleeps until the next minute boundary + 2 s when ≥ 2200 is seen — enough for the rolling 1-minute window to
fully roll over.

Usage:
    from binance_rate_limiter import bnx_limiter, AGGTRADES_WEIGHT
    await bnx_limiter.acquire(AGGTRADES_WEIGHT)
    async with session.get(...) as resp:
        raw = resp.headers.get("X-MBX-USED-WEIGHT-1M")
        if raw:
            bnx_limiter.record_used_weight(int(raw))
    await bnx_limiter.check_backpressure()
"""
from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger("collector.binance_rate_limiter")

AGGTRADES_WEIGHT = 20      # cost of one /fapi/v1/aggTrades request
IP_WEIGHT_LIMIT  = 2400    # Binance rolling 1-minute window limit

WINDOW_S     = 20    # fixed window size in seconds
WINDOW_LIMIT = 750   # max weight per window (= 2250/min, ≈ 94% headroom)

_CRIT_THRESHOLD = 2200   # Binance-reported weight that triggers 60s backstop


class _BinanceWeightLimiter:
    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None
        self._window_start = time.monotonic()
        self._window_used  = 0
        self._reported     = 0   # last X-MBX-USED-WEIGHT-1M value from Binance

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def acquire(self, weight: int = AGGTRADES_WEIGHT) -> None:
        """Block until the current window has capacity for `weight`, then deduct it.

        If the window is full, sleeps until it resets and retries.
        """
        while True:
            async with self._get_lock():
                now = time.monotonic()
                if now - self._window_start >= WINDOW_S:
                    self._window_start = now
                    self._window_used  = 0
                if self._window_used + weight <= WINDOW_LIMIT:
                    self._window_used += weight
                    return
                wait = (self._window_start + WINDOW_S) - now
            logger.debug("rate-limit window full (used=%d/%d) — waiting %.2fs",
                         self._window_used, WINDOW_LIMIT, wait)
            await asyncio.sleep(wait)

    def record_used_weight(self, used: int) -> None:
        """Store the Binance-reported used weight (X-MBX-USED-WEIGHT-1M header value)."""
        self._reported = used

    async def check_backpressure(self) -> None:
        """Safety backstop: sleep until the next minute boundary if Binance
        reports weight >= 2200.

        Binance uses a rolling 1-minute window.  Sleeping until the next
        calendar minute + 2 s buffer lets most of the used weight roll off
        without the full 60 s penalty.
        """
        if self._reported >= _CRIT_THRESHOLD:
            now = time.time()
            sleep_s = 60 - (now % 60) + 2  # seconds until next :00 + buffer
            logger.warning(
                "Binance reported weight %d/%d ≥ %d — sleeping %.0fs to let window roll over",
                self._reported, IP_WEIGHT_LIMIT, _CRIT_THRESHOLD, sleep_s,
            )
            await asyncio.sleep(sleep_s)

    @property
    def used_weight(self) -> int:
        return self._reported


# Process-level singleton — import this everywhere.
bnx_limiter = _BinanceWeightLimiter()
