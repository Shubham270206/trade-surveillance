"""
Spoofing Detection Rule — Day 8
Detects spoofing patterns using a rolling 5-second window per trader.

Spoofing criteria (all three must be met):
  1. Cancellation rate > 80% in the window
  2. Average time-to-cancel < 300ms
  3. Average order size > 90th percentile of all orders seen
"""

import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import numpy as np

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

WINDOW_SECONDS       = 5       # rolling window size
MIN_CANCEL_RATE      = 0.80    # 80% cancellation threshold
MAX_CANCEL_MS        = 300     # 300ms max time-to-cancel
SIZE_PERCENTILE      = 90      # order size must exceed this percentile
MIN_ORDERS_TO_JUDGE  = 3       # need at least this many orders to fire


# ── Data structures ───────────────────────────────────────────────────────────

class OrderRecord:
    """Lightweight record of a single order event."""
    __slots__ = ["trade_id", "trader_id", "symbol", "quantity",
                 "timestamp", "cancelled", "cancel_ms"]

    def __init__(self, trade: dict):
        self.trade_id  = trade["trade_id"]
        self.trader_id = trade["trader_id"]
        self.symbol    = trade["symbol"]
        self.quantity  = trade["quantity"]
        self.timestamp = _parse_ts(trade["timestamp"])
        self.cancelled = trade.get("cancelled", False)

        # Time-to-cancel in milliseconds
        cancel_ts = _parse_ts(trade.get("cancel_timestamp"))
        if self.cancelled and cancel_ts and self.timestamp:
            delta_ms = (cancel_ts - self.timestamp).total_seconds() * 1000
            self.cancel_ms = max(0.0, delta_ms)
        else:
            self.cancel_ms = None


# ── Size percentile tracker ───────────────────────────────────────────────────

class SizePercentileTracker:
    """
    Maintains a rolling sample of order sizes to compute live percentiles.
    Keeps last 1000 orders globally across all traders.
    """

    def __init__(self, max_samples: int = 1000):
        self._sizes     = deque(maxlen=max_samples)

    def add(self, quantity: int):
        self._sizes.append(quantity)

    def percentile(self, p: float) -> float:
        if not self._sizes:
            return 0.0
        return float(np.percentile(list(self._sizes), p))


# ── Spoofing Detector ─────────────────────────────────────────────────────────

class SpoofingDetector:
    """
    Stateful detector — call .process(trade_dict) for every incoming trade.
    Returns an alert dict if spoofing is detected, else None.
    """

    def __init__(self):
        # Per-trader rolling window of OrderRecords
        self._windows: dict[str, deque] = defaultdict(
            lambda: deque()
        )
        self._size_tracker = SizePercentileTracker()
        self._alert_cooldown: dict[str, datetime] = {}  # avoid alert storms
        self._COOLDOWN_SECS = 10

    def process(self, trade: dict) -> Optional[dict]:
        """
        Process one trade event.
        Returns alert dict if spoofing detected, else None.
        """
        record = OrderRecord(trade)

        # Only learn "normal" sizing from orders that were NOT cancelled.
        # Otherwise spoofers' own huge cancelled orders inflate the
        # percentile threshold and the rule can never fire (self-poisoning).
        if not record.cancelled:
            self._size_tracker.add(record.quantity)

        trader_id = record.trader_id
        window    = self._windows[trader_id]
        now       = record.timestamp or datetime.now(timezone.utc)

        # Add to window
        window.append(record)

        # Evict records older than WINDOW_SECONDS
        cutoff = now.timestamp() - WINDOW_SECONDS
        while window and window[0].timestamp and \
              window[0].timestamp.timestamp() < cutoff:
            window.popleft()

        # Need minimum orders to make a judgement
        if len(window) < MIN_ORDERS_TO_JUDGE:
            return None

        # Check cooldown — don't re-alert the same trader too quickly
        last_alert = self._alert_cooldown.get(trader_id)
        if last_alert:
            elapsed = (now - last_alert).total_seconds()
            if elapsed < self._COOLDOWN_SECS:
                return None

        return self._evaluate(trader_id, window, now)

    def _evaluate(self, trader_id: str, window: deque,
                  now: datetime) -> Optional[dict]:
        orders    = list(window)
        total     = len(orders)
        cancelled = [o for o in orders if o.cancelled]
        n_cancel  = len(cancelled)

        # ── Criterion 1: cancellation rate ───────────────────────────────────
        cancel_rate = n_cancel / total
        if cancel_rate <= MIN_CANCEL_RATE:
            return None

        # ── Criterion 2: average time-to-cancel ──────────────────────────────
        cancel_times = [o.cancel_ms for o in cancelled if o.cancel_ms is not None]
        if not cancel_times:
            return None
        avg_cancel_ms = sum(cancel_times) / len(cancel_times)
        if avg_cancel_ms >= MAX_CANCEL_MS:
            return None

        # ── Criterion 3: order size above 90th percentile ────────────────────
        avg_size      = sum(o.quantity for o in orders) / total
        size_threshold = self._size_tracker.percentile(SIZE_PERCENTILE)
        if avg_size <= size_threshold:
            return None

        # ── All criteria met — generate alert ────────────────────────────────
        self._alert_cooldown[trader_id] = now
        confidence = self._compute_confidence(cancel_rate, avg_cancel_ms, avg_size,
                                              size_threshold)
        trade_ids  = [o.trade_id for o in orders]
        symbols    = list({o.symbol for o in orders})

        explanation = (
            f"Flagged as SPOOFING: {n_cancel} of {total} orders cancelled "
            f"within window. Cancellation rate: {cancel_rate:.1%}. "
            f"Avg time-to-cancel: {avg_cancel_ms:.0f}ms "
            f"(threshold: {MAX_CANCEL_MS}ms). "
            f"Avg order size: {avg_size:.0f} "
            f"(90th pct: {size_threshold:.0f}). "
            f"Symbols: {', '.join(symbols)}."
        )

        log.warning("SPOOFING ALERT: %s — %s", trader_id, explanation)

        return {
            "trader_id":   trader_id,
            "alert_type":  "SPOOFING",
            "confidence":  round(confidence, 4),
            "explanation": explanation,
            "triggered_at": now.isoformat(),
            "trade_ids":   trade_ids,
        }

    def _compute_confidence(self, cancel_rate: float, avg_cancel_ms: float,
                            avg_size: float, size_threshold: float) -> float:
        """
        Confidence score 0–1 based on how far each metric exceeds the threshold.
        Simple weighted average of three normalised signals.
        """
        # How much above 80% is the cancel rate? (cap at 1.0)
        c1 = min((cancel_rate - MIN_CANCEL_RATE) / (1.0 - MIN_CANCEL_RATE), 1.0)

        # How far below 300ms is the cancel time? (faster = more suspicious)
        c2 = min((MAX_CANCEL_MS - avg_cancel_ms) / MAX_CANCEL_MS, 1.0)
        c2 = max(c2, 0.0)

        # How far above 90th pct is the size?
        if size_threshold > 0:
            c3 = min((avg_size - size_threshold) / size_threshold, 1.0)
            c3 = max(c3, 0.0)
        else:
            c3 = 0.5

        return round(0.4 * c1 + 0.4 * c2 + 0.2 * c3, 4)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(ts_str) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except Exception:
        return None
