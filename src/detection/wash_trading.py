"""
Wash Trading Detection Rule — Day 9
Detects circular buy/sell pairs between coordinated accounts.

Wash trading criteria:
  - Two trades on the same symbol within WINDOW_SECONDS
  - One BUY, one SELL
  - Quantities match within QUANTITY_TOLERANCE
  - Prices match within PRICE_TOLERANCE_PCT
  - Different trader IDs (but could be same entity)
"""

import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

WINDOW_SECONDS      = 5      # look for matching trades within 5 seconds
PRICE_TOLERANCE_PCT = 0.005  # prices must be within 0.5% of each other
QUANTITY_TOLERANCE  = 0.02   # quantities must match within 2%
MIN_QUANTITY        = 100    # ignore tiny orders (noise)
COOLDOWN_SECS       = 15     # avoid re-alerting same pair too quickly


def _parse_ts(ts_str) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except Exception:
        return None


class WashTradingDetector:
    """
    Stateful detector — call .process(trade_dict) for every incoming trade.
    Maintains a rolling window of recent trades per symbol and looks for
    matching opposing pairs that indicate wash trading.
    """

    def __init__(self):
        # symbol -> deque of recent trade dicts
        self._windows: dict[str, deque] = defaultdict(deque)
        # frozenset({trader_a, trader_b}) -> last alert time
        self._cooldowns: dict[frozenset, datetime] = {}

    def process(self, trade: dict) -> Optional[dict]:
        """
        Process one trade. Returns alert dict if wash trading detected, else None.
        """
        if trade.get("cancelled"):
            return None  # wash trades don't get cancelled

        quantity = trade.get("quantity", 0)
        if quantity < MIN_QUANTITY:
            return None

        symbol    = trade["symbol"]
        now       = _parse_ts(trade["timestamp"]) or datetime.now(timezone.utc)
        window    = self._windows[symbol]

        # Evict stale trades
        cutoff = now.timestamp() - WINDOW_SECONDS
        while window and window[0].get("_ts", 0) < cutoff:
            window.popleft()

        # Store timestamp as float for fast comparison
        trade_copy = dict(trade)
        trade_copy["_ts"] = now.timestamp()
        window.append(trade_copy)

        # Look for a matching opposing trade in the window
        return self._find_wash_pair(trade, window, now)

    def _find_wash_pair(self, incoming: dict, window: deque,
                        now: datetime) -> Optional[dict]:
        incoming_side = incoming["side"]
        opposite_side = "SELL" if incoming_side == "BUY" else "BUY"
        incoming_qty  = incoming["quantity"]
        incoming_price = incoming["price"]
        incoming_trader = incoming["trader_id"]

        for candidate in window:
            if candidate["trade_id"] == incoming["trade_id"]:
                continue  # skip self

            if candidate["side"] != opposite_side:
                continue

            if candidate["trader_id"] == incoming_trader:
                continue  # same trader — not wash trading (just normal activity)

            # Check quantity match
            cand_qty = candidate["quantity"]
            qty_diff = abs(incoming_qty - cand_qty) / max(incoming_qty, cand_qty)
            if qty_diff > QUANTITY_TOLERANCE:
                continue

            # Check price match
            cand_price = candidate["price"]
            price_diff = abs(incoming_price - cand_price) / max(incoming_price, cand_price)
            if price_diff > PRICE_TOLERANCE_PCT:
                continue

            # ── Match found ───────────────────────────────────────────────
            pair_key = frozenset({incoming_trader, candidate["trader_id"]})

            # Cooldown check
            last_alert = self._cooldowns.get(pair_key)
            if last_alert:
                elapsed = (now - last_alert).total_seconds()
                if elapsed < COOLDOWN_SECS:
                    return None

            self._cooldowns[pair_key] = now

            # Time delta between the two trades
            delta_secs = abs(now.timestamp() - candidate["_ts"])

            confidence = self._compute_confidence(qty_diff, price_diff, delta_secs)

            explanation = (
                f"Flagged as WASH TRADING: {incoming_trader} and "
                f"{candidate['trader_id']} executed opposing {incoming['symbol']} "
                f"trades within {delta_secs:.1f}s. "
                f"Quantities: {incoming_qty} vs {cand_qty} "
                f"(diff: {qty_diff:.1%}). "
                f"Prices: ${incoming_price:.2f} vs ${cand_price:.2f} "
                f"(diff: {price_diff:.1%})."
            )

            log.warning("WASH TRADING ALERT: %s ↔ %s — %s",
                        incoming_trader, candidate["trader_id"], explanation)

            return {
                "trader_id":    incoming_trader,
                "alert_type":   "WASH_TRADING",
                "confidence":   round(confidence, 4),
                "explanation":  explanation,
                "triggered_at": now.isoformat(),
                "trade_ids":    [incoming["trade_id"], candidate["trade_id"]],
            }

        return None

    def _compute_confidence(self, qty_diff: float, price_diff: float,
                            delta_secs: float) -> float:
        """
        Higher confidence when:
        - quantities match more exactly (lower qty_diff)
        - prices match more exactly (lower price_diff)
        - trades are closer in time (lower delta_secs)
        """
        qty_score   = 1.0 - (qty_diff / QUANTITY_TOLERANCE)
        price_score = 1.0 - (price_diff / PRICE_TOLERANCE_PCT)
        time_score  = max(0.0, 1.0 - (delta_secs / WINDOW_SECONDS))

        return round(
            0.4 * qty_score + 0.4 * price_score + 0.2 * time_score,
            4
        )
