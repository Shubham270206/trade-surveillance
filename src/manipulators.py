"""
Manipulative Trader Patterns — Day 3
Spoofer: places large orders, cancels 85%+ within 200ms to fake demand.
WashTrader: executes circular buy/sell between coordinated accounts.
"""

import random
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

from models import TradeEvent, Side, OrderType, TraderType
from simulator import get_market_price, NormalTrader, TradeSimulator

log = logging.getLogger(__name__)


# ── Spoofer ─────────────────────────────────────────────────────────────────

class Spoofer:
    """
    Simulates a spoofing trader.

    Pattern:
    - Places large orders (90th+ percentile size: 3000–8000 shares)
    - Cancels 85%+ of them within 50–200ms (looks like HFT noise)
    - Occasionally lets a small real order execute to profit from moved price
    - Focuses on a single symbol to maximise price impact
    """

    CANCEL_RATE    = 0.87          # 87% of orders get cancelled
    MIN_CANCEL_MS  = 50            # cancel between 50ms
    MAX_CANCEL_MS  = 200           # and 200ms
    MIN_QTY        = 3000          # large orders to move price
    MAX_QTY        = 8000

    def __init__(self, trader_id: str):
        self.trader_id   = trader_id
        self.trader_type = TraderType.SPOOFER
        self.symbol      = random.choice(["AAPL", "MSFT", "GOOGL", "AMZN"])
        # Spoofers typically push price one direction
        self.spoof_side  = random.choice([Side.BUY, Side.SELL])

    def generate_spoof_order(self) -> TradeEvent:
        """Large order, usually cancelled within 200ms."""
        price    = get_market_price(self.symbol)
        quantity = random.randint(self.MIN_QTY, self.MAX_QTY)

        # Place on same side to create illusion of demand/supply
        spread_pct = random.uniform(0.001, 0.003)
        if self.spoof_side == Side.BUY:
            price = round(price * (1 - spread_pct), 2)
        else:
            price = round(price * (1 + spread_pct), 2)

        now   = datetime.now(timezone.utc)
        trade = TradeEvent(
            trader_id  = self.trader_id,
            symbol     = self.symbol,
            side       = self.spoof_side,
            quantity   = quantity,
            price      = price,
            order_type = OrderType.LIMIT,
            timestamp  = now,
        )

        # 87% chance of cancellation within 200ms
        if random.random() < self.CANCEL_RATE:
            cancel_ms              = random.randint(self.MIN_CANCEL_MS, self.MAX_CANCEL_MS)
            trade.cancelled        = True
            trade.cancel_timestamp = now + timedelta(milliseconds=cancel_ms)

        return trade

    def generate_real_order(self) -> TradeEvent:
        """Small real order on opposite side — the actual profit trade."""
        price    = get_market_price(self.symbol)
        quantity = random.randint(50, 300)          # small real fill
        real_side = Side.SELL if self.spoof_side == Side.BUY else Side.BUY

        return TradeEvent(
            trader_id  = self.trader_id,
            symbol     = self.symbol,
            side       = real_side,
            quantity   = quantity,
            price      = round(price, 2),
            order_type = OrderType.MARKET,
            timestamp  = datetime.now(timezone.utc),
            cancelled  = False,
        )

    def generate_burst(self) -> List[TradeEvent]:
        """
        Generate a spoofing burst:
        3–6 large spoof orders, then 1 small real order.
        This is the classic spoofing sequence.
        """
        trades = []
        num_spoof_orders = random.randint(3, 6)
        for _ in range(num_spoof_orders):
            trades.append(self.generate_spoof_order())
        # Real profit order at the end
        trades.append(self.generate_real_order())
        return trades

    def __repr__(self):
        return f"Spoofer({self.trader_id}, symbol={self.symbol}, side={self.spoof_side.value})"


# ── Wash Trader ──────────────────────────────────────────────────────────────

class WashTrader:
    """
    Simulates wash trading between two coordinated accounts.

    Pattern:
    - Two trader IDs controlled by the same entity
    - One buys, the other sells, same symbol, same price ±0.1%
    - Creates artificial volume to make a stock look active
    - Trades happen within 1–3 seconds of each other
    """

    def __init__(self, trader_id_a: str, trader_id_b: str):
        self.trader_id_a = trader_id_a
        self.trader_id_b = trader_id_b
        self.trader_type = TraderType.WASH_TRADER
        self.symbol      = random.choice(["BAC", "GS", "JPM", "TSLA"])
        self.quantity    = random.randint(500, 2000)   # consistent size

    def generate_wash_pair(self) -> Tuple[TradeEvent, TradeEvent]:
        """
        Generate a matched buy/sell pair between two coordinated accounts.
        Returns (buy_trade, sell_trade).
        """
        price     = get_market_price(self.symbol)
        now       = datetime.now(timezone.utc)

        # Tiny price difference to avoid exact match detection
        buy_price  = round(price * random.uniform(0.999, 1.001), 2)
        sell_price = round(price * random.uniform(0.999, 1.001), 2)

        # Slight time offset — sell comes 1–3 seconds after buy
        sell_delay = timedelta(seconds=random.uniform(1, 3))

        buy_trade = TradeEvent(
            trader_id  = self.trader_id_a,
            symbol     = self.symbol,
            side       = Side.BUY,
            quantity   = self.quantity,
            price      = buy_price,
            order_type = OrderType.LIMIT,
            timestamp  = now,
            cancelled  = False,
        )

        sell_trade = TradeEvent(
            trader_id  = self.trader_id_b,
            symbol     = self.symbol,
            side       = Side.SELL,
            quantity   = self.quantity,
            price      = sell_price,
            order_type = OrderType.LIMIT,
            timestamp  = now + sell_delay,
            cancelled  = False,
        )

        return buy_trade, sell_trade

    def __repr__(self):
        return (
            f"WashTrader({self.trader_id_a} ↔ {self.trader_id_b}, "
            f"symbol={self.symbol})"
        )


# ── Extended Simulator ───────────────────────────────────────────────────────

class MixedTradeSimulator(TradeSimulator):
    """
    Extends TradeSimulator with manipulative traders injected into the stream.
    Manipulative events appear randomly mixed with normal trading activity.
    """

    def __init__(
        self,
        num_normal:      int = 10,
        num_spoofers:    int = 2,
        num_wash_pairs:  int = 2,
    ):
        super().__init__(num_normal=num_normal)

        self.spoofers = [
            Spoofer(f"T_spoofer_{i:03d}")
            for i in range(num_spoofers)
        ]
        self.wash_traders = [
            WashTrader(f"T_wash_{i:03d}a", f"T_wash_{i:03d}b")
            for i in range(num_wash_pairs)
        ]

        log.info("Added %d spoofers:", len(self.spoofers))
        for s in self.spoofers:
            log.info("  %s", s)
        log.info("Added %d wash trader pairs:", len(self.wash_traders))
        for w in self.wash_traders:
            log.info("  %s", w)

    def next_trade(self):
        """
        90% of the time emit a normal trade.
        7% of the time emit a spoofing burst.
        3% of the time emit a wash trading pair.
        """
        roll = random.random()

        if roll < 0.90:
            # Normal trade
            return super().next_trade()

        elif roll < 0.97:
            # Spoofing burst — return trades one at a time
            spoofer = random.choice(self.spoofers)
            burst   = spoofer.generate_burst()
            # Emit the first trade now, queue the rest
            self._pending = getattr(self, "_pending", [])
            self._pending.extend(burst[1:])
            return burst[0]

        else:
            # Wash trading pair
            wash  = random.choice(self.wash_traders)
            buy_t, sell_t = wash.generate_wash_pair()
            self._pending = getattr(self, "_pending", [])
            self._pending.append(sell_t)
            return buy_t

    def run(self, trades_per_second: float = 5.0, max_trades: int = None):
        """Override run to drain pending queue between normal trades."""
        self._pending = []
        interval      = 1.0 / trades_per_second
        log.info("Starting mixed simulator at %.1f trades/sec", trades_per_second)

        try:
            while True:
                # Drain any pending trades first (burst remainder)
                if self._pending:
                    trade = self._pending.pop(0)
                else:
                    trade = self.next_trade()

                self._on_trade(trade)
                self.trade_count += 1

                if max_trades and self.trade_count >= max_trades:
                    log.info("Reached max_trades=%d, stopping.", max_trades)
                    break

                time.sleep(interval)

        except KeyboardInterrupt:
            log.info("Simulator stopped after %d trades.", self.trade_count)

    def _on_trade(self, trade: TradeEvent):
        """Log with trader type highlighted."""
        status = "CANCELLED" if trade.cancelled else "ACTIVE  "

        # Label manipulative traders clearly in the log
        tag = ""
        if "spoofer" in trade.trader_id:
            tag = " ⚠ SPOOF"
        elif "wash" in trade.trader_id:
            tag = " ⚠ WASH "

        log.info(
            "[%s] %s %s %5d @ $%7.2f  %-6s  %s%s",
            status,
            trade.trader_id,
            trade.side.value,
            trade.quantity,
            trade.price,
            trade.symbol,
            trade.trade_id[:8],
            tag,
        )


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sim = MixedTradeSimulator(
        num_normal     = 10,
        num_spoofers   = 2,
        num_wash_pairs = 2,
    )
    sim.run(trades_per_second=3.0, max_trades=80)
