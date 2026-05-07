"""
Trade Simulator — Day 2
Generates realistic normal trader activity.
Normal traders place orders with realistic sizing, timing, and low cancellation rates.
"""

import random
import time
import logging
from datetime import datetime, timezone
from typing import List

from models import TradeEvent, Side, OrderType, TraderType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Market configuration ────────────────────────────────────────────────────

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "JPM", "BAC", "GS"]

# Realistic mid-prices per symbol
BASE_PRICES: dict[str, float] = {
    "AAPL":  182.50,
    "GOOGL": 141.80,
    "MSFT":  415.20,
    "AMZN":  178.90,
    "TSLA":  245.60,
    "JPM":   198.30,
    "BAC":    38.75,
    "GS":    465.10,
}

# Price walk state — shared across all traders so prices drift realistically
_price_state: dict[str, float] = dict(BASE_PRICES)


def get_market_price(symbol: str) -> float:
    """Return current simulated market price with small random walk."""
    drift    = random.gauss(0, 0.02)          # ±2% std dev per tick
    _price_state[symbol] *= (1 + drift / 100)
    _price_state[symbol]  = max(_price_state[symbol], 1.0)
    return round(_price_state[symbol], 2)


# ── Normal Trader ───────────────────────────────────────────────────────────

class NormalTrader:
    """
    Simulates a legitimate market participant.

    Behaviour:
    - Places orders with realistic sizes (50–2000 shares)
    - Cancellation rate ~10% (vs 85%+ for spoofers)
    - Time-to-cancel when cancelled: 2–30 seconds (vs <200ms for spoofers)
    - Roughly balanced buy/sell ratio
    - Trades 1–3 symbols they "specialise" in
    """

    def __init__(self, trader_id: str):
        self.trader_id   = trader_id
        self.trader_type = TraderType.NORMAL
        # Each normal trader focuses on 1–3 symbols
        self.symbols     = random.sample(SYMBOLS, k=random.randint(1, 3))
        # Slight directional bias (some traders are net buyers, some net sellers)
        self.buy_bias    = random.uniform(0.4, 0.6)

    def generate_trade(self) -> TradeEvent:
        symbol   = random.choice(self.symbols)
        price    = get_market_price(symbol)
        side     = Side.BUY if random.random() < self.buy_bias else Side.SELL

        # Realistic order sizing — mostly small, occasionally larger
        quantity = int(random.gauss(250, 150))   # ~50–2000
        quantity = max(10, min(quantity, 5000))

        # Small spread around mid-price for limit orders
        spread_pct = random.uniform(0.001, 0.005)
        if side == Side.BUY:
            price = round(price * (1 - spread_pct), 2)   # bid below mid
        else:
            price = round(price * (1 + spread_pct), 2)   # ask above mid

        trade = TradeEvent(
            trader_id  = self.trader_id,
            symbol     = symbol,
            side       = side,
            quantity   = quantity,
            price      = price,
            order_type = OrderType.LIMIT,
            timestamp  = datetime.now(timezone.utc),
        )

        # ~10% cancellation rate, with realistic delay
        if random.random() < 0.10:
            cancel_delay_secs          = random.uniform(2, 30)
            trade.cancelled            = True
            trade.cancel_timestamp     = datetime(
                *datetime.now(timezone.utc).timetuple()[:6],
                tzinfo=timezone.utc,
            )

        return trade

    def __repr__(self):
        return f"NormalTrader({self.trader_id}, symbols={self.symbols})"


# ── Simulator ───────────────────────────────────────────────────────────────

class TradeSimulator:
    """
    Manages a pool of traders and emits a continuous stream of TradeEvents.
    """

    def __init__(self, num_normal: int = 10):
        self.traders: List[NormalTrader] = [
            NormalTrader(f"T_normal_{i:03d}")
            for i in range(num_normal)
        ]
        self.trade_count = 0
        log.info("Simulator initialised with %d normal traders", len(self.traders))
        for t in self.traders:
            log.info("  %s", t)

    def next_trade(self) -> TradeEvent:
        """Pick a random trader and generate their next trade."""
        trader = random.choice(self.traders)
        trade  = trader.generate_trade()
        self.trade_count += 1
        return trade

    def run(self, trades_per_second: float = 5.0, max_trades: int = None):
        """
        Run the simulator loop.
        trades_per_second: target throughput
        max_trades: stop after N trades (None = run forever)
        """
        interval = 1.0 / trades_per_second
        log.info("Starting simulator at %.1f trades/sec", trades_per_second)

        try:
            while True:
                trade = self.next_trade()
                self._on_trade(trade)

                if max_trades and self.trade_count >= max_trades:
                    log.info("Reached max_trades=%d, stopping.", max_trades)
                    break

                time.sleep(interval)

        except KeyboardInterrupt:
            log.info("Simulator stopped by user after %d trades.", self.trade_count)

    def _on_trade(self, trade: TradeEvent):
        """Hook — override or extend to add Kafka producer here (Day 5)."""
        status = "CANCELLED" if trade.cancelled else "ACTIVE  "
        log.info(
            "[%s] %s %s %4d @ $%7.2f  %-6s  %s",
            status,
            trade.trader_id,
            trade.side.value,
            trade.quantity,
            trade.price,
            trade.symbol,
            trade.trade_id[:8],
        )


# ── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sim = TradeSimulator(num_normal=10)
    sim.run(trades_per_second=3.0, max_trades=50)
