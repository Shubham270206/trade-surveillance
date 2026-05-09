"""
Kafka-wired simulator entry point — Day 5
Runs MixedTradeSimulator and publishes every trade to raw_trades topic.
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from kafka_pipeline import make_producer, publish_trade
from manipulators import MixedTradeSimulator
from models import TradeEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


class KafkaSimulator(MixedTradeSimulator):
    """MixedTradeSimulator with Kafka producer wired into _on_trade."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.producer = make_producer()
        log.info("Kafka producer connected to raw_trades topic")

    def _on_trade(self, trade: TradeEvent):
        super()._on_trade(trade)               # keep console logging
        publish_trade(self.producer, trade.to_dict())

    def stop(self):
        self.producer.flush()
        self.producer.close()
        log.info("Kafka producer closed.")


if __name__ == "__main__":
    sim = KafkaSimulator(
        num_normal     = 10,
        num_spoofers   = 2,
        num_wash_pairs = 2,
    )
    try:
        sim.run(trades_per_second=3.0, max_trades=100)
    finally:
        sim.stop()
