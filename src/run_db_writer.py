"""
DB Writer Runner — Day 6
Consumes from raw_trades topic and writes every trade to Postgres.
Run this alongside run_simulator.py.
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from db_writer import TradeWriter, verify_trades
from kafka_pipeline import consume_trades

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


if __name__ == "__main__":
    writer = TradeWriter()

    try:
        consume_trades(
            on_trade_fn=writer.write,
            max_messages=100,
        )
    finally:
        writer.close()
        verify_trades(limit=10)
