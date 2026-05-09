"""
Consumer runner — Day 5
Run this in a separate terminal while the simulator is running.
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from kafka_pipeline import consume_trades

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)

if __name__ == "__main__":
    consume_trades(max_messages=100)
