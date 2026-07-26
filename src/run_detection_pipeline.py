"""
DB Writer + Detection Runner — Day 9
Consumes from raw_trades, writes trades to Postgres, runs spoofing
and wash trading detection, writes alerts to the alerts table.
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from db_writer import TradeWriter, verify_trades
from kafka_pipeline import consume_trades
from detection.spoofing import SpoofingDetector
from detection.wash_trading import WashTradingDetector
from detection.alert_writer import AlertWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


if __name__ == "__main__":
    trade_writer   = TradeWriter()
    alert_writer   = AlertWriter()
    spoof_detector = SpoofingDetector()
    wash_detector  = WashTradingDetector()

    def on_trade(trade: dict):
        trade_writer.write(trade)
        for detector in [spoof_detector, wash_detector]:
            alert = detector.process(trade)
            if alert:
                alert_writer.write(alert)

    try:
        consume_trades(
            on_trade_fn=on_trade,
            max_messages=150,
        )
    finally:
        trade_writer.close()
        alert_writer.close()
        verify_trades(limit=10)
