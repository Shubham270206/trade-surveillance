"""
Test Wash Trading Detector — Day 9
Runs MixedTradeSimulator and feeds every trade into both detectors.
"""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(__file__))

from manipulators import MixedTradeSimulator
from detection.spoofing import SpoofingDetector
from detection.wash_trading import WashTradingDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


class FullDetectionSimulator(MixedTradeSimulator):
    """Runs both spoofing and wash trading detectors on every trade."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spoof_detector = SpoofingDetector()
        self.wash_detector  = WashTradingDetector()
        self.alerts         = []

    def _on_trade(self, trade):
        super()._on_trade(trade)
        d = trade.to_dict()

        for detector in [self.spoof_detector, self.wash_detector]:
            alert = detector.process(d)
            if alert:
                self.alerts.append(alert)
                print(f"\n{'='*70}")
                print(f"  🚨 {alert['alert_type']} — {alert['trader_id']}")
                print(f"  Confidence: {alert['confidence']:.2%}")
                print(f"  {alert['explanation']}")
                print(f"{'='*70}\n")


if __name__ == "__main__":
    sim = FullDetectionSimulator(
        num_normal     = 10,
        num_spoofers   = 2,
        num_wash_pairs = 2,
    )
    sim.run(trades_per_second=5.0, max_trades=150)

    print(f"\nTotal alerts: {len(sim.alerts)}")
    by_type = {}
    for a in sim.alerts:
        by_type[a["alert_type"]] = by_type.get(a["alert_type"], 0) + 1
    for t, c in by_type.items():
        print(f"  {t}: {c}")

    wash_on_normal = [a for a in sim.alerts
                      if a["alert_type"] == "WASH_TRADING"
                      and "wash" not in a["trader_id"]]
    print(f"  False positives (wash rule on normal traders): {len(wash_on_normal)}")
