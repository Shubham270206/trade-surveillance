"""
Test Spoofing Detector — Day 8
Runs the MixedTradeSimulator and feeds every trade into SpoofingDetector
to confirm spoofers get caught and normal traders don't.
"""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(__file__))

from manipulators import MixedTradeSimulator
from detection.spoofing import SpoofingDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


class DetectingSimulator(MixedTradeSimulator):
    """Simulator that runs every trade through the spoofing detector."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.detector     = SpoofingDetector()
        self.alerts_fired = []

    def _on_trade(self, trade):
        super()._on_trade(trade)
        alert = self.detector.process(trade.to_dict())
        if alert:
            self.alerts_fired.append(alert)
            print(f"\n{'='*70}")
            print(f"  🚨 ALERT: {alert['alert_type']} — {alert['trader_id']}")
            print(f"  Confidence: {alert['confidence']:.2%}")
            print(f"  {alert['explanation']}")
            print(f"{'='*70}\n")


if __name__ == "__main__":
    sim = DetectingSimulator(
        num_normal     = 10,
        num_spoofers   = 2,
        num_wash_pairs = 2,
    )

    sim.run(trades_per_second=5.0, max_trades=150)

    print(f"\nTotal alerts fired: {len(sim.alerts_fired)}")
    spoofer_alerts = [a for a in sim.alerts_fired if "spoofer" in a["trader_id"]]
    normal_alerts  = [a for a in sim.alerts_fired if "normal" in a["trader_id"]]
    print(f"  Correctly flagged spoofers: {len(spoofer_alerts)}")
    print(f"  False positives (normal traders flagged): {len(normal_alerts)}")
