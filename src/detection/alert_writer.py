"""
Alert Writer — Day 8
Writes detection alerts (spoofing, wash trading, etc.) to the alerts table.
"""

import os
import logging
from datetime import datetime
from typing import Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "trades_db"),
        user=os.getenv("POSTGRES_USER", "surveillance"),
        password=os.getenv("POSTGRES_PASSWORD", "surveillance123"),
    )


class AlertWriter:
    """Persists detection alerts to the alerts table and updates trader_scores."""

    def __init__(self):
        self.conn  = get_connection()
        self.count = 0
        log.info("AlertWriter connected to Postgres")

    def write(self, alert: dict) -> None:
        """Insert an alert and bump the trader's risk score."""
        insert_sql = """
            INSERT INTO alerts (
                trader_id, alert_type, confidence,
                explanation, triggered_at, trade_ids
            ) VALUES (
                %(trader_id)s, %(alert_type)s, %(confidence)s,
                %(explanation)s, %(triggered_at)s, %(trade_ids)s::uuid[]
            );
        """
        upsert_score_sql = """
            INSERT INTO trader_scores (trader_id, risk_score, total_flags, last_updated)
            VALUES (%(trader_id)s, %(confidence)s, 1, NOW())
            ON CONFLICT (trader_id) DO UPDATE SET
                risk_score   = GREATEST(trader_scores.risk_score, EXCLUDED.risk_score),
                total_flags  = trader_scores.total_flags + 1,
                last_updated = NOW();
        """
        try:
            ts = _parse_ts(alert.get("triggered_at"))
            with self.conn.cursor() as cur:
                cur.execute(insert_sql, {
                    "trader_id":    alert["trader_id"],
                    "alert_type":   alert["alert_type"],
                    "confidence":   alert["confidence"],
                    "explanation":  alert["explanation"],
                    "triggered_at": ts,
                    "trade_ids":    alert.get("trade_ids", []),
                })
                cur.execute(upsert_score_sql, {
                    "trader_id":  alert["trader_id"],
                    "confidence": alert["confidence"],
                })
            self.conn.commit()
            self.count += 1
            log.info("Alert persisted (#%d): %s — %s",
                      self.count, alert["trader_id"], alert["alert_type"])

        except Exception as e:
            self.conn.rollback()
            log.error("Failed to write alert: %s", e)
            try:
                self.conn = get_connection()
            except Exception as re:
                log.error("Reconnect failed: %s", re)

    def close(self):
        self.conn.close()
        log.info("AlertWriter closed. Total alerts written: %d", self.count)


def _parse_ts(ts_str) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except Exception:
        return None