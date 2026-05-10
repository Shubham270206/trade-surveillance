"""
Database Writer — Day 6
Consumes from raw_trades Kafka topic and persists every trade to Postgres.
"""

import os
import logging
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from kafka_pipeline import consume_trades

load_dotenv()

log = logging.getLogger(__name__)

# ── Connection ───────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "trades_db"),
        user=os.getenv("POSTGRES_USER", "surveillance"),
        password=os.getenv("POSTGRES_PASSWORD", "surveillance123"),
    )


# ── Writer ───────────────────────────────────────────────────────────────────

class TradeWriter:
    """
    Persists trade events to the trades table.
    Maintains a single long-lived Postgres connection with auto-reconnect.
    """

    def __init__(self):
        self.conn  = get_connection()
        self.count = 0
        log.info("TradeWriter connected to Postgres (trades_db)")

    def write(self, trade: dict) -> None:
        """Insert a single trade event into the trades table."""
        sql = """
            INSERT INTO trades (
                trade_id, trader_id, symbol, side,
                quantity, price, timestamp,
                cancelled, cancel_timestamp, order_type
            ) VALUES (
                %(trade_id)s, %(trader_id)s, %(symbol)s, %(side)s,
                %(quantity)s, %(price)s, %(timestamp)s,
                %(cancelled)s, %(cancel_timestamp)s, %(order_type)s
            )
            ON CONFLICT (trade_id, timestamp) DO NOTHING;
        """
        try:
            # Parse timestamp strings back to datetime objects
            trade["timestamp"] = _parse_ts(trade.get("timestamp"))
            trade["cancel_timestamp"] = _parse_ts(trade.get("cancel_timestamp"))

            with self.conn.cursor() as cur:
                cur.execute(sql, trade)
            self.conn.commit()
            self.count += 1

            if self.count % 10 == 0:
                log.info("Persisted %d trades to Postgres", self.count)

        except psycopg2.errors.UniqueViolation:
            self.conn.rollback()
            log.debug("Duplicate trade skipped: %s", trade.get("trade_id"))

        except Exception as e:
            self.conn.rollback()
            log.error("Failed to write trade %s: %s", trade.get("trade_id"), e)
            # Attempt reconnect
            try:
                self.conn = get_connection()
                log.info("Reconnected to Postgres")
            except Exception as re:
                log.error("Reconnect failed: %s", re)

    def close(self):
        self.conn.close()
        log.info("TradeWriter closed. Total trades written: %d", self.count)


def _parse_ts(ts_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string to datetime, return None if missing."""
    if not ts_str:
        return None
    try:
        # Handle both with and without timezone info
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str)
    except Exception:
        return None


# ── Verification query ───────────────────────────────────────────────────────

def verify_trades(limit: int = 5) -> None:
    """Print the most recent trades from Postgres for verification."""
    conn = get_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT trader_id, symbol, side, quantity, price,
                   cancelled, timestamp
            FROM trades
            ORDER BY timestamp DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()

    conn.close()

    print(f"\n=== Last {limit} trades in Postgres ===")
    for row in rows:
        cancelled = " [CANCELLED]" if row["cancelled"] else ""
        print(
            f"  {row['trader_id']:<20} {row['side']:<4} "
            f"{row['quantity']:>5} @ ${row['price']:<8.2f} "
            f"{row['symbol']:<6}{cancelled}"
        )
    print()
