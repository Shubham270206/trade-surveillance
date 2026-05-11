"""
Week 1 Integration Test — Day 7
Verifies the full pipeline: Simulator → Kafka → Postgres
Run with Kafka already running.
"""

import os
import sys
import time
import json
import threading
import logging

sys.path.insert(0, os.path.dirname(__file__))

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer

load_dotenv()

logging.basicConfig(level=logging.WARNING)  # suppress kafka noise during tests

BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC      = os.getenv("KAFKA_TOPIC_TRADES", "raw_trades")
DB         = dict(
    host     = os.getenv("POSTGRES_HOST", "localhost"),
    port     = int(os.getenv("POSTGRES_PORT", 5432)),
    dbname   = os.getenv("POSTGRES_DB", "trades_db"),
    user     = os.getenv("POSTGRES_USER", "surveillance"),
    password = os.getenv("POSTGRES_PASSWORD", "surveillance123"),
)

PASS = "✅ PASS"
FAIL = "❌ FAIL"


# ── Individual tests ──────────────────────────────────────────────────────────

def test_postgres_connection():
    print("1. Postgres connection...", end=" ")
    try:
        conn = psycopg2.connect(**DB)
        conn.close()
        print(PASS)
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_schema_tables():
    print("2. Schema tables exist...", end=" ")
    required = {"trades", "alerts", "trader_scores"}
    try:
        conn = psycopg2.connect(**DB)
        cur  = conn.cursor()
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public';")
        tables = {r[0] for r in cur.fetchall()}
        conn.close()
        missing = required - tables
        if missing:
            print(f"{FAIL} — missing: {missing}")
            return False
        print(PASS)
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_partition_exists():
    print("3. 2026-05 partition exists...", end=" ")
    try:
        conn = psycopg2.connect(**DB)
        cur  = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM pg_tables
            WHERE tablename = 'trades_2026_05';
        """)
        count = cur.fetchone()[0]
        conn.close()
        if count == 0:
            print(f"{FAIL} — partition trades_2026_05 not found")
            return False
        print(PASS)
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_kafka_producer():
    print("4. Kafka producer...", end=" ")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            request_timeout_ms=5000,
        )
        future = producer.send(TOPIC, {"test": "ping"})
        future.get(timeout=5)
        producer.close()
        print(PASS)
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_kafka_consumer():
    print("5. Kafka consumer receives messages...", end=" ")
    received = []

    def consume():
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode()),
                auto_offset_reset="latest",
                consumer_timeout_ms=6000,
                group_id="integration-test-2",
                request_timeout_ms=10000,
            )
            consumer.poll(timeout_ms=3000)  # force partition assignment
            for msg in consumer:
                received.append(msg.value)
                break
            consumer.close()
        except Exception:
            pass

    t = threading.Thread(target=consume)
    t.start()
    time.sleep(3)  # wait longer for group join

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode(),
            request_timeout_ms=10000,
        )
        producer.send(TOPIC, {"test": "integration"})
        producer.flush()
        producer.close()
    except Exception as e:
        print(f"{FAIL} — producer error: {e}")
        return False

    t.join(timeout=8)

    if received:
        print(PASS)
        return True
    else:
        print(f"{FAIL} — no messages received")
        return False


def test_trades_in_db():
    print("6. Trades exist in database...", end=" ")
    try:
        conn = psycopg2.connect(**DB)
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades;")
        count = cur.fetchone()[0]
        conn.close()
        if count == 0:
            print(f"{FAIL} — trades table is empty (run the simulator first)")
            return False
        print(f"{PASS} — {count} trades in DB")
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_trader_scores_seeded():
    print("7. trader_scores seeded...", end=" ")
    try:
        conn = psycopg2.connect(**DB)
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trader_scores;")
        count = cur.fetchone()[0]
        conn.close()
        if count == 0:
            print(f"{FAIL} — trader_scores is empty")
            return False
        print(f"{PASS} — {count} traders")
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_simulator_import():
    print("8. Simulator imports cleanly...", end=" ")
    try:
        from models      import TradeEvent, Side, OrderType
        from simulator   import NormalTrader, TradeSimulator
        from manipulators import Spoofer, WashTrader, MixedTradeSimulator

        trader = NormalTrader("T_test_001")
        trade  = trader.generate_trade()
        assert trade.trader_id == "T_test_001"
        assert trade.quantity  > 0
        assert trade.price     > 0
        print(PASS)
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


def test_trade_serialisation():
    print("9. TradeEvent serialises to dict...", end=" ")
    try:
        from models    import TradeEvent, Side, OrderType
        from simulator import NormalTrader

        trader = NormalTrader("T_test_002")
        trade  = trader.generate_trade()
        d      = trade.to_dict()

        required_keys = {
            "trade_id", "trader_id", "symbol", "side",
            "quantity", "price", "timestamp", "cancelled",
            "cancel_timestamp", "order_type"
        }
        missing = required_keys - set(d.keys())
        if missing:
            print(f"{FAIL} — missing keys: {missing}")
            return False
        print(PASS)
        return True
    except Exception as e:
        print(f"{FAIL} — {e}")
        return False


# ── Runner ────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*50)
    print("  Week 1 Integration Tests — Trade Surveillance")
    print("="*50 + "\n")

    tests   = [
        test_postgres_connection,
        test_schema_tables,
        test_partition_exists,
        test_kafka_producer,
        test_kafka_consumer,
        test_trades_in_db,
        test_trader_scores_seeded,
        test_simulator_import,
        test_trade_serialisation,
    ]

    results = [t() for t in tests]
    passed  = sum(results)
    total   = len(results)

    print(f"\n{'='*50}")
    print(f"  Results: {passed}/{total} passed")
    if passed == total:
        print("  🎉 Week 1 complete — ready to tag v0.1!")
    else:
        print("  ⚠  Fix failing tests before tagging.")
    print("="*50 + "\n")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
