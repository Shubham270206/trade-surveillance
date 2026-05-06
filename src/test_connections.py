"""
Day 1 smoke test — confirms Kafka and Postgres are reachable.
Run after `docker compose up -d` and waiting ~20 seconds.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def test_postgres():
    import psycopg2
    print("Testing Postgres...", end=" ")
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trader_scores;")
        count = cur.fetchone()[0]
        conn.close()
        print(f"OK — trader_scores has {count} rows")
        return True
    except Exception as e:
        print(f"FAILED — {e}")
        return False


def test_kafka():
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    print("Testing Kafka...", end=" ")
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            request_timeout_ms=5000,
        )
        producer.send("test_topic", b"hello")
        producer.flush(timeout=5)
        producer.close()
        print("OK — producer connected and sent test message")
        return True
    except NoBrokersAvailable:
        print("FAILED — no brokers available (is Kafka running?)")
        return False
    except Exception as e:
        print(f"FAILED — {e}")
        return False


def test_schema_tables():
    import psycopg2
    print("Testing schema tables...", end=" ")
    expected = {"trades", "alerts", "trader_scores"}
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public';
        """)
        tables = {row[0] for row in cur.fetchall()}
        conn.close()
        found = expected.intersection(tables)
        missing = expected - tables
        if missing:
            print(f"MISSING TABLES: {missing}")
            return False
        print(f"OK — found: {found}")
        return True
    except Exception as e:
        print(f"FAILED — {e}")
        return False


if __name__ == "__main__":
    print("\n=== Day 1 Connection Tests ===\n")
    results = [
        test_postgres(),
        test_kafka(),
        test_schema_tables(),
    ]
    print()
    if all(results):
        print("All tests passed. Day 1 complete — commit!")
    else:
        print("Some tests failed. Check Docker logs with: docker compose logs")