"""
Kafka Pipeline — Day 5
Producer: pushes trade events to raw_trades topic.
Consumer: reads from raw_trades, logs each event (detection engine hook ready).
"""

import json
import os
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

load_dotenv()

log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRADES      = os.getenv("KAFKA_TOPIC_TRADES", "raw_trades")
TOPIC_ALERTS      = os.getenv("KAFKA_TOPIC_ALERTS", "trade_alerts")


# ── Producer ─────────────────────────────────────────────────────────────────

def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                  # wait for leader acknowledgement
        retries=3,
        request_timeout_ms=10_000,
    )


def publish_trade(producer: KafkaProducer, trade_dict: dict) -> None:
    """Send a single trade event to raw_trades topic."""
    try:
        future = producer.send(TOPIC_TRADES, value=trade_dict)
        future.get(timeout=5)        # block until confirmed
    except KafkaError as e:
        log.error("Failed to publish trade %s: %s", trade_dict.get("trade_id"), e)


# ── Consumer ─────────────────────────────────────────────────────────────────

def make_consumer(group_id: str = "detection-engine") -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_TRADES,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",   # only read new messages
        enable_auto_commit=True,
        consumer_timeout_ms=1000,     # stop iteration if no messages for 1s
    )


def consume_trades(on_trade_fn=None, max_messages: int = None):
    """
    Consume from raw_trades topic.
    on_trade_fn: optional callback(trade_dict) — detection engine hooks in here.
    max_messages: stop after N messages (None = run forever).
    """
    consumer    = make_consumer()
    count       = 0
    log.info("Consumer started. Listening on topic: %s", TOPIC_TRADES)

    try:
        while True:
            for message in consumer:
                trade = message.value
                count += 1

                # Log the incoming trade
                log.info(
                    "[CONSUMED #%d] %s %s %d @ $%.2f  %s",
                    count,
                    trade.get("trader_id"),
                    trade.get("side"),
                    trade.get("quantity"),
                    trade.get("price"),
                    trade.get("symbol"),
                )

                # Hook for detection engine (Day 8+)
                if on_trade_fn:
                    on_trade_fn(trade)

                if max_messages and count >= max_messages:
                    log.info("Reached max_messages=%d, stopping consumer.", max_messages)
                    return

    except KeyboardInterrupt:
        log.info("Consumer stopped after %d messages.", count)
    finally:
        consumer.close()
