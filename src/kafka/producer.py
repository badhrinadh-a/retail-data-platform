"""
Kafka Producer — Retail Event Generator

Continuously produces synthetic retail events for customers, orders,
inventory, and payments to Kafka topics.
Uses structured logging for production observability.
"""

import json
import os
import random
import time
import uuid
from typing import Any, Dict

from confluent_kafka import Producer
from faker import Faker

from src.shared.logging_config import get_logger

logger = get_logger(__name__)
fake = Faker()

KAFKA_BROKER = os.getenv("KAFKA_BROKER_EXTERNAL", "localhost:9092")
TOPICS = {
    "orders": "orders",
    "customers": "customers",
    "inventory": "inventory",
    "payments": "payments",
}


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        logger.error(
            "Message delivery failed",
            extra={"error": str(err), "topic": msg.topic()},
        )
    else:
        logger.debug(
            "Message delivered",
            extra={"topic": msg.topic(), "partition": msg.partition()},
        )


def generate_customer() -> Dict[str, Any]:
    return {
        "customer_id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "created_at": time.time(),
    }


def generate_order(customer_id: str) -> Dict[str, Any]:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "amount": round(random.uniform(10.0, 500.0), 2),
        "status": random.choice(["PENDING", "COMPLETED", "CANCELLED"]),
        "created_at": time.time(),
    }


def generate_inventory() -> Dict[str, Any]:
    return {
        "item_id": str(uuid.uuid4()),
        "sku": fake.ean(length=13),
        "quantity": random.randint(0, 100),
        "location": fake.city(),
        "updated_at": time.time(),
    }


def generate_payment(order_id: str) -> Dict[str, Any]:
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id": order_id,
        "method": random.choice(["CREDIT_CARD", "PAYPAL", "BITCOIN"]),
        "status": random.choice(["SUCCESS", "FAILED"]),
        "processed_at": time.time(),
    }


def main():
    logger.info("Initializing Kafka producer", extra={"broker": KAFKA_BROKER})
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    message_count = 0
    logger.info("Starting data generation loop")
    try:
        while True:
            # Generate Entities
            customer = generate_customer()
            order = generate_order(customer["customer_id"])
            inventory = generate_inventory()
            payment = generate_payment(order["order_id"])

            # Produce to topics
            producer.produce(
                TOPICS["customers"],
                json.dumps(customer).encode("utf-8"),
                callback=delivery_report,
            )
            producer.produce(
                TOPICS["orders"],
                json.dumps(order).encode("utf-8"),
                callback=delivery_report,
            )
            producer.produce(
                TOPICS["inventory"],
                json.dumps(inventory).encode("utf-8"),
                callback=delivery_report,
            )
            producer.produce(
                TOPICS["payments"],
                json.dumps(payment).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)
            message_count += 4

            # Log periodic summary instead of per-message noise
            if message_count % 100 == 0:
                logger.info(
                    "Producer progress",
                    extra={"messages_produced": message_count},
                )

            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        logger.info(
            "Stopping generation",
            extra={"total_messages_produced": message_count},
        )
    finally:
        producer.flush()
        logger.info("Producer flushed and shut down cleanly")


if __name__ == "__main__":
    main()
