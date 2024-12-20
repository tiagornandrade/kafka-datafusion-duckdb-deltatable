import json
import datetime
import random
from faker import Faker
from confluent_kafka import Producer
import logging
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_TOPIC = "user-events"
producer = Producer({"bootstrap.servers": "localhost:9092"})

fake = Faker()

def gen_event():
    """Gera um evento com estrutura correta para inserção."""
    event = {
        "event_uuid": str(uuid.uuid4()),
        "event_type": random.choice(["created", "deleted", "updated"]),
        "event_timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "payload": {
            "account_id": str(uuid.uuid4()),
            "account_type": random.choice(["personal", "business"]),
            "balance": random.randint(0, 1000),
            "currency": random.choice(["USD", "EUR", "BRL"]),
            "status": random.choice(["active", "inactive"]),
            "user_id": str(uuid.uuid4()),
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        },
    }
    return event

def gen_events(n):
    return {i: gen_event() for i in range(n)}

def produce_events(events):
    logger.info(f"Producing {len(events)} events to topic '{USER_TOPIC}'...")
    for _, event in events.items():
        event_json = json.dumps(event)
        producer.produce(USER_TOPIC, key=b"user_key", value=event_json.encode("utf-8"))
    producer.flush()
    logger.info("Events production completed.")

def main():
    logger.info("Starting event generation and production script...")

    events = gen_events(2)
    produce_events(events)

    logger.info("Script execution completed.")

if __name__ == "__main__":
    main()