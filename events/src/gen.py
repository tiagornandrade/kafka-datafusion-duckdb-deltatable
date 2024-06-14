import json
import datetime
from faker import Faker
from confluent_kafka import Producer
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_TOPIC = 'user-events'
producer = Producer({'bootstrap.servers': 'localhost:9092'})

fake = Faker()


def gen_event():
    return {
        "uuid": str(fake.uuid4()),
        "name": fake.name(),
        "city": fake.city(),
        "timestamp": datetime.datetime.now().timestamp()
    }


def gen_events(n):
    return [gen_event() for _ in range(n)]


def produce_events(events):
    logger.info(f"Producing {len(events)} events to topic '{USER_TOPIC}'...")
    for event in events:
        event_json = json.dumps(event)
        producer.produce(USER_TOPIC, key=b'user_key', value=event_json.encode('utf-8'))
    producer.flush()
    logger.info("Events production completed.")


def main():
    logging.info("Starting event generation and production script...")

    events = gen_events(1000)
    produce_events(events)

    logging.info("Script execution completed.")


if __name__ == "__main__":
    main()
