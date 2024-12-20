from confluent_kafka import Consumer
import logging

logger = logging.getLogger(__name__)


def consume_messages(topic, consumer_config, batch_size=100, persist_path=None):
    logger.info(f"Starting to consume messages from topic '{topic}'...")
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    try:
        while True:
            messages = []
            for _ in range(batch_size):
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    break
                if msg.error():
                    logger.error(f"Error consuming message: {msg.error()}")
                    continue
                messages.append(msg.value().decode("utf-8"))

            if not messages:
                break

            logger.info(f"Consumed batch of {len(messages)} messages.")

            if persist_path:
                persist_messages(messages, persist_path)

            yield messages
    except KeyboardInterrupt:
        logger.info("User interrupted. Closing consumer...")
    except Exception as e:
        logger.error(f"Unexpected error during consumption: {e}")
        raise
    finally:
        consumer.close()
        logger.info("Consumer closed successfully.")


def persist_messages(messages, filepath="consumed_messages.jsonl"):
    with open(filepath, "a") as f:
        for message in messages:
            f.write(f"{message}\n")
    logger.info(f"Persisted {len(messages)} messages to {filepath}.")