from confluent_kafka import Consumer
import logging

logger = logging.getLogger(__name__)


def consume_messages(topic, consumer_config):
    logger.info(f"Starting to consume messages from topic '{topic}'...")
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    messages = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                logger.error(f"Error consuming message: {msg.error()}")
                continue
            messages.append(msg.value().decode("utf-8"))
    except KeyboardInterrupt:
        logger.info("User interrupted. Closing consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed successfully.")

    logger.info(f"Consumed {len(messages)} messages.")
    return messages
