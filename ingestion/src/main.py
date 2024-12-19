import logging
from consumer.kafka_consumer import consume_messages
from processor.event_processor import process_events
from delta_writer import DeltaWriter
from config.CONST import USER_TOPIC, CONSUMER_CONF, DELTA_TABLE

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting main script execution...")

    messages = consume_messages(USER_TOPIC, CONSUMER_CONF)

    if not messages:
        logger.warning("No messages consumed. Exiting.")
        return

    result = process_events(messages)

    if result.empty:
        logger.warning("Processed result is empty. Exiting.")
        return

    delta_writer = DeltaWriter(DELTA_TABLE)
    delta_writer.write(result, partition_by=["timestamp"], mode="append")


if __name__ == "__main__":
    main()