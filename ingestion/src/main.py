import logging
from consumer.kafka_consumer import consume_messages
from processor.event_processor import process_events
from delta_writer import DeltaWriter
from config.CONST import USER_TOPIC, CONSUMER_CONF, DELTA_TABLE_MAPPING

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting main script execution...")

    messages = consume_messages(USER_TOPIC, CONSUMER_CONF)

    if not messages:
        logger.warning("No messages consumed. Exiting.")
        return

    processed_results = process_events(messages, table_mapping=DELTA_TABLE_MAPPING)

    if not processed_results:
        logger.warning("No events processed. Exiting.")
        return

    for event_type, df in processed_results.items():
        if df.empty:
            logger.warning(f"No data to write for event type '{event_type}'. Skipping.")
            continue

        delta_table_path = DELTA_TABLE_MAPPING.get(event_type)
        if not delta_table_path:
            logger.warning(f"No Delta table path found for event type '{event_type}'. Skipping.")
            continue

        delta_writer = DeltaWriter(delta_table_path)
        delta_writer.write(df, partition_by=["timestamp"], mode="append")

    logger.info("All events processed and written to Delta tables successfully.")


if __name__ == "__main__":
    main()