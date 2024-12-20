import logging
import pandas as pd
from consumer.kafka_consumer import consume_messages
from processor.event_processor import process_events
from writer.delta_writer import DeltaWriter
from config.CONST import USER_TOPIC, CONSUMER_CONF, DELTA_TABLE_MAPPING

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def truncate_timestamp_to_day(df):
    if isinstance(df, pd.Series):
        df = df.to_frame().T
    if isinstance(df, pd.DataFrame):
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("D")
        else:
            logger.warning("Column 'timestamp' not found in DataFrame. Skipping truncation.")
    else:
        logger.error("Invalid type for 'df'. Expected DataFrame or Series.")
    return df

def main():
    logger.info("Starting main script execution...")

    try:
        messages = consume_messages(USER_TOPIC, CONSUMER_CONF)

        if not messages:
            logger.warning("No messages consumed. Exiting.")
            return

        if not isinstance(messages, list):
            logger.info(f"Messages are in {type(messages)} format, converting to list.")
            messages = list(messages)

        processed_results = process_events(messages, table_mapping=DELTA_TABLE_MAPPING)

        if not processed_results:
            logger.warning("No valid results processed. Exiting.")
            return

        for event_type, df in processed_results.items():
            if df.empty:
                logger.warning(f"No data to write for event type '{event_type}'. Skipping.")
                continue

            df = truncate_timestamp_to_day(df)

            delta_table_path = DELTA_TABLE_MAPPING.get(event_type)
            if not delta_table_path:
                logger.warning(f"No Delta table path found for event type '{event_type}'. Skipping.")
                continue

            delta_writer = DeltaWriter(delta_table_path)
            delta_writer.write(df, partition_by=["timestamp"], mode="overwrite")

        logger.info("All events processed and written to Delta tables successfully.")

    except Exception as e:
        logger.error(f"An error occurred during script execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()