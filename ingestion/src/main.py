import os
import yaml
import logging
import pandas as pd
from consumer.kafka_consumer import consume_messages
from processor.event_processor import process_events
from writer.delta_writer import DeltaWriter
from config.CONST import USER_TOPIC, CONSUMER_CONF

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def load_delta_table_mapping(mapping_file):
    try:
        logger.info(f"Attempting to load Delta table mapping from: {mapping_file}")
        with open(mapping_file, "r") as file:
            return yaml.safe_load(file).get("event_table_mapping", {})
    except FileNotFoundError:
        logger.error(f"Delta table mapping file not found: {mapping_file}")
    except Exception as e:
        logger.error(f"Failed to load delta table mapping file: {e}")
    return {}


def get_table_path(event_type, table_mapping):
    event_mapping = table_mapping.get("user-events", {})
    return event_mapping.get(event_type)


def truncate_timestamp_to_day(df):
    if isinstance(df, pd.Series):
        df = df.to_frame().T

    if isinstance(df, pd.DataFrame):
        if "timestamp" not in df.columns:
            logger.info(
                "Creating 'timestamp' field in DataFrame from 'event_timestamp'..."
            )
            if "event_timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["event_timestamp"]).dt.floor("D")
            else:
                logger.warning(
                    "Neither 'timestamp' nor 'event_timestamp' found in DataFrame."
                )
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("D")
    else:
        logger.error("Invalid type for 'df'. Expected DataFrame or Series.")
    return df


def main():
    logger.info("Starting main script execution...")

    mapping_file = "ingestion/src/config/delta_table_mapping.yaml"
    delta_table_mapping = load_delta_table_mapping(mapping_file)

    if not delta_table_mapping:
        logger.error("Delta table mapping is empty. Exiting.")
        return

    try:
        messages = consume_messages(USER_TOPIC, CONSUMER_CONF)
        if not messages:
            logger.warning("No messages consumed. Exiting.")
            return

        if not isinstance(messages, list):
            logger.info(f"Messages are in {type(messages)} format, converting to list.")
            messages = list(messages)

        processed_results = process_events(messages, table_mapping=delta_table_mapping)
        if not processed_results:
            logger.warning("No valid results processed. Exiting.")
            return

        for event_type, df in processed_results.items():
            if df.empty:
                logger.warning(
                    f"No data to write for event type '{event_type}'. Skipping."
                )
                continue

            delta_table_path = get_table_path(event_type, delta_table_mapping)

            if not delta_table_path:
                logger.warning(
                    f"No table mapping found for event type '{event_type}'. Skipping."
                )
                continue

            df = truncate_timestamp_to_day(df)

            logger.info(
                f"Writing data for event type '{event_type}' to path: {delta_table_path}"
            )

            try:
                delta_writer = DeltaWriter(delta_table_path)
                delta_writer.write(df, partition_by=["timestamp"], mode="overwrite")
            except Exception as e:
                logger.error(
                    f"Failed to write data for event type '{event_type}' to Delta table: {e}"
                )

        logger.info("All events processed and written to Delta tables successfully.")

    except Exception as e:
        logger.error(f"An error occurred during script execution: {e}", exc_info=True)


if __name__ == "__main__":
    main()