import json
import logging
import duckdb
import pandas as pd
import re
import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


def save_invalid_messages(messages):
    logger.warning("Saving invalid messages for future analysis.")
    with open("invalid_messages.log", "a") as f:
        for message in messages:
            f.write(f"{json.dumps(message)}\n")


def validate_event(event):
    try:
        datetime.datetime.strptime(event["event_timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        if "event_uuid" in event and "event_type" in event and "payload" in event:
            return True
        logger.warning(f"Event missing required fields: {event}")
        return False
    except (ValueError, KeyError) as e:
        logger.warning(f"Invalid event detected: {e}. Event: {event}")
        return False


def create_table_if_not_exists(con, table_name):
    if not table_name:
        logger.error(
            f"Invalid table name: {table_name}. Ensure the mapping is correct."
        )
        raise ValueError("Table name cannot be None or empty.")

    logger.info(f"Ensuring table '{table_name}' exists...")
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_uuid VARCHAR,
            event_type VARCHAR,
            event_timestamp TIMESTAMP,
            payload JSON,
            timestamp TIMESTAMP
        )
    """
    )
    logger.info(f"Table '{table_name}' is ready.")


def process_in_batches(messages, batch_size):
    for i in range(0, len(messages), batch_size):
        yield messages[i : i + batch_size]


def process_events(messages, table_mapping, batch_size=1000):
    logger.info("Starting event processing...")
    con = duckdb.connect(database="./database/events.duckdb", read_only=False)
    valid_events_by_type = defaultdict(list)

    for message in messages:
        if isinstance(message, list):
            logger.warning(
                f"Encountered a list in messages: {message}. Iterating through elements."
            )
            for sub_message in message:
                process_single_message(sub_message, valid_events_by_type)
        else:
            process_single_message(message, valid_events_by_type)

    if not valid_events_by_type:
        logger.info("No valid events to process.")
        return {}

    for event_type, events in valid_events_by_type.items():
        table_name = f"user_events_{event_type}"
        create_table_if_not_exists(con, table_name)

        for batch in process_in_batches(events, batch_size):
            df = pd.DataFrame(batch)

            if "timestamp" not in df.columns and "event_timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["event_timestamp"]).dt.floor("D")

            table_schema = con.execute(f"DESCRIBE {table_name}").fetchall()
            table_columns = [col[0] for col in table_schema]

            df = df[table_columns]

            logger.info(
                f"Processing batch with {len(df)} records for table '{table_name}'."
            )
            con.register("batch", df)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM batch")
            logger.info(f"Batch processed for table '{table_name}'.")

    con.close()
    logger.info("All events processed successfully.")
    return {
        event_type: pd.DataFrame(events)
        for event_type, events in valid_events_by_type.items()
    }


def process_single_message(message, valid_events_by_type):
    try:
        message = parse_message(message)
        if message and validate_event(message):
            event_type = message.get("event_type")
            if event_type:
                valid_events_by_type[event_type].append(message)
            else:
                logger.error(f"Event is missing 'event_type': {message}. Skipping.")
                save_invalid_messages([message])
        else:
            save_invalid_messages([message])
    except Exception as e:
        logger.warning(f"Unexpected error processing message: {e}. Message: {message}")
        save_invalid_messages([message])


def parse_message(message):
    try:
        return json.loads(message) if isinstance(message, str) else message
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON message: {message}. Skipping.")
        return None
