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
        datetime.datetime.strptime(event["event_timestamp"], '%Y-%m-%d %H:%M:%S.%f')
        if "event_uuid" in event and "event_type" in event and "payload" in event:
            return True
        logger.warning(f"Event missing required fields: {event}")
        return False
    except (ValueError, KeyError) as e:
        logger.warning(f"Invalid event detected: {e}. Event: {event}")
        return False

def create_table_if_not_exists(con, table_name):
    logger.info(f"Ensuring table '{table_name}' exists...")
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_uuid VARCHAR,
            event_type VARCHAR,
            event_timestamp TIMESTAMP,
            payload JSON
        )
    """)
    logger.info(f"Table '{table_name}' is ready.")

def process_in_batches(messages, batch_size):
    for i in range(0, len(messages), batch_size):
        yield messages[i:i + batch_size]

def process_events(messages, table_mapping, batch_size=1000):
    logger.info("Starting event processing...")

    con = duckdb.connect(database="./database/events.duckdb", read_only=False)
    valid_events_by_type = defaultdict(list)

    for message in messages:
        if isinstance(message, list):
            logger.warning(f"Encountered a list: {message}. Iterating through elements.")
            for sub_message in message:
                try:
                    sub_message = json.loads(sub_message) if isinstance(sub_message, str) else sub_message
                    if validate_event(sub_message):
                        event_type = sub_message.get("event_type")
                        valid_events_by_type[event_type].append(sub_message)
                    else:
                        save_invalid_messages([sub_message])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Invalid sub-message: {sub_message}. Skipping.")
                    save_invalid_messages([sub_message])
        else:
            try:
                message = json.loads(message) if isinstance(message, str) else message
                if validate_event(message):
                    event_type = message.get("event_type")
                    valid_events_by_type[event_type].append(message)
                else:
                    save_invalid_messages([message])
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Skipping invalid message: {message}. Expected a valid JSON string.")
                save_invalid_messages([message])

    if not valid_events_by_type:
        logger.info("No valid events to process.")
        return {}

    for event_type, events in valid_events_by_type.items():
        table_name = table_mapping.get(event_type)
        create_table_if_not_exists(con, table_name)

        for batch in process_in_batches(events, batch_size):
            df = pd.DataFrame(batch)
            logger.info(f"Processing batch with {len(df)} records for table '{table_name}'.")
            con.register("batch", df)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM batch")
            logger.info(f"Batch processed for table '{table_name}'.")

    con.close()
    logger.info("All events processed successfully.")

    return {event_type: pd.DataFrame(events) for event_type, events in valid_events_by_type.items()}