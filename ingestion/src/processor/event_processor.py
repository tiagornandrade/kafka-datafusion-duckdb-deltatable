import json
import pandas as pd
import duckdb
import logging
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


def save_invalid_messages(messages):
    logger.warning("Saving invalid messages for future analysis.")
    with open("invalid_messages.log", "a") as f:
        for message in messages:
            f.write(f"{message}\n")


def create_table_if_not_exists(con, table_name):
    logger.info(f"Ensuring table '{table_name}' exists...")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_uuid STRING,
            event_timestamp TIMESTAMP,
            event_type STRING,
            payload JSON
        )
    """)
    logger.info(f"Table '{table_name}' is ready.")


def process_events(messages, table_mapping, batch_size=1000):
    logger.info("Processing events...")
    try:
        valid_messages = defaultdict(list)
        invalid_messages = []

        for message in messages:
            try:
                parsed_message = json.loads(message)
                for key, payload in parsed_message.items():
                    event_type = payload.get("event_type", "unknown_event")
                    valid_messages[event_type].append({
                        "event_uuid": str(uuid.uuid4()),
                        "event_timestamp": datetime.utcnow().isoformat(),
                        "event_type": event_type,
                        "payload": json.dumps(payload),
                    })
            except json.JSONDecodeError as e:
                invalid_messages.append(message)

        if invalid_messages:
            save_invalid_messages(invalid_messages)

        if not valid_messages:
            logger.warning("No valid messages to process. Exiting.")
            return

        con = duckdb.connect(database="events.duckdb", read_only=False)

        for event_type, records in valid_messages.items():
            table_name = table_mapping.get(event_type, f"default_{event_type}")
            create_table_if_not_exists(con, table_name)

            for batch in process_in_batches(records, batch_size):
                df = pd.DataFrame(batch)
                logger.info(f"Processing batch with {len(df)} records for table '{table_name}'.")
                con.register("batch", df)
                con.execute(f"INSERT INTO {table_name} SELECT * FROM batch")
                logger.info(f"Batch processed for table '{table_name}'.")

        con.close()
        logger.info("All events processed successfully.")
    except Exception as e:
        logger.error(f"Error during event processing: {e}")
        raise


def process_in_batches(messages, batch_size):
    for i in range(0, len(messages), batch_size):
        yield messages[i:i + batch_size]
