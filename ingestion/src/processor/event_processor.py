import json
import pandas as pd
import duckdb
import logging

logger = logging.getLogger(__name__)

def process_events(messages):
    logger.info("Processing events...")
    try:
        parsed_messages = []
        for message in messages:
            try:
                parsed_messages.append(json.loads(message))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {message}. Error: {e}")

        if not parsed_messages:
            logger.warning("No valid messages to process. Exiting.")
            return pd.DataFrame()

        df = pd.DataFrame(parsed_messages)
        logger.info(f"Initial parsed DataFrame: {df.head()}")

        if 'uuid' not in df.columns:
            logger.error("Missing required field 'uuid' in messages. Exiting.")
            return pd.DataFrame()

        con = duckdb.connect(database=':memory:', read_only=False)
        con.execute('CREATE TABLE events AS SELECT * FROM df')
        result = con.execute("SELECT * FROM events").fetchdf()
        logger.info(f"Events processed. Total records: {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Error during event processing: {e}")
        raise
