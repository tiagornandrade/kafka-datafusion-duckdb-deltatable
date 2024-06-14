import os
import duckdb
import pandas as pd
from minio import Minio
from confluent_kafka import Consumer
from deltalake.writer import write_deltalake
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_TOPIC = 'user-events'
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'user-events-group',
    'auto.offset.reset': 'earliest'
}

MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minio_access_key'
MINIO_SECRET_KEY = 'minio_secret_key'
MINIO_BUCKET = 'deltalake'
MINIO_DELTA_PATH = 'delta_table'


def consume_messages(topic, consumer_config):
    logger.info(f"Starting consuming messages from topic '{topic}'...")
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
            messages.append(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        logger.info("User interrupted. Closing consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed successfully.")

    logger.info(f"Consumed {len(messages)} messages.")
    return messages


def process_events(messages):
    logger.info("Processing events...")
    df = pd.DataFrame(messages, columns=['message'])
    df['message'] = df['message'].astype('int32')
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute('CREATE TABLE events AS SELECT * FROM df')
    result = con.execute("SELECT * FROM events").fetchdf()
    logger.info(f"Events processed. Total records: {len(result)}")
    return result


def write_delta_table(local_path, data, mode='overwrite'):
    logger.info(f"Writing DeltaTable to {local_path}...")
    write_deltalake(local_path, data, mode=mode)
    logger.info("DeltaTable written successfully.")


def upload_to_minio(local_path, minio_client, bucket_name, minio_path):
    logger.info(f"Starting upload of DeltaTable to MinIO ({MINIO_ENDPOINT})...")
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logger.info(f"Created bucket '{bucket_name}' on MinIO.")

    for root, _, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            minio_file = os.path.join(minio_path, relative_path).replace("\\", "/")
            minio_client.fput_object(bucket_name, minio_file, local_file)
            logger.info(f"File '{minio_file}' uploaded to MinIO.")

    logger.info(f'DeltaTable stored on MinIO at {minio_path}')


def main():
    logging.info("Starting main script execution...")

    messages = consume_messages(USER_TOPIC, consumer_conf)
    result = process_events(messages)

    local_delta_path = '/tmp/delta_table'
    write_delta_table(local_delta_path, result, mode='overwrite')

    minio_client = Minio(MINIO_ENDPOINT,
                         access_key=MINIO_ACCESS_KEY,
                         secret_key=MINIO_SECRET_KEY,
                         secure=False)

    upload_to_minio(local_delta_path, minio_client, MINIO_BUCKET, MINIO_DELTA_PATH)

    logging.info("Main script execution completed.")


if __name__ == "__main__":
    main()
