import logging
from consumer.kafka_consumer import consume_messages
from processor.event_processor import process_events
from writer.delta_writer import write_delta_table
from uploader.minio_uploader import upload_to_minio
from config.config import USER_TOPIC, CONSUMER_CONF, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, LOCAL_DELTA_PATH
from minio import Minio

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

    write_delta_table(LOCAL_DELTA_PATH, result, mode='overwrite')

    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    upload_to_minio(LOCAL_DELTA_PATH, minio_client, MINIO_BUCKET, f'raw/{USER_TOPIC}')


if __name__ == '__main__':
    main()
