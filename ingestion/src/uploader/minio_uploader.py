import os
from config.config import MINIO_ENDPOINT
import logging

logger = logging.getLogger(__name__)

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
