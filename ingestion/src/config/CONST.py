USER_TOPIC = "user-events"
CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "user-events-group",
    "auto.offset.reset": "earliest",
}

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
MINIO_BUCKET = "delta-lake"
MINIO_DELTA_PATH = f"raw/{USER_TOPIC}"

LOCAL_DELTA_PATH = "/tmp/delta_table"
DELTA_TABLE = f"./database/delta-lake/raw/user-events"
