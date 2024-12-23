USER_TOPIC = "user-events"
CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "user-events-group",
    "auto.offset.reset": "earliest",
}

LOCAL_DELTA_PATH = "/tmp/delta_table"
DELTA_TABLE = f"./database/delta-lake/raw/user-events"

DELTA_TABLE_MAPPING = {
    "user-events": "./database/delta-lake/raw/user-events",
}
