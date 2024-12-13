import duckdb
import pandas as pd
import tabulate
import logging
from deltalake import DeltaTable

logger = logging.getLogger(__name__)
delta_table_path = "./database/delta-lake/raw/user-events"

delta_table = DeltaTable(delta_table_path)
df = delta_table.to_pandas()

conn = duckdb.connect("./database/data_lake.duckdb")

logger.info("Starting create table...")
conn.execute("DROP TABLE IF EXISTS main.raw_user_events")
conn.execute("""
    CREATE TABLE IF NOT EXISTS main.raw_user_events (
        uuid STRING,
        name STRING,
        city STRING,
        country STRING,
        is_deleted BOOLEAN,
        timestamp DOUBLE
    )
""")

conn.execute("INSERT INTO main.raw_user_events SELECT * FROM delta_scan('./database/delta-lake/raw/user-events')")

logger.info("Finish insert data...")

df1 = conn.execute("SELECT * FROM main.raw_user_events").fetchdf()

print(tabulate.tabulate(df1, headers="keys", tablefmt="pretty"))

conn.close()
