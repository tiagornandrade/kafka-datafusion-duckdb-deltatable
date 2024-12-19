import duckdb
import pandas as pd
import tabulate
import logging
from datafusion import SessionContext
from deltalake import DeltaTable

logger = logging.getLogger(__name__)
ctx = SessionContext()
delta_table_path = "./database/delta-lake/raw/user-events"

delta_table = DeltaTable(delta_table_path)
ctx.register_dataset("user_events", delta_table.to_pyarrow_dataset())
df_raw_user_events = ctx.table("user_events")

df_raw_user_events_pandas = df_raw_user_events.to_pandas()

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

conn.register("df_raw_user_events", df_raw_user_events_pandas)
conn.execute("INSERT INTO main.raw_user_events SELECT * FROM df_raw_user_events")

logger.info("Finish insert data...")

df1 = conn.execute("SELECT * FROM main.raw_user_events").fetchdf()

print(tabulate.tabulate(df1, headers="keys", tablefmt="pretty"))

conn.close()