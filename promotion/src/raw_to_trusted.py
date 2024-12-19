import os
import duckdb
import pandas as pd
import yaml
import logging
from tabulate import tabulate

logger = logging.getLogger(__name__)

def load_config(config_path):
    absolute_path = os.path.abspath(config_path)
    with open(absolute_path, "r") as f:
        return yaml.safe_load(f)

def rename_columns(df, rename_map):
    return df.rename(columns=rename_map, inplace=False)

def create_table_in_duckdb(conn, table_name, schema):
    columns = ", ".join([f"{col['name']} {col['type']}" for col in schema])
    conn.execute(f"DROP TABLE IF EXISTS main.{table_name}")
    conn.execute(f"CREATE TABLE IF NOT EXISTS main.{table_name} ({columns})")
    logger.info(f"Created table {table_name} with schema: {schema}")

def process_table(conn, table_config):
    raw_table = table_config["raw_table"]
    trusted_table = table_config["trusted_table"]
    rename_map = table_config["rename_columns"]
    schema = table_config["schema"]

    logger.info(f"Processing raw table {raw_table} to trusted table {trusted_table}...")

    df_raw = conn.execute(f"SELECT * FROM main.{raw_table}").fetchdf()

    df_transformed = rename_columns(df_raw, rename_map)
    if "event_timestamp" in df_transformed.columns:
        df_transformed["event_timestamp"] = pd.to_datetime(df_transformed["event_timestamp"], unit="s")

    create_table_in_duckdb(conn, trusted_table, schema)

    conn.register(f"df_{trusted_table}", df_transformed)
    conn.execute(f"INSERT INTO main.{trusted_table} SELECT * FROM df_{trusted_table}")
    logger.info(f"Data inserted into trusted table {trusted_table}.")

    sample_data = conn.execute(f"SELECT * FROM main.{trusted_table}").fetchdf().head(5)
    print(tabulate(sample_data, headers="keys", tablefmt="pretty"))

def main():
    config = load_config("./promotion/config/trusted/tables_config.yaml")
    conn = duckdb.connect("./database/data_lake.duckdb")

    for table_config in config["tables"]:
        process_table(conn, table_config)

    conn.close()
    logger.info("All tables processed successfully.")

if __name__ == "__main__":
    main()