import os
import dlt
import duckdb
import yaml
import logging
from rich.logging import RichHandler
import pandas as pd
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[RichHandler()])
logger = logging.getLogger(__name__)

CONFIG_PATH = "./ingestion/src/config/tables_config.yaml"
DUCKDB_DATABASE_PATH = "./database/data_lake.duckdb"


def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_table_in_duckdb(conn, table_name, schema):
    columns = ", ".join([f"{col['name']} {col['type']}" for col in schema])
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} ({columns})")
    logger.info(f"Table {table_name} successfully created in DuckDB.")


@dlt.source(name="delta_source")
def read_delta_table(table_config):
    delta_path = table_config["delta_path"]
    df = pd.read_parquet(delta_path)
    logger.info(f"Data read from Delta Table at {delta_path}.")
    yield dlt.resource(df, name=table_config["name"])


@dlt.resource
def transform_table(delta_resource, table_config):
    schema = table_config["schema"]
    relevant_fields = table_config.get("relevant_fields", [])

    for df in delta_resource:
        for column in df.columns:
            if isinstance(df[column].dtype, pd.CategoricalDtype):
                df[column] = df[column].astype(str)

        if "payload" in df.columns:
            payload_df = pd.json_normalize(df["payload"]).filter(items=relevant_fields)
            df_expanded = pd.concat([df.drop(columns=["payload"]), payload_df], axis=1)
        else:
            df_expanded = df

        df_expanded = df_expanded[[col["name"] for col in schema]]
        yield df_expanded


def run_landing_to_raw_pipeline():
    config = load_config(CONFIG_PATH)
    logger.info("Configuration loaded successfully.")

    pipeline = dlt.pipeline(
        pipeline_name="delta_to_duckdb_pipeline",
        destination="duckdb",
        dataset_name="promotion_data",
        dev_mode=True,
    )
    logger.info("Pipeline initialized.")

    conn = duckdb.connect(DUCKDB_DATABASE_PATH)

    for table_config in config["tables"]:
        table_name = table_config["name"]
        schema = table_config["schema"]

        create_table_in_duckdb(conn, table_name, schema)

        delta_resource = read_delta_table(table_config)
        transformed_resource = transform_table(delta_resource, table_config)
        load_info = pipeline.run(transformed_resource)

        logger.info(f"Data from table {table_name} processed and saved to DuckDB.")

    conn.close()
    logger.info("Connection to DuckDB closed.")
