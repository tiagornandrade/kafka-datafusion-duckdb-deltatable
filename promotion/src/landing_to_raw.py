import os
import duckdb
import yaml
import logging
from datafusion import SessionContext
from deltalake import DeltaTable
from tabulate import tabulate

logger = logging.getLogger(__name__)


def load_config(config_path):
    absolute_path = os.path.abspath(config_path)
    with open(absolute_path, "r") as f:
        return yaml.safe_load(f)


def create_table_in_duckdb(conn, table_name, schema):
    columns = ", ".join([f"{col['name']} {col['type']}" for col in schema])
    conn.execute(f"DROP TABLE IF EXISTS main.{table_name}")
    conn.execute(f"CREATE TABLE IF NOT EXISTS main.{table_name} ({columns})")
    logger.info(f"Created table {table_name} with schema: {schema}")


def process_table(ctx, conn, table_config):
    table_name = table_config["name"]
    delta_path = table_config["delta_path"]
    schema = table_config["schema"]

    logger.info(f"Processing table {table_name} from {delta_path}...")

    delta_table = DeltaTable(delta_path)
    ctx.register_dataset(table_name, delta_table.to_pyarrow_dataset())
    df = ctx.table(table_name).to_pandas()

    create_table_in_duckdb(conn, table_name, schema)

    conn.register(f"df_{table_name}", df)
    conn.execute(f"INSERT INTO main.{table_name} SELECT * FROM df_{table_name}")
    logger.info(f"Data inserted into {table_name}.")

    sample_data = conn.execute(f"SELECT * FROM main.{table_name}").fetchdf().head(5)
    print(tabulate(sample_data, headers="keys", tablefmt="pretty"))


def main():
    config = load_config("./promotion/config/raw/tables_config.yaml")
    ctx = SessionContext()
    conn = duckdb.connect("./database/data_lake.duckdb")

    for table_config in config["tables"]:
        process_table(ctx, conn, table_config)

    conn.close()
    logger.info("All tables processed successfully.")


if __name__ == "__main__":
    main()
