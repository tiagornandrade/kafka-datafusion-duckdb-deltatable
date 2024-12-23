import os
import duckdb
import yaml
import logging
import pandas as pd
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

    df = pd.read_parquet(delta_path)

    if 'payload' in df.columns:
        relevant_fields = ['account_id', 'account_type', 'balance', 'currency', 'status',
                           'user_id']
        payload_df = pd.json_normalize(df['payload']).filter(items=relevant_fields)

        df_expanded = pd.concat([df.drop(columns=['payload']), payload_df], axis=1)
    else:
        df_expanded = df

    print(f"Colunas após expansão: {df_expanded.columns}")
    print(tabulate(df_expanded.head(2), headers="keys", tablefmt="pretty"))

    table_columns = [col['name'] for col in schema]
    if len(table_columns) != len(df_expanded.columns):
        print(
            f"Erro: O número de colunas na tabela ({len(table_columns)}) não corresponde ao número de colunas no DataFrame ({len(df_expanded.columns)})")
        return

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f'{col["name"]} {col["type"]}' for col in schema])}
    );
    """
    conn.execute(create_table_sql)

    print(f"Inserindo dados na tabela {table_name}...")
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_expanded")


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