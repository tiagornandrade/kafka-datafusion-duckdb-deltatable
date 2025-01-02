import unittest
import duckdb
from io import StringIO
from ingestion.src.main import create_table_in_duckdb


class TestDuckDBOperations(unittest.TestCase):
    def setUp(self):
        """Configura a conexão antes de cada teste"""
        self.conn = duckdb.connect(":memory:")
        self.table_name = "test_table"
        self.schema = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
        ]
        self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")

    def tearDown(self):
        """Fecha a conexão após cada teste"""
        self.conn.close()

    def test_connection(self):
        """Testa se a conexão ao banco de dados foi bem-sucedida"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)
        except Exception as e:
            self.fail(f"Falha na conexão: {e}")

    def test_create_table(self):
        """Testa se a tabela foi criada com sucesso"""
        create_table_in_duckdb(self.conn, self.table_name, self.schema)

        result = self.conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}'"
        ).fetchdf()
        self.assertEqual(len(result), 1)

    def test_table_schema(self):
        """Testa se a tabela foi criada com o esquema correto"""
        create_table_in_duckdb(self.conn, self.table_name, self.schema)

        result = self.conn.execute(f"PRAGMA table_info({self.table_name})").fetchdf()
        columns = set(result["name"].tolist())

        expected_columns = {"id", "name"}
        self.assertTrue(expected_columns.issubset(columns))

    def test_insert_data(self):
        """Testa se a inserção de dados na tabela funciona"""
        create_table_in_duckdb(self.conn, self.table_name, self.schema)

        self.conn.execute(
            f"INSERT INTO {self.table_name} (id, name) VALUES (1, 'Test')"
        )

        result = self.conn.execute(f"SELECT * FROM {self.table_name}").fetchdf()
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["id"], 1)
        self.assertEqual(result.iloc[0]["name"], "Test")


if __name__ == "__main__":
    unittest.main()
