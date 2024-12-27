import duckdb

from src.dbtbasic.backend.base_backend import BaseBackend


class DuckDBBackend(BaseBackend):
    def __init__(self):
        self.conn = duckdb.connect('file.db')

    def create_schema(self, schema_name: str) -> None:
        self.conn.sql(f'CREATE SCHEMA if not exists {schema_name};')

    def create_view(self, schema_name: str, table_name: str, query: str) -> None:
        string = f'drop view if exists {schema_name}.{table_name} cascade; create view {schema_name}.{table_name} as ({query});'
        self.conn.sql(string)

    def create_table(self, schema_name: str, table_name: str, query: str) -> None:
        string = f'create table if not exists {schema_name}.{table_name} as ({query});'
        self.conn.sql('show tables').show()
        self.conn.sql(string)

    def upload_csv_as_table(self, schema_name: str, table_name: str, csv_file_path: str) -> None:
        self.conn.sql(f"CREATE TABLE if not exists {schema_name}.{table_name} AS SELECT * FROM read_csv('{csv_file_path}');")

    def create_index(self, schema_name: str, table_name: str, index_columns: list | str) -> None:
        pass
