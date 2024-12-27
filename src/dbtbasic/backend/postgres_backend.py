import numpy as np
import pandas as pd
import postgreasy
from psycopg2 import sql

from src.dbtbasic.backend.base_backend import BaseBackend


class PostgresBackend(BaseBackend):
    def __init__(self):
        self.conn = postgreasy.get_connection()

    def create_schema(self, schema_name: str) -> None:
        postgreasy.create_schema(schema_name, self.conn)

    def create_view(self, schema_name: str, table_name: str, query: str) -> None:
        # we have to drop old view, in case columns are removed after update
        creation_query = sql.SQL(
            """
            drop view if exists {schema_name}.{table_name} cascade;
            create view         {schema_name}.{table_name} as ({query});
            """
        ).format(schema_name=sql.Identifier(schema_name), table_name=sql.Identifier(table_name), query=sql.SQL(query))
        postgreasy.execute(creation_query, self.conn)

    def create_table(self, schema_name: str, table_name: str, query: str) -> None:
        # we have to drop old view, in case columns are removed after update
        creation_query = sql.SQL(
            """
            drop table if exists {schema_name}.{table_name};
            create table         {schema_name}.{table_name} as ({query});
            """
        ).format(schema_name=sql.Identifier(schema_name), table_name=sql.Identifier(table_name), query=sql.SQL(query))
        postgreasy.execute(creation_query, self.conn)

    def create_index(self, schema_name: str, table_name: str, index_columns: list | str) -> None:
        if not isinstance(index_columns, list):
            index_columns = [index_columns]

        index_query = sql.SQL('CREATE INDEX {index_name} ON {schema_name}.{table_name} ({columns});').format(
            index_name=sql.Identifier(f'{table_name}_{"-".join(index_columns)}_idx'),
            schema_name=sql.Identifier(schema_name),
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(', ').join(map(sql.Identifier, index_columns)),
        )
        postgreasy.execute(index_query, self.conn)

    def upload_csv_as_table(self, schema_name: str, table_name: str, csv_file_path: str) -> None:
        df = pd.read_csv(csv_file_path)
        columns = _get_sql_columns_string(df)

        postgreasy.create_table(schema_name, table_name, sql.SQL(columns), self.conn)
        postgreasy.insert_df(df, schema_name, table_name, self.conn)


def _get_sql_columns_string(df: pd.DataFrame):
    """
    Make the sql query for the columns by getting their names and checking their type
    """
    col_texts = []
    for col in df.columns:
        dtype = df[col].dtype
        sql_type = 'text'
        if np.issubdtype(dtype, np.float_):  # type:ignore
            sql_type = 'numeric'
        elif np.issubdtype(dtype, np.integer):  # type:ignore
            sql_type = 'integer'
        elif np.issubdtype(dtype, np.datetime64):  # type:ignore
            sql_type = 'timestamptz'

        col_texts.append(f'{col} {sql_type}')

    return ', '.join(col_texts)
