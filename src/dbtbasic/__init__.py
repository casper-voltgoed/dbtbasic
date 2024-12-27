import enum
import os

import logbasic

from src.dbtbasic.backend.base_backend import BaseBackend
from src.dbtbasic.backend.duckdb_backend import DuckDBBackend
from src.dbtbasic.backend.postgres_backend import PostgresBackend

from .yaml import load_yaml_file


"""
This module is a simple version of dbt using postgres.
It loads the sql files in a folder and "realizes" them in the correct order and adds indices as well
"""


class BackendType(enum.Enum):
    postgres = 'postgres'
    duckdb = 'duckdb'

    def get_backend(self) -> BaseBackend:
        if self == BackendType.postgres:
            return PostgresBackend()
        elif self == BackendType.duckdb:
            return DuckDBBackend()
        else:
            raise Exception('No backend for this type')


def create_sql_project(folder_path: str, backend_type: BackendType):
    """
    Converts the SQL and CSV files in a folder into tables/views, also adds indexes that are specified in an `index.yaml` file.
    The files in the folder will be created under the schema with the name of the folder.
    ## Params
        *  `folder_name`, The name of the folder in `include/sql/` where the files are located
    """
    backend = backend_type.get_backend()

    if not os.path.isdir(folder_path):
        raise Exception(f'Cannot find folder in "sql" folder: {folder_path}')

    # find all sql files
    sql_file_dict = find_sql_files(folder_path)

    ordered_sql_tables = find_order(sql_file_dict)
    print(ordered_sql_tables)

    # create schema
    folder_name = os.path.basename(folder_path)
    backend.create_schema(folder_name)

    # create seeds
    realize_seeds(folder_path, backend)

    # create tables and views
    for sql_table_name in ordered_sql_tables:
        realize_query(query=sql_file_dict[sql_table_name], table_name=sql_table_name, schema_name=folder_name, backend=backend)

    # create indices
    yaml_file_path = os.path.join(folder_path, 'index.yaml')

    if os.path.isfile(yaml_file_path):
        yaml_dict = load_yaml_file(yaml_file_path)

        for sql_table, index_value in yaml_dict.items():
            backend.create_index(schema_name=folder_name, table_name=sql_table, index_columns=index_value)


def find_sql_files(folder_path: str) -> dict[str, str]:
    sql_files = {}  # key is the name of the table, value is the contents of the file
    for root, _, files in os.walk(folder_path):
        for name in files:
            if name.lower().endswith('.sql'):
                file_path = os.path.join(root, name)

                with open(file_path, 'r') as sql_file:
                    sql_query = sql_file.read()

                sql_files[name[:-4]] = sql_query

    return sql_files


def find_order(sql_files: dict[str, str]) -> list:
    """
    Gives a topological ordering of a list.
    ## Params
        * `sql_files`, Dict where they key is the name of a sql file that will become a table/view and the value is the contents of the sql file
    ## Result
        * The keys of `sql_files` ordered in such a way that all references have been taken into account.
    """
    # This dict shows which sql file "blocks" other sql files. The key blocks its value(s) so for example stg_x : [int_y, final_z], then the ordering is stg_x, int_y, final_z
    blocks_dict: dict[str, list[str]] = {}
    for sqlfile1 in sql_files:
        blocks_dict[sqlfile1] = []
        for sqlfile2, sqlfile2_query in sql_files.items():
            # if the name of file 1 is mentioned in file 2. Then file 1 must be created before file 2 and so file 1 "blocks" file 2.
            if sqlfile1 in sqlfile2_query:
                blocks_dict[sqlfile1].append(sqlfile2)

    logbasic.debug('Blocks dict:', blocks_dict)
    order = find_order_from_blocks_dict(blocks_dict)  # type:ignore

    return order


def find_order_from_blocks_dict(blocks_dict: dict[str, list[str]]) -> list:
    """
    Recursively perform depth-first search to find the ordering, given the computed blocks_dict. As not every
    """
    order = []

    def dfs(node):
        visited[node] = True
        if node in blocks_dict:
            for neigh in blocks_dict[node]:
                if not visited[neigh]:
                    dfs(neigh)
        order.append(node)

    items = set(list(blocks_dict.keys()) + sum(blocks_dict.values(), []))
    visited = {item: False for item in items}

    for item in items:
        if not visited[item]:
            dfs(item)

    return order[::-1]


def realize_seeds(folder_path: str, backend: BaseBackend):
    """
    TODO make dataframe types the column types (text, timestamp, numeric, integer only to keep it easy)
    Converts .csv files to sql tables. The name of the csv file will be the name of the csv file and the schema the name of the folder it is in
    ## Params
        * `folder_path`, The path to a folder filled with csv files
        * `postgres_db`, object to execute the sql queries
    """
    import os

    schema_name = os.path.basename(folder_path)

    # upload all csv files
    for root, _, files in os.walk(folder_path):
        for name in files:
            if name.lower().endswith('.csv'):
                table_name = name[:-4]

                csv_file_path = os.path.join(root, name)
                backend.upload_csv_as_table(schema_name, table_name, csv_file_path)


def realize_query(query: str, table_name: str, schema_name: str, backend: BaseBackend):
    """
    Converts a SELECT sql query to a table/view. if the table name starts with `stg_` or `int_` it will become a view.
    ## Params
        * `query`, The select query that will become a table/view
        * `table_name`, the name of the table/view
        * `schema_name`, the schema where in the table/view is put
        * `postgres_db`, the postgres database object with which the realization is executed
    """
    # % problem in python
    query = query.replace('%', '%%')
    logbasic.debug(f'Realizing {table_name}')

    if table_name.startswith('stg_') or table_name.startswith('int_'):
        backend.create_view(schema_name, table_name, query)
    else:
        backend.create_table(schema_name, table_name, query)
