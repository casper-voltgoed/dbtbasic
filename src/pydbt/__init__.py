import os
import numpy as np
import pandas as pd
from psycopg2 import sql
import postgreasy

from .yaml import load_yaml_file


"""
This module is a simple version of dbt using postgres.
It loads the sql files in a folder and "realizes" them in the correct order and adds indices as well
"""


def create_sql_project(folder_path: str):
    """
    Converts the SQL and CSV files in a folder into tables/views, also adds indexes that are specified in an `index.yaml` file.
    The files in the folder will be created under the schema with the name of the folder.
    ## Params
        *  `folder_name`, The name of the folder in `include/sql/` where the files are located
    """

    if not os.path.isdir(folder_path):
        raise Exception(f'Cannot find folder in "sql" folder: {folder_path}')

    # find all sql files
    sql_file_dict = find_sql_files(folder_path)

    ordered_sql_tables = find_order(sql_file_dict)

    # create schema
    folder_name = os.path.basename(folder_path)
    postgreasy.create_schema(folder_name)

    # create seeds
    realize_seeds(folder_path)

    # create tables and views
    for sql_table_name in ordered_sql_tables:
        realize_query(query=sql_file_dict[sql_table_name], table_name=sql_table_name, schema_name=folder_name)

    # create indices
    yaml_file_path = os.path.join(folder_path, 'index.yaml')

    if os.path.isfile(yaml_file_path):
        yaml_dict = load_yaml_file(yaml_file_path)

        for sql_table, index_value in yaml_dict.items():
            if isinstance(index_value, list):
                index_query = sql.SQL('CREATE INDEX {index_name} ON {schema_name}.{table_name} ({column_list});').format(
                    index_name=sql.Identifier(f'{sql_table}_{"-".join(index_value)}_idx'),
                    schema_name=sql.Identifier(folder_name),
                    table_name=sql.Identifier(sql_table),
                    column_list=sql.SQL(', ').join(map(sql.Identifier, index_value)),
                )
            else:
                index_query = sql.SQL('CREATE INDEX {index_name} ON {schema_name}.{table_name} ({column});').format(
                    index_name=sql.Identifier(f'{sql_table}_{index_value}_idx'),
                    schema_name=sql.Identifier(folder_name),
                    table_name=sql.Identifier(sql_table),
                    column=sql.Identifier(index_value),
                )
            postgreasy.execute(index_query)


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
        for sqlfile2, sqlfile2_query in sql_files.items():
            # if the name of file 1 is mentioned in file 2. Then file 1 must be created before file 2 and so file 1 "blocks" file 2.
            if sqlfile1 in sqlfile2_query:
                if sqlfile1 not in blocks_dict:
                    blocks_dict[sqlfile1] = [sqlfile2]
                else:
                    blocks_dict[sqlfile1].append(sqlfile2)
        # if the file blocks nothing, we still need to add it to the dictionary
        else:
            blocks_dict[sqlfile1] = []

    order = find_order_from_blocks_dict(blocks_dict)  # type:ignore

    return order[::-1]


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

    return order


def realize_seeds(folder_path: str):
    """
    TODO make dataframe types the column types (text, timestamp, numeric, integer only to keep it easy)
    Converts .csv files to sql tables. The name of the csv file will be the name of the csv file and the schema the name of the folder it is in
    ## Params
        * `folder_path`, The path to a folder filled with csv files
        * `postgres_db`, object to execute the sql queries
    """
    import os
    import pandas as pd

    schema_name = os.path.basename(folder_path)

    # upload all csv files
    for root, _, files in os.walk(folder_path):
        for name in files:
            if name.lower().endswith('.csv'):
                table_name = name[:-4]

                df = pd.read_csv(os.path.join(root, name))
                columns = get_sql_columns_string(df)

                postgreasy.create_table(schema_name, table_name, sql.SQL(columns))
                postgreasy.insert_df(df, schema_name, table_name)


def get_sql_columns_string(df: pd.DataFrame):
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


def realize_query(query: str, table_name: str, schema_name: str):
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

    if table_name.startswith('stg_') or table_name.startswith('int_'):
        # create as view
        # we have to drop old view, in case columns are removed after update
        creation_query = sql.SQL(
            """
            drop view if exists {schema_name}.{table_name} cascade;
            create view         {schema_name}.{table_name} as ({query});
            """
        )

    else:
        creation_query = sql.SQL(
            """
            drop table if exists {schema_name}.{table_name};
            create table         {schema_name}.{table_name} as ({query});
            """
        )

    postgreasy.execute(creation_query.format(schema_name=sql.Identifier(schema_name), table_name=sql.Identifier(table_name), query=sql.SQL(query)))
