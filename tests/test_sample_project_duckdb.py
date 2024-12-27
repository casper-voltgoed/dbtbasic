import datetime
import os

import duckdb
from src.dbtbasic import BackendType, create_sql_project


def test_all():
    db_name = 'file.db'
    if os.path.isfile(db_name):
        os.remove(db_name)

    create_sql_project(os.path.join('tests', 'sample_project'), BackendType.duckdb)

    conn = duckdb.connect('file.db')
    result = conn.sql('select * from sample_project.stg_lat_temps limit 1').fetchall()
    print(result)

    assert result == [(1, 1, datetime.date(2023, 12, 1), 0.15)]
