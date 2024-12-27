import pandas as pd

from src.dbtbasic.backend.postgres_backend import _get_sql_columns_string


def test_sql_columns():
    df = pd.DataFrame({'fcol': [1.0], 'icol': [1], 'timecol': [pd.Timestamp('20180310')], 'textcol': ['foo']})

    result = _get_sql_columns_string(df)

    assert result == 'fcol numeric, icol integer, timecol timestamptz, textcol text'
