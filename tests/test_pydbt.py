import pandas as pd
from src.dbtbasic import find_order_from_blocks_dict, get_sql_columns_string


def test_order_block_dict():
    blocks_dict = {
        'a': ['b'],
        'b': ['c'],
        'c': [],
    }
    result = find_order_from_blocks_dict(blocks_dict)

    assert result == ['c', 'b', 'a']


def test_order_block_dict_missing_key():
    blocks_dict = {
        'a': ['b'],
        'b': ['c'],
    }
    result = find_order_from_blocks_dict(blocks_dict)

    assert result == ['c', 'b', 'a']


def test_order_block_dict_blank_key():
    blocks_dict = {
        'a': ['b'],
        'b': ['c'],
        'd': [],
    }
    result = find_order_from_blocks_dict(blocks_dict)

    assert result == ['c', 'b', 'a', 'd']


def test_sql_columns():
    df = pd.DataFrame({'fcol': [1.0], 'icol': [1], 'timecol': [pd.Timestamp('20180310')], 'textcol': ['foo']})

    result = get_sql_columns_string(df)

    assert result == 'fcol numeric, icol integer, timecol timestamptz, textcol text'
