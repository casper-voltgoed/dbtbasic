from src.dbtbasic import find_order_from_blocks_dict


def test_order_block_dict():
    blocks_dict = {
        'a': ['b'],
        'b': ['c'],
        'c': [],
    }
    result = find_order_from_blocks_dict(blocks_dict)

    assert result == ['a', 'b', 'c']


def test_order_block_dict_missing_key():
    blocks_dict = {
        'a': ['b'],
        'b': ['c'],
    }
    result = find_order_from_blocks_dict(blocks_dict)

    assert result == ['a', 'b', 'c']


def test_order_block_dict_blank_key():
    blocks_dict = {
        'd': [],
        'a': ['b'],
        'b': ['c'],
    }
    result = find_order_from_blocks_dict(blocks_dict)

    assert 'd' in result
    result.remove('d')
    assert result == ['a', 'b', 'c']
