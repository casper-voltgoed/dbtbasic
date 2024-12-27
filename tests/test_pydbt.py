from src.dbtbasic import find_order_from_blocks_dict


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
