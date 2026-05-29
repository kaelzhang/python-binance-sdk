import random
import pytest

from binance.core.common.sequenced_list import SequencedList


@pytest.fixture
def seq():
    return SequencedList([
        (x, random.randint(0, 100)) for x in range(0, 10)
    ])


def test_add_to_first(seq):
    assert seq.add((-1, 10)) == (0, False)

    assert seq[0] == (-1, 10)


def test_add_overridden(seq):
    assert seq.add((0, 2)) == (0, True)

    assert seq[0] == (0, 2)


def test_zero_quantity(seq):
    assert seq.add((1, 0)) == (1, True)

    # The original seq[1] has been removed
    assert seq[1][0] == 2
    assert len(seq) == 9


def test_add_last(seq):
    assert seq.add((100, 100)) == (10, False)
    assert seq[10] == (100, 100)
    assert len(seq) == 11


def test_zero_quantity_price_non_exists(seq):
    origin_quantity = seq[0][1]

    assert seq.add((- 1, 0)) == (0, False)
    assert seq[0][1] == origin_quantity


def test_add_to_last(seq):
    assert seq.add((101, 1)) == (10, False)
    assert seq[10][0] == 101
