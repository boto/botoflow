import pytest
from botoflow.retry.transiency_retry_strategy import TransiencyRetryStrategy


def test_init():
    trs = TransiencyRetryStrategy()

    assert trs.consecutive_errors == 0
    assert trs.max_consecutive_errors is None


def test_init_errors():
    with pytest.raises(AttributeError):
        TransiencyRetryStrategy(max_consecutive_errors=0)


def test_next_delay():
    trs = TransiencyRetryStrategy()

    assert 0 == trs.next_delay(RuntimeError(), 1)
    assert isinstance(trs.error, RuntimeError)
    assert 1 == trs.timestamp


def test_next_delay_max_errors():
    trs = TransiencyRetryStrategy(max_consecutive_errors=2)

    assert 0 == trs.next_delay(RuntimeError(), 1)
    assert 0 == trs.next_delay(RuntimeError(), 1)

    with pytest.raises(RuntimeError):
        trs.next_delay(RuntimeError(), 1)


def test_next_delay_retry_on():
    trs = TransiencyRetryStrategy(retry_on=[RuntimeError])

    assert 0 == trs.next_delay(RuntimeError(), 1)

    with pytest.raises(AssertionError):
        trs.next_delay(AssertionError(), 1)


def test_next_delay_raise_on():
    trs = TransiencyRetryStrategy(raise_on=[AssertionError])

    assert 0 == trs.next_delay(RuntimeError(), 1)

    with pytest.raises(AssertionError):
        trs.next_delay(AssertionError(), 1)