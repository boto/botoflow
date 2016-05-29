import pytest

from botoflow.retry.base_retry_strategy import BaseRetryStrategy


def test_init():
    brs = BaseRetryStrategy()

    assert brs.retry_on == tuple()
    assert brs.raise_on == tuple()
    assert brs._error_count == 0
    assert brs.error is None
    assert brs.first_timestamp is None
    assert brs.timestamp is None
    assert brs.user_context is None


def test_init_retry_and_raise_set():
    with pytest.raises(AttributeError):
        BaseRetryStrategy(retry_on=[RuntimeError], raise_on=[RuntimeError])


def test_next_delay():
    with pytest.raises(NotImplementedError):
        BaseRetryStrategy().next_delay('error', 'datetime')


def test_set_properties():
    brs = BaseRetryStrategy()
    brs._set_properties('error', 'datetime', 'user_context')

    assert brs._error_count == 1
    assert brs.error == 'error'
    assert brs.timestamp == 'datetime'
    assert brs.user_context == 'user_context'