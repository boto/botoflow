import pytest

try:
    from unittest.mock import MagicMock, patch, call
except ImportError:
    from mock import MagicMock, patch, call

from datetime import datetime

from awsflow import workflow_time
from awsflow.context import DecisionContext
from awsflow.decider import Decider


@patch.object(workflow_time, 'get_context')
def test_time(m_get_context):
    dt = datetime.fromtimestamp(10.0)
    m_get_context.return_value = MagicMock(spec=DecisionContext, _workflow_time=dt)
    assert 10 == workflow_time.time()


def test_time_wrong_context():
    with pytest.raises(TypeError):
        workflow_time.time()

@patch.object(workflow_time, 'get_context')
def test_time_no_context(m_get_context):
    m_get_context.side_effect = AttributeError()

    with pytest.raises(TypeError):
        workflow_time.time()


@patch.object(workflow_time, 'get_context')
def test_sleep(m_get_context):
    m_decider = MagicMock(spec=Decider)
    m_decider.handle_execute_timer.return_value = 'Works'
    m_get_context.return_value = MagicMock(spec=DecisionContext, decider=m_decider)

    assert 'Works' == workflow_time.sleep(10)
    assert m_decider.handle_execute_timer.mock_calls == [call(10)]


def test_sleep_wrong_context():
    with pytest.raises(TypeError):
        workflow_time.sleep(10)

@patch.object(workflow_time, 'get_context')
def test_sleep_no_context(m_get_context):
    m_get_context.side_effect = AttributeError()

    with pytest.raises(TypeError):
        workflow_time.sleep()


