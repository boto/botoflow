from mock import MagicMock, call, patch

from awsflow.constants import USE_WORKER_TASK_LIST
from awsflow.core import AllFuture, Future, AsyncEventLoop
from awsflow.decider import activity_task_handler


def test_init():
    ath = activity_task_handler.ActivityTaskHandler('decider', 'task_list')

    assert ath._decider == 'decider'
    assert ath._task_list == 'task_list'
    assert ath._open_activities == {}
    assert ath._schedule_event_to_activity_id == {}
    assert ath._open_cancels == {}


def test_del():
    ath = activity_task_handler.ActivityTaskHandler('decider', 'task_list')
    m_future = MagicMock()
    ath._open_activities = {'activity': {'handler': m_future}}

    del ath
    assert m_future.close.mock_calls == [call()]


@patch.object(activity_task_handler, 'ActivityFuture')
def test_handle_execute_activity(m_ActivityFuture):
    m_decider = MagicMock()
    m_data_converter = MagicMock()
    m_activity_type = MagicMock(data_converter=m_data_converter)
    ath = activity_task_handler.ActivityTaskHandler(m_decider, 'task_list')

    decision_dict = {'task_list': {'name': USE_WORKER_TASK_LIST},
                     'activity_type_name': 'name', 'activity_type_version': 'version'}
    activity_future = ath.handle_execute_activity(m_activity_type, decision_dict, [], {})
    assert activity_future == m_ActivityFuture()
    assert m_data_converter.dumps.mock_calls == [call([[], {}])]


def test_request_cancel_activity_task_all():
    m_decider = MagicMock()
    ath = activity_task_handler.ActivityTaskHandler(m_decider, 'task_list')
    ath.request_cancel_activity_task = MagicMock()
    my_future = Future()
    ath._open_activities = {'1': {'future': my_future}}

    ev = AsyncEventLoop()
    with ev:
        assert isinstance(ath.request_cancel_activity_task_all(), AllFuture)

    ev.execute_all_tasks()
    assert call(my_future, '1') in ath.request_cancel_activity_task.mock_calls


def test_request_cancel_activity_task_duplicate():
    m_decider = MagicMock()
    ath = activity_task_handler.ActivityTaskHandler(m_decider, 'task_list')
    ath._open_cancels = {'1': {'future': 'future'}}

    assert ath.request_cancel_activity_task(None, '1') == 'future'
