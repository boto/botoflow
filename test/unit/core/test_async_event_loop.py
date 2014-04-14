import pytest
from awsflow.core import async_event_loop

pytestmark = pytest.mark.usefixtures('core_debug')


def test_smoke():
    ev = async_event_loop.AsyncEventLoop()
    assert None == ev.execute_all_tasks()

