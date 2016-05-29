try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


from botoflow.core import AsyncEventLoop, BaseFuture, Future, async, return_
from botoflow.decider.activity_future import ActivityFuture


def test_activity_future():

    ev = AsyncEventLoop()
    with ev:

        future = Future()

        m_activity_task_handler = MagicMock()
        activity_future = ActivityFuture(future, m_activity_task_handler, 1)

        @async
        def main():
            result = yield activity_future
            return_(result)

        main_future = main()
        future.set_result(3)

    ev.execute_all_tasks()
    assert main_future.result() == 3


def test_activity_future_cancel():

    ev = AsyncEventLoop()
    with ev:

        future = Future()
        cancel_future = BaseFuture()

        m_activity_task_handler = MagicMock()
        m_activity_task_handler.request_cancel_activity_task.return_value = cancel_future
        activity_future = ActivityFuture(future, m_activity_task_handler, 1)

        @async
        def main():
            cancel_future.cancel()
            result = yield activity_future.cancel()
            assert result == cancel_future

        main()
        future.set_result(3)

    ev.execute_all_tasks()
    assert cancel_future.cancelled()


def test_activity_future_cancel_failed():

    ev = AsyncEventLoop()
    with ev:

        future = Future()
        cancel_future = BaseFuture()

        m_activity_task_handler = MagicMock()
        m_activity_task_handler.request_cancel_activity_task.return_value = cancel_future
        activity_future = ActivityFuture(future, m_activity_task_handler, 1)

        @async
        def main():
            cancel_future.set_exception(RuntimeError())
            yield activity_future.cancel()

        main_future = main()
        future.set_result(3)

    ev.execute_all_tasks()
    assert isinstance(main_future.exception(), RuntimeError)
