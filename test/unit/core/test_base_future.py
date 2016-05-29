import re
import pytest
import logging
from collections import namedtuple

import botoflow.core.base_future as futuremod
from botoflow.core.async_event_loop import AsyncEventLoop
from botoflow.core.async_task import AsyncTask
from botoflow.logging_filters import AWSFlowFilter

logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger('botoflow').addFilter(AWSFlowFilter())

pytestmark = pytest.mark.usefixtures('core_debug')


@pytest.yield_fixture(scope='function')
def ev():
    ev = AsyncEventLoop()
    yield ev


@pytest.yield_fixture(scope='function')
def callback_info():
    _callbacks = list()
    def task_callback(*args, **kwargs):
        _callbacks.append((args, kwargs))

    yield namedtuple("CallbackInfo", "callbacks task_callback") \
        (_callbacks, task_callback)


def test_constructor(ev):
    with ev:
        future = futuremod.BaseFuture()
        assert future._state == futuremod.PENDING
        assert future._result == None
        assert future._tasks == []


def test_states(ev):
    with ev:
        future = futuremod.BaseFuture()

        assert future._state == futuremod.PENDING
        assert not future.done()
        assert not future.running()
        assert not future.cancelled()

        future.set_running_or_notify_cancel()

        assert future._state == futuremod.RUNNING
        assert not future.done()
        assert future.running()
        assert not future.cancelled()

        future.set_result(None)

        assert future._state == futuremod.FINISHED
        assert future.done()
        assert not future.running()
        assert not future.cancelled()

        future = futuremod.BaseFuture.with_cancel()

        assert future._state == futuremod.CANCELLED
        assert future.done()
        assert not future.running()
        assert future.cancelled()


def test_set_result(ev):
    with ev:
        future = futuremod.BaseFuture()
        future.set_result('TestResult')

    assert future._result == 'TestResult'
    assert future.result() == 'TestResult'


def test_with_result():
    future = futuremod.BaseFuture.with_result(True)

    assert future.result()


def test_set_exception():
    future = futuremod.BaseFuture()
    err = RuntimeError('TestError')
    future.set_exception(err)

    assert future._exception == err
    assert future._traceback == None
    assert future.done()
    assert future.exception() == err
    with pytest.raises(RuntimeError):
        future.result()


def test_with_exception():
    err = RuntimeError('test')
    future = futuremod.BaseFuture.with_exception(err, True)

    assert err == future.exception()
    assert future.traceback()


def test_set_result_then_task(ev, callback_info):
    with ev:
        future = futuremod.BaseFuture()
        task = AsyncTask(callback_info.task_callback, (future,))
        future.set_result(12)
        future.add_task(task)

    ev.execute_all_tasks()

    assert 1 == len(callback_info.callbacks)
    assert callback_info.callbacks[0][0] == (future,)
    assert future.done()


def test_task_then_set_result(ev, callback_info):
    with ev:
        future = futuremod.BaseFuture()
        task = AsyncTask(callback_info.task_callback, (future,))
        future.add_task(task)
        assert not callback_info.callbacks
        future.set_result(12)

    ev.execute_all_tasks()
    assert 1 == len(callback_info.callbacks)
    assert callback_info.callbacks[0][0] == (future,)
    assert future.done()


def test_set_exception_then_task(ev, callback_info):
    with ev:
        future = futuremod.BaseFuture()
        task = AsyncTask(callback_info.task_callback, (future,))
        future.set_exception(RuntimeError('TestError'))
        future.add_task(task)

    ev.execute_all_tasks()
    assert 1 == len(callback_info.callbacks)
    assert callback_info.callbacks[0][0] == (future,)
    assert future.done()


def test_task_then_set_exception(ev, callback_info):
    with ev:
        future = futuremod.BaseFuture()
        task = AsyncTask(callback_info.task_callback, (future,))
        future.add_task(task)
        assert not callback_info.callbacks
        future.set_exception(RuntimeError('TestError'))

    ev.execute_all_tasks()
    assert 1 == len(callback_info.callbacks)
    assert callback_info.callbacks[0][0] == (future,)
    assert future.done()


def test_cancel():
    future = futuremod.BaseFuture()
    assert future.cancel()
    # the second time should just return as already cancelled
    assert future.cancel()

    future = futuremod.BaseFuture.with_result(True)
    assert False == future.cancel()


def test_repr():
    future = futuremod.BaseFuture()

    assert re.match(r'<BaseFuture at .* state=pending>',
                    repr(future))

    future.set_result('TestResult')
    assert re.match(r'<BaseFuture at .* state=finished returned TestResult',
                    repr(future))


def test_return_is_base_exception():
    assert isinstance(futuremod.Return(), BaseException)
