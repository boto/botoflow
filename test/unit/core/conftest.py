import pytest


@pytest.yield_fixture(scope='session')
def core_debug():
    from awsflow.core import (
        async_context, async_event_loop, async_task, async_task_context,
        base_future, decorators, future)

    async_context.DEBUG = True
    async_event_loop.DEBUG = True
    async_task.DEBUG = True
    async_task_context.DEBUG = True
    base_future.DEBUG = True
    decorators.DEBUG = True
    future.DEBUG = True

    yield None

    async_context.DEBUG = False
    async_event_loop.DEBUG = False
    async_task.DEBUG = False
    async_task_context.DEBUG = False
    base_future.DEBUG = False
    decorators.DEBUG = False
    future.DEBUG = False
