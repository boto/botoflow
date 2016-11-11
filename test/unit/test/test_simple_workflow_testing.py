import pytest
from mock import patch

from botoflow import WorkflowDefinition, execute, activities, activity, return_, Future, coroutine
from botoflow.test.workflow_testing_context import WorkflowTestingContext


@activities(schedule_to_start_timeout=60,
            start_to_close_timeout=60)
class BunchOfActivities(object):
    @activity(version='1.1')
    def sum(self, x, y):
        return x + y

    @activity(version='1.4',
              schedule_to_close_timeout=60*2)
    def mul(self, x, y):
        return x * y

    @activity('1.1')
    def throw(self):
        raise ValueError("Hello-Error")


class SimpleWorkflow(WorkflowDefinition):

    @execute('1.0', 5)
    def run(self, x, y):
        retval = None
        sum_result = yield BunchOfActivities.sum(x, y)
        if sum_result > 2:
            retval = yield BunchOfActivities.mul(retval, 2)

        return_(retval)

    @coroutine
    def sync_method(self, x, y):
        return_(x+y)

    @coroutine
    def async_method(self, x, y):
        result = yield BunchOfActivities.sum(x, y)
        return_(result)


def test_simple_workflow_testing():
    with patch.object(BunchOfActivities, 'sum', return_value=Future.with_result(3)), \
         patch.object(BunchOfActivities, 'mul', return_value=Future.with_result(6)):

        with WorkflowTestingContext():
            result = SimpleWorkflow.run(1,2)
    assert 6 == result.result()
    assert BunchOfActivities.__dict__['sum'].__dict__['func'].swf_options['activity_type'].schedule_to_start_timeout == 60
    assert BunchOfActivities.__dict__['mul'].__dict__['func'].swf_options['activity_type'].schedule_to_start_timeout == 60
    assert BunchOfActivities.__dict__['mul'].__dict__['func'].swf_options['activity_type'].schedule_to_close_timeout == 120


def test_activity_not_stubbed_exception():
    with WorkflowTestingContext():
        result = SimpleWorkflow.run(1,2)

    assert NotImplementedError == type(result.exception())
    assert "Activity BunchOfActivities.sum must be stubbed" in repr(result.exception())


def test_sync_method():
    with WorkflowTestingContext():
        result = SimpleWorkflow(None).sync_method(1, 2)
    assert 3 == result.result()


@patch.object(BunchOfActivities, 'sum', return_value=Future.with_result(3))
def test_async_method(m_sum):
    with WorkflowTestingContext():
        result = SimpleWorkflow(None).async_method(1, 2)
    assert 3 == result.result()
