import pytest
from mock import patch

from awsflow import WorkflowDefinition, execute, activities, activity, return_, Future
from awsflow.test.workflow_testing_context import WorkflowTestingContext


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


def test_simple_workflow_testing():
    with patch.object(BunchOfActivities, 'sum', return_value=Future.with_result(3)), \
         patch.object(BunchOfActivities, 'mul', return_value=Future.with_result(6)):

        with WorkflowTestingContext():
            result = SimpleWorkflow.run(1,2)
    assert 6 == result.result()


def test_activity_not_stubbed_exception():
    with WorkflowTestingContext():
        result = SimpleWorkflow.run(1,2)

    assert NotImplementedError == type(result.exception())
    assert "Activity BunchOfActivities.sum must be stubbed" in repr(result.exception())
