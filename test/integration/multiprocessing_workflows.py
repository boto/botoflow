from awsflow import *

from various_activities import BunchOfActivities


class NoActivitiesWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1):
        raise Return(arg1)

        class OneActivityWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                arg_sum = yield BunchOfActivities.sum(arg1, arg2)
                raise Return(arg_sum)


class NoActivitiesFailureWorkflow(WorkflowDefinition):

    @execute(version='1.1', execution_start_to_close_timeout=60)
    def execute(self, arg1):
        raise RuntimeError("ExecutionFailed")


class OneActivityWorkflow(WorkflowDefinition):

    @execute(version='1.1', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        arg_sum = yield BunchOfActivities.sum(arg1, arg2)
        raise Return(arg_sum)


class OneMultiWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        arg_sum = yield BunchOfActivities.sum(arg1, arg2)
        raise Return(arg_sum)


class TwoMultiWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        arg_sum = yield BunchOfActivities.sum(arg1, arg2)
        raise Return(arg_sum)

