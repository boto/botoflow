import logging
import time
import unittest

from calendar import timegm

from botoflow import (WorkflowDefinition, execute, return_, async, activity, ThreadedWorkflowExecutor,
                      ThreadedActivityExecutor, WorkflowWorker, ActivityWorker, activity_options, workflow_time,
                      flow_types, logging_filters, workflow_starter, workflow, activities, retry_activity,
                      retry_on_exception, swf_exceptions)

from botoflow.exceptions import ActivityTaskTimedOutError, ActivityTaskFailedError
from utils import SWFMixIn


@activities(schedule_to_start_timeout=60,
            start_to_close_timeout=60)
class RetryingActivities(object):

    @retry_activity(stop_max_attempt_number=2, wait_fixed=0.5)
    @activity(version='1.0', start_to_close_timeout=1)
    def activity_timing_out(self, sleep_time):
        return time.sleep(sleep_time)

    @retry_activity(stop_max_attempt_number=2, retry_on_exception=retry_on_exception(RuntimeError))
    @activity(version='1.0', start_to_close_timeout=2)
    def activity_raises_errors(self, exception):
        raise exception


logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger().addFilter(logging_filters.BotoflowFilter())


class TestRetryingActivitiesWorkflows(SWFMixIn, unittest.TestCase):

    def test_activity_timeouts(self):
        class ActivityRetryOnTimeoutWorkflow(WorkflowDefinition):
            @execute(version='1.2', execution_start_to_close_timeout=60)
            def execute(self, sleep):
                try:
                    yield RetryingActivities.activity_timing_out(sleep)
                except ActivityTaskTimedOutError:
                    pass

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, ActivityRetryOnTimeoutWorkflow)

        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, RetryingActivities())

        with workflow_starter(self.session, self.region, self.domain, self.task_list):
            instance = ActivityRetryOnTimeoutWorkflow.execute(sleep=2)
            self.workflow_execution = instance.workflow_execution

        for i in range(2):
            wf_worker.run_once()
            try:
                act_worker.run_once()
            except swf_exceptions.UnknownResourceError:
                # we expect the activity to have timed out already
                pass
            # for the timer
            wf_worker.run_once()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 22)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(hist[10]['eventType'], 'TimerStarted')
        self.assertEqual(hist[10]['timerStartedEventAttributes']['startToFireTimeout'], "1")

    def test_activity_exception_retries(self):
        class ActivityRetryOnExceptionWorkflow(WorkflowDefinition):
            @execute(version='1.2', execution_start_to_close_timeout=60)
            def execute(self):
                try:
                    yield RetryingActivities.activity_raises_errors(RuntimeError)
                except ActivityTaskFailedError as err:
                    assert isinstance(err.cause, RuntimeError)

                try:
                    yield RetryingActivities.activity_raises_errors(AttributeError)
                except ActivityTaskFailedError as err:
                    assert isinstance(err.cause, AttributeError)
                    pass

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, ActivityRetryOnExceptionWorkflow)

        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, RetryingActivities())

        with workflow_starter(self.session, self.region, self.domain, self.task_list):
            instance = ActivityRetryOnExceptionWorkflow.execute()
            self.workflow_execution = instance.workflow_execution

        for i in range(2):
            wf_worker.run_once()
            act_worker.run_once()
            # for the timer
            wf_worker.run_once()

        act_worker.run_once()
        wf_worker.run_once()

        # wf_worker.run_once()
        # print 'wfrun'

        time.sleep(1)

        # check that we have a timer started and that the workflow length is the same to validate
        # that retries happened only on one of the exceptions
        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 28)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(hist[10]['eventType'], 'TimerStarted')
        self.assertEqual(hist[10]['timerStartedEventAttributes']['startToFireTimeout'], "0")


if __name__ == '__main__':
    unittest.main()
