# -*- mode:python ; fill-column:120 -*-
import json
import logging
import time
import unittest

from awsflow import (WorkflowDefinition, execute, return_, get_context, WorkflowWorker,
                     WorkflowStarter)
from awsflow.core import CancelledError
from awsflow.exceptions import (RequestCancelExternalWorkflowExecutionInvalidError,
                                RequestCancelExternalWorkflowExecutionFailedError)
from awsflow.logging_filters import AWSFlowFilter
from various_activities import BunchOfActivities
from utils import SWFMixIn


logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger().addFilter(AWSFlowFilter)


class TestActivityRaisedCancels(SWFMixIn, unittest.TestCase):
    def test_one_activity_heartbeat(self):
        # test basic heartbeating

        class OneActivityHeartbeatWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                yield self.activities_client.heartbeating_activity(1)

        wf = OneActivityHeartbeatWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule first act
        act_worker.run_once()  # run first act
        wf_worker.run_once()  # finish
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(len(hist), 11)

    def test_one_activity_heartbeat_cancel_before_schedule(self):
        # test issuing a cancel with schedule decision

        class OneActivityHeartbeatCancelBeforeScheduleWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatCancelBeforeScheduleWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                activity_future = self.activities_client.heartbeating_activity(2)
                yield activity_future.cancel()
                yield activity_future
                return_(False)

        wf = OneActivityHeartbeatCancelBeforeScheduleWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # start/finish
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'Activity was cancelled before being scheduled with SWF')
        self.assertEqual(len(hist), 5)

    def test_one_activity_heartbeat_cancel_before_start(self):
        # test canceling an activity after being scheduled, but before being polled

        class OneActivityHeartbeatCancelBeforeStartWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatCancelBeforeStartWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                activity_future = self.activities_client.wrong_tasklist_activity()
                yield self.activities_client.sum(1, 2)
                yield activity_future.cancel()
                yield activity_future
                return_(False)

        wf = OneActivityHeartbeatCancelBeforeStartWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule both activities
        act_worker.run_once()  # start summing activity
        wf_worker.run_once()  # cancel wrong tasklist activity (which would have never started)
        wf_worker.run_once()  # finish
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'Activity was cancelled before being picked up by activity worker')
        self.assertEqual(len(hist), 17)

    def test_one_activity_heartbeat_cancel_raise(self):
        # test heartbeat activity raising cancel exception

        class OneActivityHeartbeatCancelRaiseWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatCancelRaiseWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                activity_future = self.activities_client.heartbeating_activity(10)
                yield self.activities_client.sum(1, 2)
                yield activity_future.cancel()
                yield activity_future
                return_(False)

        wf = OneActivityHeartbeatCancelRaiseWorkflow
        wf_worker, act_worker = self.get_workers(wf, threaded_act_worker=True)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule first two activities
        act_worker.start(1, 4)
        time.sleep(3)  # ensure both activities start before we cancel
        wf_worker.run_once()  # cancel decision
        wf_worker.run_once()  # raise cancel
        act_worker.stop()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'Cancel was requested during activity heartbeat')
        # hist differs depending on whether the heartbeat activity started or not
        self.assertTrue(len(hist) in [17, 18])

    def test_one_activity_heartbeat_ignore_cancel(self):
        # test heartbeat activity raising cancel exception that is ignored by execution

        class OneActivityHeartbeatIgnoreCancel(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatIgnoreCancel, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                activity_future = self.activities_client.heartbeating_activity(2)
                yield activity_future.cancel()
                try:
                    yield activity_future
                except CancelledError:
                    pass
                return_(False)

        wf = OneActivityHeartbeatIgnoreCancel
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule/cancel together, then trigger raise, pass, and complete
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(len(hist), 5)

    def test_one_activity_heartbeat_cancel_failure(self):
        # ensure bad internal logic path is covered

        class OneActivityHeartbeatCancelFailureWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatCancelFailureWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                activity_future = self.activities_client.heartbeating_activity(5)
                yield self.activities_client.sum(1, 2)
                activity_future._activity_id = '100'  # set invalid ID
                yield activity_future.cancel()
                return_(False)

        wf = OneActivityHeartbeatCancelFailureWorkflow
        wf_worker, act_worker = self.get_workers(wf, threaded_act_worker=True)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule both activities
        act_worker.start(1, 4)
        wf_worker.run_once()  # attempt cancel with wrong activity id
        wf_worker.run_once()  # respond to failed cancel event -> raise -> fail
        act_worker.stop()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionFailed')
        error = json.loads(hist[-1]['workflowExecutionFailedEventAttributes']['details'])[0]['__obj']
        self.assertEqual(error[0], "awsflow.exceptions:RequestCancelActivityTaskFailedError")
        self.assertEqual(error[1]['cause'], 'ACTIVITY_ID_UNKNOWN')
        self.assertEqual(len(hist), 17)


class TestWorkflowRaisedCancels(SWFMixIn, unittest.TestCase):

    class SelfCancellingWorkflow(WorkflowDefinition):
        @execute(version='1.1', execution_start_to_close_timeout=60)
        def execute(self, details=None):
            self.cancel(details)
            return_(True)

        def cancellation_handler(self):
            raise Exception("should not be called for self cancel.")

    def test_cancel_workflow_no_details(self):
        wf = TestWorkflowRaisedCancels.SelfCancellingWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'], 'None')
        self.assertEqual(len(hist), 5)

    def test_cancel_workflow_with_details(self):
        wf = TestWorkflowRaisedCancels.SelfCancellingWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf, 'some details')

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'some details')
        self.assertEqual(len(hist), 5)

    def test_cancel_workflow_suppressed(self):
        class SelfCancellingWorkflowSuppressedCancel(WorkflowDefinition):
            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, details=None):
                try:
                    self.cancel(details)
                except CancelledError:
                    pass
                return_(True)

            def cancellation_handler(self):
                raise Exception("should not be called for self cancel.")

        wf = SelfCancellingWorkflowSuppressedCancel
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(len(hist), 5)

    def test_cancel_workflow_clean_close(self):
        class SelfCancellingWorkflowCleanClose(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(SelfCancellingWorkflowCleanClose, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, details=None):
                try:
                    self.cancel(details)
                except CancelledError as err:
                    yield self.activities_client.cleanup_state_activity()
                    raise err
                return_(True)

            def cancellation_handler(self):
                raise Exception("should not be called for self cancel.")

        wf = SelfCancellingWorkflowCleanClose
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf, 'some details')

        wf_worker.run_once()  # cancel, catcch, schedule
        act_worker.run_once()
        wf_worker.run_once()  # raise/complete
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'some details')
        self.assertEqual(len(hist), 11)

    def test_cancel_workflow_with_activity_cascade(self):
        class SelfCancellingWorkflowWithCascade(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(SelfCancellingWorkflowWithCascade, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                self.activities_client.heartbeating_activity(5)
                yield self.activities_client.sum(1, 2)
                self.cancel()
                return_(True)

            def cancellation_handler(self):
                raise Exception("should not be called for self cancel.")

        wf = SelfCancellingWorkflowWithCascade
        wf_worker, act_worker = self.get_workers(wf, threaded_act_worker=True)
        self.start_workflow(wf)

        wf_worker.run_once()  # start both activities
        act_worker.start(1, 4)
        wf_worker.run_once()  # cancel workflow and the heartbeat activity
        wf_worker.run_once()  # additional for potential retry
        act_worker.stop()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-2]['eventType'], 'ActivityTaskCancelRequested')
        # hist differs depending on whether the heartbeat activity started or not
        self.assertTrue(len(hist) in [13, 14])


class TestCancelRequestedWorkflows(SWFMixIn, unittest.TestCase):

    def retry_until_cancelled(self, wf_worker, max_retries=3):
        """
        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_Decision.html

        Given that there may/may not be pending decisions at time of cancellation,
        a CancelWorkflowExecutionFailed event may come down. The decider should then
        resend the cancel decision. This could repeat (until all pending decisions are
        cleared). This test re-runs workflow worker until it goes through.

        May be worth implementing separate test that loops until we get a failure event
        to ensure that handling is in place...
        """
        if max_retries < 1:
            return

        time.sleep(2)
        hist = self.get_workflow_execution_history()
        fail_count = 0
        while(hist[-1]['eventType'] not in ['WorkflowExecutionCanceled', 'WorkflowExecutionTerminated']):
            # loop until we successfully cancel
            fail_count += 1
            failed_cancels = len(self.get_events(hist, 'CancelWorkflowExecutionFailed'))
            self.assertEqual(failed_cancels, fail_count)
            wf_worker.run_once()
            time.sleep(2)
            if fail_count == max_retries:  # dont loop forever if bad logic in place
                break
            hist = self.get_workflow_execution_history()

    class CancelRequestWorkflow(WorkflowDefinition):
        def __init__(self, workflow_execution):
            super(TestCancelRequestedWorkflows.CancelRequestWorkflow, self).__init__(
                workflow_execution)
            self.activities_client = BunchOfActivities()

        @execute(version='1.1', execution_start_to_close_timeout=120)
        def execute(self):
            # lots of activities to exercise edge case handling
            self.activities_client.heartbeating_activity(5)
            self.activities_client.sum(1, 0)
            self.activities_client.sum(2, 0)
            yield self.activities_client.sum(3, 0)
            yield self.activities_client.sum(4, 0)
            yield self.activities_client.heartbeating_activity(5)
            return_(True)

    def test_cancel_workflow_request(self):
        wf = TestCancelRequestedWorkflows.CancelRequestWorkflow
        wf_worker, act_worker = self.get_workers(wf, threaded_act_worker=True)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule first 4 activities
        act_worker.start(1, 2)  # start some

        self.request_cancel(self.workflow_execution)

        wf_worker.run_once()  # process request to cancel any non-completed activities

        self.retry_until_cancelled(wf_worker, max_retries=3)

        act_worker.stop()
        act_worker.join()

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')

    def test_cancel_workflow_request_custom_handler_ignore(self):
        class CancelRequestCustomHandlerIgnoreWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(CancelRequestCustomHandlerIgnoreWorkflow, self).__init__(
                    workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                yield self.activities_client.sleep_activity(10)
                return_(True)

            def cancellation_handler(self, event):
                pass

        wf = CancelRequestCustomHandlerIgnoreWorkflow
        wf_worker, act_worker = self.get_workers(wf, threaded_act_worker=True)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule activity
        act_worker.start(1, 1)

        self.request_cancel(self.workflow_execution)

        wf_worker.run_once()  # process/ignore cancel request
        wf_worker.run_once()  # process activity complete; finish
        act_worker.stop()
        act_worker.join()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        cancel_requested_events = self.get_events(hist, 'WorkflowExecutionCancelRequested')
        self.assertEqual(len(cancel_requested_events), 1)

    def test_cancel_workflow_request_custom_handler_no_yield(self):
        # test to ensure we clear any new activity decisions that are scheduled with
        # a cancel decision

        class CancelRequestCustomHandlerNoYieldWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(CancelRequestCustomHandlerNoYieldWorkflow, self).__init__(
                    workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                yield self.activities_client.sleep_activity(10)
                return_(True)

            def cancellation_handler(self, event):
                self.cancel_activities()
                self.activities_client.cleanup_state_activity()  # should be ignored
                raise CancelledError('custom cancel')

        wf = CancelRequestCustomHandlerNoYieldWorkflow
        wf_worker, act_worker = self.get_workers(wf, threaded_act_worker=True)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule first activity
        act_worker.start(1, 1)

        self.request_cancel(self.workflow_execution)

        wf_worker.run_once()  # handle request, cancel workflow
        act_worker.stop()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        cleanup_scheduled_events = self.get_scheduled_activities(
            hist, 'BunchOfActivities.cleanup_state_activity')
        self.assertEqual(len(cleanup_scheduled_events), 0)


class TestExternalExecutionCancelWorkflows(SWFMixIn, unittest.TestCase):

    def test_cancel_external_execution_not_external(self):
        class ExternalExecutionCancelNotExternalWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(ExternalExecutionCancelNotExternalWorkflow, self).__init__(
                    workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                workflow_exec = get_context().workflow_execution
                try:
                    self.cancel_external(external_workflow_id=workflow_exec.workflow_id,
                                         external_run_id=workflow_exec.run_id)
                except RequestCancelExternalWorkflowExecutionInvalidError:
                    return_('pass')
                return_('fail')

        wf = ExternalExecutionCancelNotExternalWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 'pass')

    def test_cancel_external_execution_success(self):
        class ExternalExecutionCancelTargetWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(ExternalExecutionCancelTargetWorkflow, self).__init__(
                    workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                yield self.activities_client.sleep_activity(30)
                return_(True)

        class ExternalExecutionCancelSourceWorkflow(WorkflowDefinition):
            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, target_wf_id, target_run_id):
                yield self.cancel_external(target_wf_id, target_run_id)
                return_('pass')

        source_wf = ExternalExecutionCancelSourceWorkflow
        target_wf = ExternalExecutionCancelTargetWorkflow

        source_wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, 'source_task_list', source_wf, target_wf)
        target_wf_worker, target_act_worker = self.get_workers(
            [source_wf, target_wf], threaded_act_worker=True)

        target_execution = self.start_workflow(target_wf)
        with WorkflowStarter(self.session, self.region, self.domain, 'source_task_list'):
            instance = source_wf.execute(*target_execution)
            source_execution = instance.workflow_execution

        target_wf_worker.run_once()  # sched sleep act
        target_act_worker.start(1, 1)  # start sleep act
        source_wf_worker.run_once()  # make cancel request
        target_wf_worker.run_once()  # receieve request; cancel self
        source_wf_worker.run_once()  # resolve cancel future; complete
        target_act_worker.stop()
        target_act_worker.join()

        source_hist = self.get_workflow_execution_history(
            workflow_id=source_execution.workflow_id, run_id=source_execution.run_id)
        self.assertEqual(source_hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            source_hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 'pass')
        self.assertEqual(len(source_hist), 10)

        target_hist = self.get_workflow_execution_history(
            workflow_id=target_execution.workflow_id, run_id=target_execution.run_id)
        self.assertEqual(target_hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        target_act_cancel_event = self.get_events(target_hist, 'ActivityTaskCancelRequested')
        self.assertEqual(len(target_act_cancel_event), 1)

    def test_cancel_external_execution_fail(self):
        class ExternalExecutionCancelFailWorkflow(WorkflowDefinition):
            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                try:
                    yield self.cancel_external('fake', 'fake')
                except RequestCancelExternalWorkflowExecutionFailedError:
                    return_('pass')
                return_('fail')

        wf = ExternalExecutionCancelFailWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # make external request
        wf_worker.run_once()  # process request failure event and complete
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 'pass')
        self.assertEqual(len(hist), 10)

if __name__ == '__main__':
    unittest.main()
