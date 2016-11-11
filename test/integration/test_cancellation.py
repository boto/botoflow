# -*- mode:python ; fill-column:120 -*-
import json
import logging
import time
import unittest

from threading import Thread

from botoflow import (WorkflowDefinition, execute, return_, WorkflowWorker, ActivityWorker,
                      workflow_starter, coroutine, workflow_time, workflow_options)
from botoflow.workflow_execution import WorkflowExecution
from botoflow.core import CancelledError
from botoflow.exceptions import RequestCancelExternalWorkflowExecutionFailedError
from botoflow.logging_filters import BotoflowFilter
from various_activities import BunchOfActivities
from utils import SWFMixIn


logging.getLogger().addFilter(BotoflowFilter)
logging.getLogger('botocore').setLevel(logging.ERROR)


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
                yield workflow_time.sleep(2)
                yield activity_future.cancel()
                yield activity_future
                return_(False)

        wf = OneActivityHeartbeatCancelBeforeStartWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule activity
        wf_worker.run_once()  # cancel wrong tasklist activity (which would have never started)
        wf_worker.run_once()  # finish
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'Activity was cancelled before being picked up by activity worker')
        self.assertEqual(len(hist), 16)

    def test_one_activity_heartbeat_cancel_raise(self):
        # test heartbeat activity raising cancel exception

        class OneActivityHeartbeatCancelRaiseWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityHeartbeatCancelRaiseWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                activity_future = self.activities_client.heartbeating_activity(10)
                yield workflow_time.sleep(2)
                yield activity_future.cancel()
                yield activity_future
                return_(False)

        wf = OneActivityHeartbeatCancelRaiseWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        act_worker_thread = Thread(target=act_worker.run_once)
        act_worker_thread.start()
        wf_worker.run_once()  # schedule first two activities
        time.sleep(1)  # ensure activity started
        wf_worker.run_once()  # cancel decision
        wf_worker.run_once()  # raise cancel
        act_worker_thread.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'],
                         'Cancel was requested during activity heartbeat')
        # hist differs depending on whether the heartbeat activity started or not
        self.assertEqual(len(hist), 17)

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
                yield workflow_time.sleep(2)
                activity_future._activity_id = '100'  # set invalid ID
                yield activity_future.cancel()
                return_(False)

        wf = OneActivityHeartbeatCancelFailureWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule both activities
        wf_worker.run_once()  # attempt cancel with wrong activity id
        wf_worker.run_once()  # respond to failed cancel event -> raise -> fail
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionFailed')
        error = json.loads(hist[-1]['workflowExecutionFailedEventAttributes']['details'])[0]['__obj']
        self.assertEqual(error[0], "botoflow.exceptions:RequestCancelActivityTaskFailedError")
        self.assertEqual(error[1]['cause'], 'ACTIVITY_ID_UNKNOWN')
        self.assertEqual(len(hist), 15)


class TestWorkflowRaisedCancels(SWFMixIn, unittest.TestCase):

    class SelfCancellingWorkflow(WorkflowDefinition):
        @execute(version='1.1', execution_start_to_close_timeout=60)
        def execute(self, details=None):
            self.cancel(details)
            return_(True)

    class SelfDirectCancellingWorkflow(WorkflowDefinition):
        @execute(version='1.1', execution_start_to_close_timeout=60)
        def execute(self):
            raise CancelledError()
            return_(True)

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

    def test_cancel_workflow_direct_raise(self):
        wf = TestWorkflowRaisedCancels.SelfDirectCancellingWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-1]['workflowExecutionCanceledEventAttributes']['details'], '')
        self.assertEqual(len(hist), 5)

    def test_cancel_workflow_with_handler(self):
        class SelfCancellingWorkflowWithHandler(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(SelfCancellingWorkflowWithHandler, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, details=None):
                self.cancel(details)
                return_(True)

            @coroutine
            def cancellation_handler(self):
                yield self.activities_client.cleanup_state_activity()

        wf = SelfCancellingWorkflowWithHandler
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
        completed_activities = self.get_events(hist, 'ActivityTaskCompleted')
        self.assertEqual(len(completed_activities), 1)
        result = json.loads(completed_activities[0]['activityTaskCompletedEventAttributes']['result'])
        self.assertEqual(result, 'clean')
        self.assertEqual(len(hist), 11)

    def test_cancel_workflow_with_activity_cascade(self):
        class SelfCancellingWorkflowWithCascade(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(SelfCancellingWorkflowWithCascade, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self):
                self.activities_client.heartbeating_activity(5)
                yield workflow_time.sleep(1)
                self.cancel()
                return_(True)

        wf = SelfCancellingWorkflowWithCascade
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        act_worker_thread = Thread(target=act_worker.run_once)
        act_worker_thread.start()
        wf_worker.run_once()  # start activity
        wf_worker.run_once()  # cancel workflow and the heartbeat activity
        act_worker_thread.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-2]['eventType'], 'ActivityTaskCancelRequested')
        # hist differs depending on whether the heartbeat activity started or not
        self.assertEqual(len(hist), 13)


class TestBotoCancelWorkflows(SWFMixIn, unittest.TestCase):

    class BotoCancelRequestWorkflow(WorkflowDefinition):
        def __init__(self, workflow_execution):
            super(TestBotoCancelWorkflows.BotoCancelRequestWorkflow, self).__init__(
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
        wf = TestBotoCancelWorkflows.BotoCancelRequestWorkflow
        wf_worker, act_worker = self.get_workers(wf)
        self.start_workflow(wf)

        wf_worker.run_once()  # schedule first 4 activities
        act_worker_thread = Thread(target=lambda: (act_worker.run_once(), act_worker.run_once()))
        act_worker_thread.start()
        self.request_cancel(self.workflow_execution)

        wf_worker.run_once()  # process request to cancel any non-completed activities

        self.retry_until_cancelled(wf_worker, max_retries=3)

        act_worker_thread.join()

        hist = self.get_workflow_execution_history()
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')


class TestExternalExecutionCancelWorkflows(SWFMixIn, unittest.TestCase):

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
                external_wf = WorkflowDefinition(WorkflowExecution(target_wf_id, target_run_id))
                yield external_wf.cancel()
                return_('pass')

        source_wf = ExternalExecutionCancelSourceWorkflow
        target_wf = ExternalExecutionCancelTargetWorkflow

        source_wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, 'source_task_list', source_wf, target_wf)
        target_wf_worker, target_act_worker = self.get_workers(
            [source_wf, target_wf])

        target_execution = self.start_workflow(target_wf)
        with workflow_starter(self.session, self.region, self.domain, 'source_task_list'):
            instance = source_wf.execute(*target_execution)
            source_execution = instance.workflow_execution

        target_act_worker_thread = Thread(target=target_act_worker.run_once)
        target_act_worker_thread.start()
        target_wf_worker.run_once()  # sched sleep act
        source_wf_worker.run_once()  # make cancel request
        target_wf_worker.run_once()  # receieve request; cancel self
        source_wf_worker.run_once()  # resolve cancel future; complete
        target_act_worker_thread.join()

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
                    external_wf = WorkflowDefinition(WorkflowExecution('fake', 'fake'))
                    yield external_wf.cancel()
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


class TestCancelChildWorkflows(SWFMixIn, unittest.TestCase):
    def test_cancel_child_workflow(self):
        class CancelChildWorkflowsParentWorkflow(WorkflowDefinition):
            @execute(version='1.2', execution_start_to_close_timeout=60)
            def execute(self, child_task_list):
                with workflow_options(task_list=child_task_list):
                    instance = yield CancelChildWorkflowsChildWorkflow.execute()
                    yield instance.cancel()
                    try:
                        yield instance.workflow_result
                    except CancelledError:
                        return_('pass')

        class CancelChildWorkflowsChildWorkflow(WorkflowDefinition):
            @execute(version='1.3', execution_start_to_close_timeout=60)
            def execute(self):
                arg_sum = yield BunchOfActivities.sleep_activity(10)
                return_(arg_sum)

        child_task_list = self.task_list + '_child'

        parent_wf = CancelChildWorkflowsParentWorkflow
        child_worker = WorkflowWorker(self.session, self.region, self.domain, child_task_list,
                                      CancelChildWorkflowsChildWorkflow)

        wf_worker = WorkflowWorker(self.session, self.region, self.domain, self.task_list,
                                   parent_wf)
        act_worker = ActivityWorker(self.session, self.region, self.domain, child_task_list, BunchOfActivities())

        self.start_workflow(parent_wf, child_task_list)

        def activity_wrapper():
            try:
                act_worker.run_once()
            except Exception:
                pass

        act_worker_thread = Thread(target=activity_wrapper)
        act_worker_thread.start()
        wf_worker.run_once()  # start parent workflow
        child_worker.run_once()  # start child workflow
        wf_worker.run_once()  # cancel child
        child_worker.run_once()  # child intakes request and cancels
        time.sleep(1)
        wf_worker.run_once()  # parent waits for cancellation to complete
        act_worker_thread.join()

        time.sleep(1)
        parent_hist = self.get_workflow_execution_history()
        self.assertEqual(parent_hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        child_cancel_events = self.get_events(parent_hist, 'ExternalWorkflowExecutionCancelRequested')
        self.assertEqual(len(child_cancel_events), 1)
        child_execution = child_cancel_events[0][
            'externalWorkflowExecutionCancelRequestedEventAttributes']['workflowExecution']
        workflow_id = child_execution['workflowId']
        run_id = child_execution['runId']
        child_hist = self.get_workflow_execution_history(workflow_id=workflow_id, run_id=run_id)
        self.assertEqual(child_hist[-1]['eventType'], 'WorkflowExecutionCanceled')


if __name__ == '__main__':
    unittest.main()
