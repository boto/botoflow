# -*- mode:python ; fill-column:120 -*-
import logging
import time
import unittest

from awsflow import (WorkflowDefinition, execute, return_, WorkflowWorker,
                     ActivityWorker, WorkflowStarter, workflow_time,
                     ThreadedActivityExecutor)
from awsflow.exceptions import ChildWorkflowTimedOutError
from awsflow.logging_filters import AWSFlowFilter
from various_activities import BunchOfActivities
from utils import SWFMixIn


logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger().addFilter(AWSFlowFilter)


global child_execution
# type: awsflow.WorkflowExecution
child_execution = None


class MasterWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        global child_execution
        instance = yield ChildWorkflow.execute(arg1, arg2)
        child_execution = instance.workflow_execution
        yield instance.cancel()
        arg_sum = yield instance.workflow_result
        return_(arg_sum)


class ChildWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        yield workflow_time.sleep(3)
        return_(arg1 + arg2)


class SelfCancellingWorkflow(WorkflowDefinition):
    @execute(version='1.1', execution_start_to_close_timeout=60)
    def execute(self):
        yield self.cancel()
        return_(True)


class SelfCancellingWorkflowWithoutYield(WorkflowDefinition):
    @execute(version='1.1', execution_start_to_close_timeout=60)
    def execute(self):
        self.cancel()
        return_(True)


class SelfCancellingWorkflowWithCascade(WorkflowDefinition):
    def __init__(self, workflow_execution):
        super(SelfCancellingWorkflowWithCascade, self).__init__(workflow_execution)
        self.activities_client = BunchOfActivities()

    @execute(version='1.1', execution_start_to_close_timeout=60)
    def execute(self):
        self.activities_client.heartbeating_activity(5)
        yield self.activities_client.sum(1, 2)
        yield self.cancel()
        return_(True)


class TestChildWorkflows(SWFMixIn, unittest.TestCase):

    def test_child_workflow_cancel(self):
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            MasterWorkflow, ChildWorkflow)
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = MasterWorkflow.execute(1, 2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        time.sleep(1)
        for i in range(6):
            wf_worker.run_once()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        from pprint import pprint
        pprint(hist)
        pprint(self.get_workflow_execution_history_with_token(child_execution.workflow_id,
                                                              child_execution.run_id))
        self.assertEqual(len(hist), 10)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 1)


class TestWorkflowCancels(SWFMixIn, unittest.TestCase):

    def test_cancel_workflow(self):
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, SelfCancellingWorkflow)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SelfCancellingWorkflow.execute()
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')

    def test_cancel_and_complete_workflow_decisions_together(self):
        # a Cancel decision cannot be sent with another (causes Validation error)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            SelfCancellingWorkflowWithoutYield)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SelfCancellingWorkflowWithoutYield.execute()
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')

    def test_cancel_workflow_with_cascade_cancel(self):
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            SelfCancellingWorkflowWithCascade)

        act_worker = ThreadedActivityExecutor(ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities()))

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SelfCancellingWorkflowWithCascade.execute()
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()  # start first act
        act_worker.start(1, 4)
        wf_worker.run_once()  # cancel workflow and activity
        act_worker.stop()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 14)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCanceled')
        self.assertEqual(hist[-2]['eventType'], 'ActivityTaskCancelRequested')

if __name__ == '__main__':
    unittest.main()
