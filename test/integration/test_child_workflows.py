# -*- mode:python ; fill-column:120 -*-
import time
import unittest
import logging

from awsflow import (WorkflowDefinition, execute, return_, WorkflowWorker,
                      ActivityWorker, WorkflowStarter, workflow_options)
from awsflow.exceptions import ChildWorkflowTimedOutError, ChildWorkflowFailedError
from various_activities import BunchOfActivities
from awsflow.logging_filters import AWSFlowFilter

from utils import SWFMixIn


logging.getLogger().addFilter(AWSFlowFilter)
logging.getLogger('botocore').setLevel(logging.ERROR)


class MasterWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        instance = yield ChildWorkflow.execute(arg1, arg2)
        arg_sum = yield instance.workflow_result
        return_(arg_sum)

class ChildWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        arg_sum = yield BunchOfActivities.sum(arg1, arg2)
        return_(arg_sum)


class TimingOutMasterWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self):
        try:
            instance = yield TimingOutChildWorkflow.execute()
            yield instance.workflow_result
        except ChildWorkflowTimedOutError:
            return_(1)
        return_(2)


class TimingOutChildWorkflow(WorkflowDefinition):
    @execute(version='1.3', execution_start_to_close_timeout=1)
    def execute(self):
        return True


class MasterWorkflowWithException(WorkflowDefinition):
    @execute(version='1.0', execution_start_to_close_timeout=60)
    def execute(self, child_tasklist):
        try:
            with workflow_options(task_list=child_tasklist):
                instance = yield RaisingChildWorkflow.execute()
                yield instance.workflow_result
        except ChildWorkflowFailedError as err:
            if isinstance(err.cause, RuntimeError):
                return_(2)
        return_(1)


class RaisingChildWorkflow(WorkflowDefinition):
    @execute(version='1.0', execution_start_to_close_timeout=60)
    def execute(self):
        raise RuntimeError("Cry baby")


class TestChildWorkflows(SWFMixIn, unittest.TestCase):

    def test_two_workflows(self):
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            MasterWorkflow, ChildWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = MasterWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        for i in range(3):
            wf_worker.run_once()

        act_worker.run_once()

        for i in range(2):
            wf_worker.run_once()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 14)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 3)

    def test_child_workflow_timed_out(self):
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            TimingOutMasterWorkflow, TimingOutChildWorkflow)
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = TimingOutMasterWorkflow.execute()
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        time.sleep(3)
        wf_worker.run_once()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 1)

    def test_raising_child_workflows(self):
        child_tasklist = self.task_list + '_child'

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            MasterWorkflowWithException)
        child_wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, child_tasklist,
            RaisingChildWorkflow)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = MasterWorkflowWithException.execute(child_tasklist)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        wf_worker.run_once()
        child_wf_worker.run_once()
        wf_worker.run_once()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        assert len(hist) == 14
        assert hist[-1]['eventType'] == 'WorkflowExecutionCompleted'
        assert self.serializer.loads(hist[-1]['workflowExecutionCompletedEventAttributes']['result']) == 2


if __name__ == '__main__':
    unittest.main()
