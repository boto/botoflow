# -*- mode:python ; fill-column:120 -*-
import time
import unittest

from awsflow import (WorkflowDefinition, execute, return_, WorkflowWorker,
                      ActivityWorker, WorkflowStarter)
from awsflow.exceptions import ChildWorkflowTimedOutError
from various_activities import BunchOfActivities
from utils import SWFMixIn


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

if __name__ == '__main__':
    unittest.main()
