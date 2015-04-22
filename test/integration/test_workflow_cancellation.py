# -*- mode:python ; fill-column:120 -*-
import logging
import time
import unittest

from awsflow import (WorkflowDefinition, execute, return_, WorkflowWorker,
                     ActivityWorker, WorkflowStarter, workflow_time)
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


if __name__ == '__main__':
    unittest.main()
