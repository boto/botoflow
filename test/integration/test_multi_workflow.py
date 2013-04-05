import logging
import time
import unittest

from awsflow import WorkflowWorker, ActivityWorker, WorkflowStarter
from awsflow.logging_filters import AWSFlowFilter
from multiprocessing_workflows import OneMultiWorkflow, TwoMultiWorkflow
from various_activities import BunchOfActivities
from utils import SWFMixIn


logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger().addFilter(AWSFlowFilter)


class TestMultiWorkflows(SWFMixIn, unittest.TestCase):

    def test_two_workflows(self):
        wf_worker = WorkflowWorker(
            self.endpoint, self.domain, self.task_list,
            OneMultiWorkflow, TwoMultiWorkflow)
        act_worker = ActivityWorker(
            self.endpoint, self.domain, self.task_list, BunchOfActivities())
        with WorkflowStarter(self.endpoint, self.domain, self.task_list):
            instance = OneMultiWorkflow.execute(arg1=1, arg2=2)
            self.workflow_executions.append(instance.workflow_execution)
            instance = TwoMultiWorkflow.execute(arg1=1, arg2=2)
            self.workflow_executions.append(instance.workflow_execution)

        for i in range(2):
            wf_worker.run_once()
            act_worker.run_once()

        wf_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

if __name__ == '__main__':
    unittest.main()
