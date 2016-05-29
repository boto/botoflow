import time
import unittest

from botoflow import WorkflowWorker, ActivityWorker, WorkflowStarter
from multiprocessing_workflows import OneMultiWorkflow, TwoMultiWorkflow
from various_activities import BunchOfActivities
from utils import SWFMixIn


class TestMultiWorkflows(SWFMixIn, unittest.TestCase):

    def test_two_workflows(self):
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            OneMultiWorkflow, TwoMultiWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
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
