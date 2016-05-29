# -*- mode:python ; fill-column:120 -*-
import time
import unittest

from botoflow import (WorkflowDefinition, execute, return_, ThreadedActivityExecutor, GenericWorkflowWorker, ActivityWorker,
                      WorkflowStarter)

from botoflow.utils import extract_workflows_dict
from utils import SWFMixIn
from various_activities import BunchOfActivities


class WorkflowFinder (object):
    def __init__(self, *definitions):
        self._workflow_defintions = definitions

    @property
    def workflows(self):
        return extract_workflows_dict(self._workflow_defintions)

    def __call__(self, name, version):
        return self.workflows[(name, version)]

class TestGenericWorkflows(SWFMixIn, unittest.TestCase):

    def _register_workflows(self, generic_worker):
        for _, workflow_type, _ in generic_worker._get_workflow_finder().workflows.values():
            generic_worker._register_workflow_type(workflow_type)

    def test_one_activity(self):
        class OneActivityWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                arg_sum = yield self.activities_client.sum(arg1, arg2)
                return_(arg_sum)

        wf_worker = GenericWorkflowWorker(
            self.session, self.region, self.domain, self.task_list, WorkflowFinder(OneActivityWorkflow))

        self._register_workflows(wf_worker)

        act_worker = ThreadedActivityExecutor(ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities()))

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.start(1, 4)
        act_worker.stop()
        wf_worker.run_once()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 3)



if __name__ == '__main__':
    unittest.main()
