# -*- mode:python ; fill-column:120 -*-
import logging
import time
import unittest

from awsflow import (workflow_time, WorkflowDefinition, WorkflowWorker,
                      signal, execute, Return, WorkflowStarter)
from awsflow.logging_filters import AWSFlowFilter
from utils import SWFMixIn


logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger().addFilter(AWSFlowFilter)


class SignalledWorkflow(WorkflowDefinition):

    def __init__(self, workflow_execution):
        super(SignalledWorkflow, self).__init__(workflow_execution)
        self.msg = "Not signalled"

    @execute(version='1.0', execution_start_to_close_timeout=60)
    def execute(self):
        yield workflow_time.sleep(4)
        raise Return(self.msg)

    @signal()
    def signal(self, msg):
        self.msg = msg


class TestSignalledWorkflows(SWFMixIn, unittest.TestCase):

    def test_signalled_workflow(self):
        wf_worker = WorkflowWorker(
            self.endpoint, self.domain, self.task_list,
            SignalledWorkflow)

        with WorkflowStarter(self.endpoint, self.domain, self.task_list):
            instance = SignalledWorkflow.execute()
            self.workflow_execution = instance.workflow_execution

            # wait and signal the workflow
            time.sleep(1)
            instance.signal("Signalled")

        for i in range(2):
            wf_worker.run_once()

        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist['events']), 11)
        self.assertEqual(hist['events'][-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist['events'][-1]['workflowExecutionCompletedEventAttributes']['result']), 'Signalled')


if __name__ == '__main__':
    unittest.main()

