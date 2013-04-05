import logging
import time
import unittest


### EVIL MONKEY PATCH FOR REQUESTS
### https://github.com/kennethreitz/requests/pull/1223
### Wait for requests 1.1.1, which should be soon...
import requests.sessions
import requests.adapters
if not hasattr(requests.sessions.Session, '__attrs__'):
    def __getstate__(self):
        return dict((attr, getattr(self, attr, None)) for attr in
                    self.__attrs__)

    def __setstate__(self, state):
        for attr, value in state.items():
            setattr(self, attr, value)
        self.adapters = {}
        self.mount('http://', requests.adapters.HTTPAdapter())
        self.mount('https://', requests.adapters.HTTPAdapter())

    requests.sessions.Session.__getstate__ = __getstate__
    requests.sessions.Session.__setstate__ = __setstate__
    requests.sessions.Session.__attrs__ = [
        'headers', 'cookies', 'auth', 'timeout', 'proxies', 'hooks',
        'params', 'verify', 'cert', 'prefetch']


from awsflow import (
    MultiprocessingActivityWorker, MultiprocessingWorkflowWorker, WorkflowStarter)
from awsflow.logging_filters import AWSFlowFilter
from multiprocessing_workflows import (
    NoActivitiesWorkflow, NoActivitiesFailureWorkflow, OneActivityWorkflow)
from various_activities import BunchOfActivities
from utils import SWFMixIn


logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger().addFilter(AWSFlowFilter)


class TestMultiprocessingWorkers(SWFMixIn, unittest.TestCase):

    def test_no_activities(self):

        worker = MultiprocessingWorkflowWorker(
            self.endpoint, self.domain, self.task_list, NoActivitiesWorkflow)
        with WorkflowStarter(self.endpoint, self.domain, self.task_list):
            instance = NoActivitiesWorkflow.execute(arg1="TestExecution")
            self.workflow_execution = instance.workflow_execution

        # start + stop should run the worker's Decider once
        worker.start()
        worker.stop()
        worker.join()
        time.sleep(2)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist['events']), 5)
        self.assertEqual(hist['events'][-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist['events'][-1]['workflowExecutionCompletedEventAttributes']['result']), 'TestExecution')

    def test_no_activities_failure(self):

        worker = MultiprocessingWorkflowWorker(
            self.endpoint, self.domain, self.task_list, NoActivitiesFailureWorkflow)
        with WorkflowStarter(self.endpoint, self.domain, self.task_list):
            instance = NoActivitiesFailureWorkflow.execute(arg1="TestExecution")
            self.workflow_execution = instance.workflow_execution

        worker.start()
        worker.stop()
        worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist['events']), 5)
        self.assertEqual(hist['events'][-1]['eventType'], 'WorkflowExecutionFailed')
        self.assertEqual(str(self.serializer.loads(
            hist['events'][-1]['workflowExecutionFailedEventAttributes']['details'])[0]),
                         "ExecutionFailed")

    def test_one_activity(self):
        wf_worker = MultiprocessingWorkflowWorker(
            self.endpoint, self.domain, self.task_list, OneActivityWorkflow)

        act_worker = MultiprocessingActivityWorker(
            self.endpoint, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.endpoint, self.domain, self.task_list):
            instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.start()
        act_worker.start()
        time.sleep(20)
        act_worker.stop()
        wf_worker.stop()
        act_worker.join()
        wf_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist['events']), 11)
        self.assertEqual(hist['events'][-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist['events'][-1]['workflowExecutionCompletedEventAttributes']['result']), 3)

if __name__ == '__main__':
    unittest.main()
