import logging

import pytest
from botocore import session

from awsflow.data_converter import JSONDataConverter

from awsflow import (WorkflowWorker, ActivityWorker, WorkflowStarter, ThreadedActivityExecutor)
from various_activities import BunchOfActivities

log = logging.getLogger(__name__)

logging.getLogger('awsflow').debug('Blah')


class SWFMixIn(object):
    @pytest.fixture(autouse=True)
    def add_test_args(self, integration_test_args):
        self.test_args = integration_test_args

    def setUp(self):
        self.session = session.get_session()
        self.region = self.test_args['region']
        self.client = self.session.create_client(
            'swf', self.region)

        self.domain = self.test_args['domain']
        self.task_list = self.test_args['tasklist']
        self.workflow_execution = None
        self.workflow_executions = []
        self.serializer = JSONDataConverter()

    def tearDown(self):
        if self.workflow_execution is not None:
            self._terminate_workflow(self.workflow_execution)

        for workflow_execution in self.workflow_executions:
            self._terminate_workflow(workflow_execution)

    def _terminate_workflow(self, workflow_execution):
        try:
            self.client.terminate_workflow_execution(
                childPolicy='TERMINATE',
                domain=self.domain,
                runId=workflow_execution.run_id,
                workflowId=workflow_execution.workflow_id,
                reason='Test Teardown')
        except Exception:
            log.debug("Caught Error trying to terminate workflow:")

    def request_cancel(self, workflow_execution):
        self.client.request_cancel_workflow_execution(
            domain=self.domain,
            runId=workflow_execution.run_id,
            workflowId=workflow_execution.workflow_id)

    def get_workflow_execution_history_with_token(self, workflow_id=None, run_id=None,
                                                  next_page_token=None):
        _workflow_id, _run_id = self.workflow_execution
        if workflow_id is None:
            workflow_id = _workflow_id
        if run_id is None:
            run_id = _run_id

        history = None
        if next_page_token is None:
            history = self.client.get_workflow_execution_history(
                domain=self.domain,
                execution={"workflowId": workflow_id, "runId": run_id})
        else:
            history = self.client.get_workflow_execution_history(
                domain=self.domain,
                nextPageToken=next_page_token,
                execution={"workflowId": workflow_id, "runId": run_id})

        return history['events'], history.get('nextPageToken')

    def get_workflow_execution_history(self, **kwargs):
        return self.get_workflow_execution_history_with_token(**kwargs)[0]

    def get_events(self, history, event_type):
        return [event for event in history if event['eventType'] == event_type]

    def get_scheduled_activities(self, history, activity_name=None):
        scheduled_activities = []
        for event in self.get_events(history, 'ActivityTaskScheduled'):
            name = event['activityTaskScheduledEventAttributes']['activityType']['name']
            if not activity_name or name == activity_name:
                scheduled_activities.append(name)
        return scheduled_activities

    def get_workers(self, workflow_class, threaded_act_worker=False, activities=BunchOfActivities()):
        if not isinstance(workflow_class, list):
            workflow_class = [workflow_class]
        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, *workflow_class)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, activities)
        if threaded_act_worker:
            act_worker = ThreadedActivityExecutor(act_worker)

        return wf_worker, act_worker

    def start_workflow(self, workflow_class, *args, **kwargs):
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = workflow_class.execute(*args, **kwargs)
            self.workflow_execution = instance.workflow_execution
            return instance.workflow_execution
