import logging

import pytest
from botocore import session

from awsflow.data_converter import JSONDataConverter

log = logging.getLogger(__name__)


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
                execution={"workflowId": workflow_id,
                "runId": run_id})
        else:
            history = self.client.get_workflow_execution_history(
                domain=self.domain,
                nextPageToken=next_page_token,
                execution={"workflowId": workflow_id, "runId": run_id})

        return history['events'], history.get('nextPageToken')

    def get_workflow_execution_history(self, **kwargs):
        return self.get_workflow_execution_history_with_token(**kwargs)[0]

