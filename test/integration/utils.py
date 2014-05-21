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
        self.endpoint = session.get_session() \
                               .get_service('swf') \
                               .get_endpoint(self.test_args['region'])
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
            op = self.endpoint.service.get_operation('TerminateWorkflowExecution')
            op.call(self.endpoint,
                    child_policy='TERMINATE',
                    domain=self.domain,
                    run_id=workflow_execution.run_id,
                    workflow_id=workflow_execution.workflow_id,
                    reason='Test Teardown',
                    )
        except Exception:
            log.debug("Caught Error trying to terminate workflow:", exc_info=True) 

    def get_workflow_execution_history(self, workflow_id=None, run_id=None,
                                       next_page_token=None):
        op = self.endpoint.service.get_operation("GetWorkflowExecutionHistory")
        _workflow_id, _run_id = self.workflow_execution
        if workflow_id is None:
            workflow_id = _workflow_id
        if run_id is None:
            run_id = _run_id

        if next_page_token is None:
            return op.call(self.endpoint, domain=self.domain,
                           execution={"workflow_id": workflow_id,
                                      "run_id": run_id})[1]
        return op.call(self.endpoint, domain=self.domain,
                       next_page_token=next_page_token,
                       execution={"workflow_id": workflow_id,
                                  "run_id": run_id})[1]

