from botocore import session

from awsflow.data_converter import JSONDataConverter


class SWFMixIn(object):

    def setUp(self):
        self.endpoint = session.get_session() \
                               .get_service('swf') \
                               .get_endpoint('us-east-1')
        self.domain = 'mydomain2'
        self.task_list = 'testlist'
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
            self.swf_client.terminate_workflow_execution(
                self.domain, self.workflow_execution.workflow_id,
                run_id=self.workflow_execution.run_id)
        except Exception:
            pass

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

