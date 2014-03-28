# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
#
import time
import logging

from .context import StartWorkflowContext, get_context, set_context
from .workers.swf_op_callable import SWFOp
from .utils import random_sha1_hash

log = logging.getLogger(__name__)


class WorkflowStarter(object):
    """Use this context manager to start a new workflow execution

    Example:

    .. code-block:: python

        # start the workflow using botocore session and ExampleWorkflow class
        # with a random workflow_id
        with WorkflowStarter(endpoint, "SOMEDOMAIN", "DEFAULT_TASKLIST"):
            instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
            print instance.workflow_execution.workflow_id
            # will print the workflow execution ID
    """

    def __init__(self, endpoint, domain, default_task_list):
        self.domain = domain
        self.task_list = default_task_list

        _op = endpoint.service.get_operation("StartWorkflowExecution")
        self._start_workflow_execution_op = SWFOp(endpoint, _op)
        _op = endpoint.service.get_operation("SignalWorkflowExecution")
        self._signal_workflow_execution_op = SWFOp(endpoint, _op)
        _op = endpoint.service.get_operation("DescribeWorkflowExecution")
        self._describe_workflow_execution_op = SWFOp(endpoint, _op)
        _op = endpoint.service.get_operation("GetWorkflowExecutionHistory")
        self._get_workflow_execution_history_op = SWFOp(endpoint, _op)

    def __enter__(self):
        try:
            self._other_context = get_context()
        except AttributeError:
            self._other_context = None

        set_context(StartWorkflowContext(self))
        return self

    def __exit__(self, type, value, traceback):
        set_context(self._other_context)

    def wait_for_completion(self, workflow_instance, poll_time, check_count=None):
        attempt_nr = 0
        while check_count is None or attempt_nr < check_count:
            time.sleep(poll_time)

            execution_status, close_status = self._get_workflow_execution_status(
                workflow_instance.workflow_execution)

            if execution_status == 'OPEN':
                continue

            if close_status == 'COMPLETED':
                return self._load_workflow_execution_result(
                    workflow_instance.workflow_execution,
                    workflow_instance._data_converter)

    def _get_workflow_execution_status(self, workflow_execution):
        workflow_execution = self._describe_workflow_execution_op(
            domain=self.domain,
            execution={'workflow_id': workflow_execution.workflow_id,
                       'run_id': workflow_execution.run_id})


        execution_status = workflow_execution['executionInfo']['executionStatus']
        if execution_status != 'OPEN':
            return execution_status, workflow_execution['executionInfo']['closeStatus']
        return execution_status, None

    def _load_workflow_execution_result(self, workflow_execution, data_converter):
        last_event = self._get_workflow_execution_history_op(
            domain=self.domain,
            execution={'workflow_id': workflow_execution.workflow_id,
                       'run_id': workflow_execution.run_id})['events'][-1]

        return data_converter.loads(
            last_event['workflowExecutionCompletedEventAttributes']['result'])

    def _load_failed_workflow_execution_result(self, workflow_execution, data_converter):
        last_event = self._get_workflow_execution_history_op(
            domain=self.domain,
            execution={'workflow_id': workflow_execution.workflow_id,
                       'run_id': workflow_execution.run_id})['events'][-1]

        return data_converter.loads(
            last_event['workflowExecutionFailedEventAttributes']['details'])

    def _start_workflow_execution(self, workflow_type, *args, **kwargs):
        """Calls SWF to start the workflow using our workflow_type"""
        decision_dict = workflow_type.to_decision_dict(
            [args, kwargs], random_sha1_hash(), self.task_list,
            self.domain)

        log.debug("Starting workflow execution with args: %s",
                  decision_dict)

        response = self._start_workflow_execution_op(**decision_dict)
        return decision_dict['workflow_id'], response['runId']

