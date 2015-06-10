# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
from collections import defaultdict
import logging

import six

from ..core import async, Future
from ..context import get_context
from ..decisions import RequestCancelExternalWorkflowExecution
from ..exceptions import RequestCancelExternalWorkflowExecutionFailedError
from ..workflow_execution import workflow_execution_from_swf_event
from ..history_events import (ExternalWorkflowExecutionCancelRequested,
                              RequestCancelExternalWorkflowExecutionInitiated,
                              RequestCancelExternalWorkflowExecutionFailed)
log = logging.getLogger(__name__)


class ExternalWorkflowHandler(object):

    responds_to = (ExternalWorkflowExecutionCancelRequested,
                   RequestCancelExternalWorkflowExecutionInitiated,
                   RequestCancelExternalWorkflowExecutionFailed)

    def __init__(self, decider, task_list):
        self._decider = decider
        self._open_cancellation_requests = defaultdict(dict)
        self._task_list = task_list

    def handle_event(self, event):
        workflow_execution = get_context().workflow_execution
        external_workflow_execution = workflow_execution_from_swf_event(event)
        self._open_cancellation_requests[workflow_execution][external_workflow_execution][
            'handler'].send(event)

    def request_cancel_external_workflow_execution(self, external_workflow_execution):
        self._decider._decisions.append(RequestCancelExternalWorkflowExecution(
            workflow_id=external_workflow_execution.workflow_id,
            run_id=external_workflow_execution.run_id))

        workflow_execution = get_context().workflow_execution
        workflow_future = Future()
        handler = self.handle_external_workflow_event(external_workflow_execution, workflow_future)
        six.next(handler)
        self._open_cancellation_requests[workflow_execution][external_workflow_execution] = {
            'handler': handler}

        @async
        def wait_request_cancel_workflow_execution():
            yield workflow_future

        return wait_request_cancel_workflow_execution()

    def handle_external_workflow_event(self, external_workflow_execution, workflow_future):
        """handles the following events:

        RequestCancelExternalWorkflowExecutionInitiated
            - SWF has received decision of canceling an external workflow
        RequestCancelExternalWorkflowExecutionFailed
            - SWF could not send cancel request to external workflow due to invalid workflowID
        ExternalWorkflowExecutionCancelRequested
            - SWF successfully sent cancel request to target external workflow
        """
        event = (yield)
        if isinstance(event, RequestCancelExternalWorkflowExecutionInitiated):
            self._decider._decisions.delete_decision(RequestCancelExternalWorkflowExecution,
                                                     external_workflow_execution)
        elif isinstance(event, ExternalWorkflowExecutionCancelRequested):
            workflow_future.set_result(None)
        elif isinstance(event, RequestCancelExternalWorkflowExecutionFailed):
            attributes = event.attributes
            exception = RequestCancelExternalWorkflowExecutionFailedError(
                attributes['decisionTaskCompletedEventId'],
                attributes['initiatedEventId'],
                attributes['runId'],
                attributes['workflowId'],
                attributes['cause'])
            workflow_future.set_exception(exception)
