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

from ..core import Future, get_async_context
from ..core.async_task_context import AsyncTaskContext
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
        self._open_cancel_requests = defaultdict(dict)
        self._task_list = task_list

    def handle_event(self, event):
        external_workflow_execution = workflow_execution_from_swf_event(event)
        self._open_cancel_requests[external_workflow_execution]['handler'].send(event)

    def request_cancel_external_workflow_execution(self, external_workflow_execution):
        """Requests cancellation of another workflow.

        :param external_workflow_execution: details of target workflow to cancel
        :type external_workflow_execution: botoflow.workflow_execution.WorkflowExecution
        :return: cancel Future
        :rtype: awsflow.core.future.Future
        """
        self._decider._decisions.append(RequestCancelExternalWorkflowExecution(
            workflow_id=external_workflow_execution.workflow_id,
            run_id=external_workflow_execution.run_id))

        cancel_future = Future()
        context = AsyncTaskContext(False, get_async_context())
        cancel_future.context = context

        handler = self._handle_external_workflow_event(external_workflow_execution, cancel_future)
        six.next(handler)
        self._open_cancel_requests[external_workflow_execution] = {'handler': handler}
        return cancel_future

    def _handle_external_workflow_event(self, external_workflow_execution, cancel_future):
        """Handles external workflow events and resolves open handler(s).

        Events handled:
            RequestCancelExternalWorkflowExecutionInitiated
                - SWF has received decision of canceling an external workflow
            RequestCancelExternalWorkflowExecutionFailed
                - SWF could not send cancel request to external workflow due to invalid workflowID
            ExternalWorkflowExecutionCancelRequested
                - SWF successfully sent cancel request to target external workflow

        :param external_workflow_execution: details of target workflow to cancel
        :type external_workflow_execution: botoflow.workflow_execution.WorkflowExecution
        :param cancel_future:
        :type cancel_future: awsflow.core.future.Future
        :return:
        """
        event = (yield)

        if isinstance(event, RequestCancelExternalWorkflowExecutionInitiated):
            self._decider._decisions.delete_decision(RequestCancelExternalWorkflowExecution,
                                                     external_workflow_execution)
            event = (yield)

            if isinstance(event, ExternalWorkflowExecutionCancelRequested):
                cancel_future.set_result(None)

            elif isinstance(event, RequestCancelExternalWorkflowExecutionFailed):
                attributes = event.attributes
                exception = RequestCancelExternalWorkflowExecutionFailedError(
                    attributes['decisionTaskCompletedEventId'],
                    attributes['initiatedEventId'],
                    attributes['runId'],
                    attributes['workflowId'],
                    attributes['cause'])
                cancel_future.set_exception(exception)

            del self._open_cancel_requests[external_workflow_execution]
