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
import logging

import six

from ..core import async, Future
from ..context import get_context
from ..decisions import CancelWorkflowExecution
from ..exceptions import CancelWorkflowExecutionFailedError, CancelWorkflow
from ..history_events import (WorkflowExecutionCancelRequested, WorkflowExecutionCanceled,
                              CancelWorkflowExecutionFailed)
log = logging.getLogger(__name__)


class CancelWorkflowHandler(object):

    responds_to = (WorkflowExecutionCancelRequested, WorkflowExecutionCanceled,
                   CancelWorkflowExecutionFailed)

    def __init__(self, decider, task_list):
        self._decider = decider
        self._open_cancels = {}
        self._task_list = task_list

    def handle_event(self, event):
        workflow_execution = get_context().workflow_execution
        if isinstance(event, WorkflowExecutionCancelRequested):
            details = self._construct_cancel_details(event)
            self.cancel_workflow_execution(workflow_execution, details)
        else:
            self._open_cancels[workflow_execution]['handler'].send(event)

    def _construct_cancel_details(self, event):
        attributes = event['attributes']
        cause = attributes.get('cause')
        external_initiated_event_id = attributes.get('externalInitiatedEventId')
        external_workflow_execution = attributes.get('externalWorkflowExecution')
        if any(cause, external_initiated_event_id, external_workflow_execution):
            return ("cause={} external_workflow_execution={} "
                    "external_initiated_event_id={} ").format(external_workflow_execution,
                                                              external_initiated_event_id,
                                                              cause)
        return None

    def cancel_workflow_execution(self, workflow_execution, details):
        if self._open_cancels[workflow_execution]:
            # ignore conflicting cancels, or queue them up?
            log.warn(("Received cancel_workflow_execution request while another is in progress. "
                      "workflow_execution={}, details={} -- ignoring.").format(
                          workflow_execution, details))

        try:
            log.info("executing cancellation_handler for {}".format(workflow_execution))
            get_context().workflow._exec_cancellation_handler()
            return
        except CancelWorkflow:
            log.info("{} raised CancelWorkflow; begin cancelling workflow".format(
                workflow_execution))

        self._decider._request_cancel_activity_task_all(workflow_execution)
        self._decisions.append(CancelWorkflowExecution(details))

        cancel_future = Future()
        handler = self.handle_cancel_workflow_event(cancel_future)
        six.next(handler)
        self._open_cancels[workflow_execution] = {'handler': handler}

        @async
        def wait_workflow_cancelled():
            yield cancel_future

        return wait_workflow_cancelled()

    def handle_cancel_workflow_event(self, cancel_future):
        """handles the following cancellation related events:

        CancelWorkflowExecutionFailed
            - SWF failed to intake our cancel decision
        WorkflowExecutionCanceled
            - Workflow successfully canceled
        """
        event = (yield)
        workflow_execution = get_context().workflow_execution

        self._decisions.delete_decision(CancelWorkflowExecution, workflow_execution)
        del self._open_cancels[workflow_execution]

        if isinstance(event, CancelWorkflowExecutionFailed):
            attributes = event.attributes
            exception = CancelWorkflowExecutionFailedError(
                attributes['decisionTaskCompletedEventId'],
                workflow_execution,
                attributes['cause'])
            cancel_future.set_exception(exception)
        elif isinstance(event, WorkflowExecutionCanceled):
            # successfully canceled; shouldn't receive further events from SWF
            # set_exception() or just rely on natural closure?
            cancel_future.set_result(None)
