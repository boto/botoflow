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

import traceback
import logging

import six

from ..context import get_context
from ..core import async
from ..core import async_traceback, Future, BaseFuture

from ..decisions import (CompleteWorkflowExecution, DecisionList, FailWorkflowExecution,
                         ContinueAsNewWorkflowExecution, CancelWorkflowExecution)
from ..exceptions import CancelWorkflowExecutionFailedError, CancelWorkflow
from ..history_events import (WorkflowExecutionStarted, WorkflowExecutionSignaled,
                              WorkflowExecutionCancelRequested, CancelWorkflowExecutionFailed)
from ..workflow_types import WorkflowType

log = logging.getLogger(__name__)


class WorkflowExecutionHandler(object):

    responds_to = (WorkflowExecutionStarted, WorkflowExecutionSignaled,
                   WorkflowExecutionCancelRequested, CancelWorkflowExecutionFailed)

    def __init__(self, decider, task_list):
        self._decider = decider
        self._data_converter = WorkflowType.DEFAULT_DATA_CONVERTER
        self._continue_as_new_on_completion = None
        self._task_list = task_list
        self._open_cancel = None

    def _load_input(self, event):
        """Load initial workflow input data

        :param event:
        :type event: awsflow.history_events.WorkflowExecutionStarted
        :return: tuple of list,dict to feed as *args, *kwargs
        :rtype: tuple
        """
        if 'input' not in event.attributes:
            return [], {}
        else:
            return self._data_converter.loads(event.attributes['input'])

    def handle_event(self, event):
        if isinstance(event, WorkflowExecutionStarted):
            self._handle_workflow_execution_started(event)
        elif isinstance(event, WorkflowExecutionSignaled):
            self._signal_workflow_execution(event)
        elif isinstance(event, WorkflowExecutionCancelRequested):
            details = self._construct_cancel_details(event)
            self.cancel_workflow_execution(details)
        elif isinstance(event, CancelWorkflowExecutionFailed):
            self._open_cancel['handler'].send(event)
        else:
            log.warn("Tried to handle workflow event, but a handler is missing: %r", event)

    def _construct_cancel_details(self, event):
        """Deconstructs WorkflowExecutionCancelRequested event to generate
        details string to send with cancel request decision.

        :param event:
        :type event: awsflow.history_events.WorkflowExecutionCancelRequested
        :return: cancel request details
        :rtype: string
        """
        attributes = event['attributes']
        cause = attributes.get('cause')
        external_initiated_event_id = attributes.get('externalInitiatedEventId')
        external_workflow_execution = attributes.get('externalWorkflowExecution')
        if any(cause, external_initiated_event_id, external_workflow_execution):
            return ("cause={} external_workflow_execution={} "
                    "external_initiated_event_id={} ").format(cause,
                                                              external_workflow_execution,
                                                              external_initiated_event_id)
        return ""

    def cancel_workflow_execution(self, details):
        """Makes CancelWorkflowExecution decision and creates associated future.

        :return: cancel future
        :rtype: awsflow.core.Future
        """
        if self._open_cancel:  # duplicate request
            return self._open_cancel['future']

        context = get_context()
        cascade = False
        try:
            context.workflow.cancellation_handler(details)
            return BaseFuture.with_result(None)
        except CancelWorkflow as error:
            log.info("{} raised CancelWorkflow; begin cancelling workflow".format(
                context.workflow_execution))
            cascade = error.cascade_cancel_to_activities

        if cascade:
            self._decider._request_cancel_activity_task_all()
        decision_id = self._decider.get_next_id()
        self._decider._decisions.append(CancelWorkflowExecution(decision_id, details))

        cancel_future = Future()
        handler = self._handle_cancel_workflow_event(decision_id, cancel_future)
        six.next(handler)
        self._open_cancel = {'future': cancel_future, 'decision_id': decision_id}
        return cancel_future

    def _handle_workflow_execution_started(self, event):
        """Handle WorkflowExecutionStarted event

        :param event:
        :type event: awsflow.history_events.WorkflowExecutionStarted
        :return:
        """
        context = get_context()
        # find the workflow we're working with
        workflow_name = event.attributes['workflowType']['name']
        workflow_version = event.attributes['workflowType']['version']

        # find the workflow class based ont the event information
        workflow_definition, workflow_type, func_name = self._decider.get_workflow(workflow_name,
                                                                                   workflow_version)

        # instantiate workflow
        self._workflow_instance = workflow_definition(context._workflow_execution)

        # set the data converter used by us
        self._data_converter = workflow_type.data_converter

        # find the execution method
        execute_method = getattr(self._workflow_instance, func_name)

        context.workflow = self._workflow_instance

        args, kwargs = self._load_input(event)

        @async
        def handle_execute():
            try:
                future = execute_method(*args, **kwargs)
                # any subsequent executions will be counted "continue as new"
                self._decider.execution_started = True
                execute_result = yield future
                # XXX should these be the only decisions?

                if self._continue_as_new_on_completion is None:
                    if self._open_cancel:
                        # delete unscheduled cancel decision
                        self._decider._decisions.delete_decision(
                            CancelWorkflowExecution, self._open_cancel['decision_id'])
                        self._open_cancel['future'].set_result(None)
                        self._open_cancel = None

                    log.debug("Workflow execute() returned: %s", execute_result)
                    self._decider._decisions.append(CompleteWorkflowExecution(
                        workflow_type.data_converter.dumps(execute_result)))
                elif not self._open_cancel:
                    log.debug("ContinueAsNew: %s", self._continue_as_new_on_completion)
                    self._decider._decisions.append(self._continue_as_new_on_completion)

            except Exception as err:
                tb_list = async_traceback.extract_tb()
                log.debug("Workflow execute() raised an exception:\n%s",
                          "".join(traceback.format_exc()))
                # clean any lingering decisions as we're about to terminate the execution
                # and other decisions may conflict (i.e. CancelWorkflowExecution)
                self._decider._decisions = DecisionList()
                self._decider._decisions.append(FailWorkflowExecution(
                    '', workflow_type.data_converter.dumps([err, tb_list])))

        with self._decider._eventloop:
            handle_execute()  # schedule

        # wait for all the tasks to complete
        self._decider._eventloop.execute_all_tasks()

    def _signal_workflow_execution(self, event):
        """Process the signalling event

        :param event: event
        :type event: awsflow.history_events.WorkflowExecutionSignaled
        :return:
        """
        context = get_context()
        args, kwargs = self._load_input(event)
        signal_name = event.attributes['signalName']

        context.workflow._workflow_signals[signal_name][1](
            context.workflow, *args, **kwargs)

    def _handle_cancel_workflow_event(self, decision_id, cancel_future):
        """Handles cancellation related events to resolve the request future

        CancelWorkflowExecutionFailed
            - SWF failed to intake our cancel decision

        Note: Successful cancel event will not get sent back.
        """
        event = (yield)
        workflow_execution = get_context().workflow_execution

        self._decider._decisions.delete_decision(CancelWorkflowExecution, decision_id)
        self._open_cancel = None

        if isinstance(event, CancelWorkflowExecutionFailed):
            raise CancelWorkflowExecutionFailedError(
                event.attributes['decisionTaskCompletedEventId'],
                workflow_execution,
                event.attributes['cause'])

    def continue_as_new_workflow_execution(self, **kwargs):
        self._continue_as_new_on_completion = ContinueAsNewWorkflowExecution(**kwargs)
