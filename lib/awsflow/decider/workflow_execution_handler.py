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

from ..context import get_context
from ..core import async
from ..core import async_traceback

from ..decisions import (CompleteWorkflowExecution, DecisionList, FailWorkflowExecution,
                         ContinueAsNewWorkflowExecution, CancelWorkflowExecution)
from ..exceptions import CancelledError
from ..history_events import (WorkflowExecutionStarted, WorkflowExecutionSignaled,
                              WorkflowExecutionCancelRequested)
from ..workflow_types import WorkflowType

log = logging.getLogger(__name__)


class WorkflowExecutionHandler(object):

    responds_to = (WorkflowExecutionStarted, WorkflowExecutionSignaled,
                   WorkflowExecutionCancelRequested)

    def __init__(self, decider, task_list):
        self._decider = decider
        self._data_converter = WorkflowType.DEFAULT_DATA_CONVERTER
        self._continue_as_new_on_completion = None
        self._task_list = task_list
        self._future = None

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
            value = self._data_converter.loads(event.attributes['input'])

            # this makes it easier to submit keyword argument inputs from Java workflows.
            if isinstance(value, (tuple, list)):
                try:
                    args, kwargs = value
                except ValueError:  # malformed list-like
                    log.error("Malformed list-like input: wrong item count: %d", len(value))
                    raise
            elif isinstance(value, dict):
                args = []
                kwargs = value
            else:
                # last ditch; try for list-like behaviour, but...
                args, kwargs = value

            return args, kwargs


    def handle_event(self, event):
        if isinstance(event, WorkflowExecutionStarted):
            self._handle_workflow_execution_started(event)
        elif isinstance(event, WorkflowExecutionSignaled):
            self._signal_workflow_execution(event)
        elif isinstance(event, WorkflowExecutionCancelRequested):
            self._handle_cancel_request(event)
        else:
            log.warn("Tried to handle workflow event, but a handler is missing: %r", event)

    def _handle_cancel_request(self, event):
        """Sets workflow execution future to CancelledError

        :param event: cancel request event
        :type event: awsflow.history_events.WorkflowExecutionCancelRequested
        :return:
        """
        if self._decider._decisions.has_decision_type(CompleteWorkflowExecution,
                                                      ContinueAsNewWorkflowExecution):
            # workflow completed earlier during same decision making segment
            return

        # TODO: ability to shield from cancellation requests
        self._future.set_exception(CancelledError())

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

        context._workflow_instance = self._workflow_instance

        args, kwargs = self._load_input(event)

        @async
        def handle_execute():
            try:
                self._future = execute_method(*args, **kwargs)
                # any subsequent executions will be counted "continue as new"
                self._decider.execution_started = True
                execute_result = yield self._future
                # XXX should these be the only decisions?

                if self._continue_as_new_on_completion is None:
                    log.debug("Workflow execute() returned: %s", execute_result)
                    self._decider._decisions.append(CompleteWorkflowExecution(
                        workflow_type.data_converter.dumps(execute_result)))
                else:
                    log.debug("ContinueAsNew: %s", self._continue_as_new_on_completion)
                    self._decider._decisions.append(self._continue_as_new_on_completion)

            except CancelledError as err:
                yield context._workflow_instance.cancellation_handler()
                self._decider._request_cancel_activity_task_all()
                self._decider._decisions.append(CancelWorkflowExecution(str(err.cause)))

            except Exception as err:
                tb_list = async_traceback.extract_tb()
                log.debug("Workflow execute() raised an exception:\n%s",
                          "".join(traceback.format_exc()))

                # clean any lingering decisions as we're about to terminate the execution
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

        context._workflow_instance._workflow_signals[signal_name][1](
            context._workflow_instance, *args, **kwargs)

    def continue_as_new_workflow_execution(self, **kwargs):
        self._continue_as_new_on_completion = ContinueAsNewWorkflowExecution(**kwargs)
