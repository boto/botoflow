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

from ..core import Future, async, return_, CancelledError, get_async_context
from ..core.async_task_context import AsyncTaskContext
from ..utils import camel_keys_to_snake_case
from ..context import get_context
from ..workflow_execution import workflow_execution_from_swf_event
from ..decisions import StartChildWorkflowExecution
from ..exceptions import (ChildWorkflowFailedError, ChildWorkflowTimedOutError, ChildWorkflowTerminatedError,
                          StartChildWorkflowExecutionFailedError)
from ..history_events import (StartChildWorkflowExecutionInitiated, StartChildWorkflowExecutionFailed,
                              ChildWorkflowExecutionStarted, ChildWorkflowExecutionCompleted,
                              ChildWorkflowExecutionFailed, ChildWorkflowExecutionCanceled,
                              ChildWorkflowExecutionTimedOut, ChildWorkflowExecutionTerminated)

log = logging.getLogger(__name__)


class ChildWorkflowExecutionHandler(object):

    responds_to = (StartChildWorkflowExecutionFailed, StartChildWorkflowExecutionInitiated,
                   ChildWorkflowExecutionCompleted, ChildWorkflowExecutionFailed, ChildWorkflowExecutionStarted,
                   ChildWorkflowExecutionTerminated, ChildWorkflowExecutionTimedOut, ChildWorkflowExecutionCanceled)

    def __init__(self, decider, task_list):
        """

        :param decider: Decider
        :type decider: awsflow.decider.decider.Decider
        :param task_list: task list
        :type task_list: str
        :return:
        """
        self._decider = decider
        self._event_to_workflow_id = {}
        self._open_child_workflows = {}
        self._task_list = task_list

    def handle_start_child_workflow_execution(self, workflow_type, workflow_instance, wf_input):
        run_id = get_context().workflow_execution.run_id
        workflow_id = "%s:%s" % (run_id, self._decider.get_next_id())

        decision_dict = camel_keys_to_snake_case(
            workflow_type.to_decision_dict(wf_input, workflow_id, self._task_list))

        decision = StartChildWorkflowExecution(**decision_dict)
        self._decider._decisions.append(decision)

        log.debug("Workflow start child workflow execution: %s", decision)

        workflow_id = decision_dict['workflow_id']

        # set the future that represents the result of our activity
        workflow_future = Future()
        context = AsyncTaskContext(False, get_async_context())

        workflow_future.context = context
        workflow_started_future = Future()
        workflow_started_future.context = context

        handler = self._handler_fsm(workflow_type,
                                    workflow_id,
                                    workflow_future)
        six.next(handler)  # arm
        self._open_child_workflows[workflow_id] = {
            'future': workflow_future, 'handler': handler,
            'workflowInstance': workflow_instance,
            'workflowStartedFuture': workflow_started_future}

        @async
        def wait_workflow_start():
            try:
                _workflow_instance = yield workflow_started_future
                return_(_workflow_instance)
            except GeneratorExit:
                pass

        @async
        def wait_child_workflow_complete():
            try:
                result = yield workflow_future
                # XXX handle CancellationError
                return_(result)
            except GeneratorExit:
                pass

        workflow_instance._workflow_result = wait_child_workflow_complete()
        return wait_workflow_start()

    def handle_event(self, event):
        workflow_id = None
        if isinstance(event, (StartChildWorkflowExecutionInitiated, StartChildWorkflowExecutionFailed)):
            workflow_id = event.attributes['workflowId']

        elif isinstance(event, (ChildWorkflowExecutionStarted, ChildWorkflowExecutionCompleted,
                                ChildWorkflowExecutionFailed, ChildWorkflowExecutionTimedOut,
                                ChildWorkflowExecutionTerminated, ChildWorkflowExecutionCanceled)):
            scheduled_event_id = event.attributes['initiatedEventId']
            workflow_id = self._event_to_workflow_id[scheduled_event_id]

        if workflow_id is not None:
            self._open_child_workflows[workflow_id]['handler'].send(event)
        else:
            log.warn("Tried to handle child workfow event, but workflow_id was None: %r", event)

    def _handler_fsm(self, workflow_type, workflow_id, workflow_future):
        """

        :param workflow_type:
        :param workflow_id:
        :param workflow_future:
        :type workflow_future: awsflow.core.Future
        :return:
        """
        event = (yield)
        # TODO support ChildWorkflowExecutionTerminated event

        if isinstance(event, (StartChildWorkflowExecutionInitiated, StartChildWorkflowExecutionFailed)):
            self._decider._decisions.delete_decision(
                StartChildWorkflowExecution, (workflow_type.name, workflow_type.version, workflow_id))

        if isinstance(event, StartChildWorkflowExecutionInitiated):
            # need to be able to find the workflow id as it's not always
            # present in the history
            self._event_to_workflow_id[event.id] = workflow_id

            event = (yield)
            if isinstance(event, ChildWorkflowExecutionStarted):
                # set the workflow_execution information on the instance
                workflow_instance = self._open_child_workflows[workflow_id]['workflowInstance']
                workflow_started_future = self._open_child_workflows[workflow_id]['workflowStartedFuture']

                workflow_instance.workflow_execution = workflow_execution_from_swf_event(event)
                workflow_started_future.set_result(workflow_instance)
            elif isinstance(event, StartChildWorkflowExecutionFailed):
                # set the exception with a cause
                cause = event.attributes['cause']
                workflow_started_future = self._open_child_workflows[workflow_id]['workflowStartedFuture']

                workflow_started_future.set_exception(
                    StartChildWorkflowExecutionFailedError(cause))

            event = (yield)
            if isinstance(event, ChildWorkflowExecutionCompleted):
                result = workflow_type.data_converter.loads(event.attributes['result'])
                workflow_future.set_result(result)

            elif isinstance(event, ChildWorkflowExecutionCanceled):
                workflow_future.set_exception(CancelledError(event.attributes['details']))

            elif isinstance(event, ChildWorkflowExecutionFailed):
                exception, _traceback = workflow_type.data_converter.loads(event.attributes['details'])

                error = ChildWorkflowFailedError(
                    event.id, workflow_type,
                    get_context()._workflow_execution, cause=exception,
                    _traceback=_traceback)
                workflow_future.set_exception(error)

            elif isinstance(event, ChildWorkflowExecutionTimedOut):
                error = ChildWorkflowTimedOutError(
                    event.id, workflow_type,
                    get_context()._workflow_execution)
                workflow_future.set_exception(error)

            elif isinstance(event, ChildWorkflowExecutionTerminated):
                error = ChildWorkflowTerminatedError(
                    event.id, workflow_type,
                    get_context()._workflow_execution)
                workflow_future.set_exception(error)
            else:
                raise RuntimeError("Unexpected event/state: %s", event)

        elif isinstance(event, StartChildWorkflowExecutionFailed):
            # set the exception with a cause
            cause = event.attributes['cause']
            workflow_started_future = self._open_child_workflows[workflow_id]['workflowStartedFuture']

            workflow_started_future.set_exception(
                StartChildWorkflowExecutionFailedError(cause))

        else:
            raise RuntimeError("Unexpected event/state: %s", event)

        del self._open_child_workflows[workflow_id]  # child workflow done


