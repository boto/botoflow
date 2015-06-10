# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import itertools
import logging
import warnings

from ..context import get_context, set_context, DecisionContext
from ..workflow_execution import WorkflowExecution
from ..core import Future, AsyncEventLoop
from ..utils import pairwise
from ..swf_exceptions import swf_exception_wrapper
from ..history_events import (DecisionTaskCompleted, DecisionTaskScheduled, DecisionTaskTimedOut, DecisionTaskStarted,
                              DecisionEventBase)
from ..decisions import DecisionList
from .decision_task_poller import DecisionTaskPoller
from .workflow_execution_handler import WorkflowExecutionHandler
from .activity_task_handler import ActivityTaskHandler
from .child_workflow_execution_handler import ChildWorkflowExecutionHandler
from .timer_handler import TimerHandler
from .cancel_workflow_handler import CancelWorkflowHandler
from .external_workflow_handler import ExternalWorkflowHandler

log = logging.getLogger(__name__)


class Decider(object):

    def __init__(self, worker, domain, task_list, get_workflow, identity, _Poller=DecisionTaskPoller):
        """

        :param worker:
        :type worker: awsflow.workers.base_worker.BaseWorker
        :param domain:
        :type domain: str
        :param task_list:
        :type task_list: str
        :param get_workflow:
        :type get_workflow: function
        :param identity:
        :type identity: str
        :param _Poller:
        :type _Poller: awsflow.decider.decision_task_poller.DecisionTaskPoller
        """
        self.worker = worker
        self.domain = domain
        self.task_list = task_list
        self.identity = identity
        self.get_workflow = get_workflow

        self._poller = _Poller(worker, domain, task_list, identity)

    def _reset(self):
        self.execution_started = False

        self._decisions = DecisionList()
        self._decision_id = 0
        self._event_to_id_table = {}
        self._decision_task_token = None
        self._eventloop = AsyncEventLoop()

        self._workflow_execution_handler = WorkflowExecutionHandler(self, self.task_list)
        self._activity_task_handler = ActivityTaskHandler(self, self.task_list)
        self._child_workflow_execution_handler = ChildWorkflowExecutionHandler(self, self.task_list)
        self._timer_handler = TimerHandler(self, self.task_list)
        self._cancel_workflow_handler = CancelWorkflowHandler(self, self.task_list)
        self._external_workflow_handler = ExternalWorkflowHandler(self, self.task_list)

        # basically garbage collect
        Future.untrack_all_coroutines()

    def get_next_id(self):
        self._decision_id += 1
        return str(self._decision_id)

    def decide(self):
        self._reset()

        prev_context = None
        context = DecisionContext(self)

        # some events might come in in the middle of decision events, we
        # reorder them to look like they came in after or replaying won't work
        decision_started = False
        reordered_events = list()
        decision_start_to_completion_events = list()
        decision_completion_to_start_events = list()
        concurrent_to_decision = True
        last_decision_index = -1

        decision_task = self._poller.poll()
        if decision_task is None:
            return

        workflow_execution = WorkflowExecution(decision_task.workflow_id, decision_task.run_id)

        self._decision_task_token = decision_task.task_token
        non_replay_event_id = decision_task.previous_started_event_id
        context.workflow_execution = workflow_execution

        try:
            try:
                prev_context = get_context()
            except AttributeError:
                pass

            set_context(context)
            for event, next_event in pairwise(decision_task.events):
                # convert the event dictionary to an object
                if isinstance(event, DecisionTaskCompleted):
                    concurrent_to_decision = False
                    decision_started = False
                elif isinstance(event, DecisionTaskStarted):
                    decision_started = True
                    if next_event is None or not isinstance(next_event, DecisionTaskTimedOut):
                        get_context()._workflow_time = event.datetime
                elif isinstance(event, (DecisionTaskScheduled, DecisionTaskTimedOut)):
                    continue
                else:
                    if concurrent_to_decision:
                        decision_start_to_completion_events.append(event)
                    else:
                        if isinstance(event, DecisionEventBase):
                            last_decision_index = len(
                                decision_completion_to_start_events)
                        decision_completion_to_start_events.append(event)

                if decision_started:
                    if last_decision_index > -1:
                        reordered_events = decision_completion_to_start_events

                    reordered_events = itertools.chain(
                        reordered_events, decision_start_to_completion_events)
                    for event in reordered_events:
                        if event.id >= non_replay_event_id:
                            get_context()._replaying = False
                        self._handle_history_event(workflow_execution, event)

                    reordered_events = list()
                    decision_completion_to_start_events = list()
                    decision_start_to_completion_events = list()
                    decision_started = False

            self._process_decisions()
        finally:
            set_context(prev_context)

    def _process_decisions(self):
        # drain all tasks before submitting more decisions
        self._eventloop.execute_all_tasks()

        if self._decision_task_token is not None:
            # get the workflow_state (otherwise known as execution context)
            # workflow is attached to context by _workflow_execution_handler
            workflow_state = get_context().workflow.workflow_state

            log.debug("Sending workflow decisions: %s", self._decisions)
            with swf_exception_wrapper():
                self.worker.client.respond_decision_task_completed(
                    taskToken=self._decision_task_token,
                    decisions=self._decisions.to_swf(),
                    executionContext=workflow_state)

    def _handle_history_event(self, workflow_execution, event):
        log.debug("Handling history event: %s", event)

        try:
            for handler in (self._workflow_execution_handler, self._activity_task_handler,
                            self._child_workflow_execution_handler, self._timer_handler,
                            self._cancel_workflow_handler):
                if isinstance(event, handler.responds_to):
                    handler.handle_event(event)
                    break
            warnings.warn("Handler for the event {} not implemented".format(event))
        except StopIteration:
            pass
        self._eventloop.execute_all_tasks()

    def _handle_execute_activity(self, activity_type, decision_dict, args, kwargs):
        return self._activity_task_handler.handle_execute_activity(
            activity_type, decision_dict, args, kwargs)

    def _handle_start_child_workflow_execution(self, workflow_type, workflow_instance, input):
        return self._child_workflow_execution_handler.handle_start_child_workflow_execution(
            workflow_type, workflow_instance, input)

    def _cancel_workflow_execution(self, workflow_execution, details):
        """An execution-internal cancellation.

        Returns async call that waits until (1) the cancellation handler for the
        respective workflow definition is executed, and (2) activities/workflow
        is canceled through SWF as necessary (based upon cancellation handler).
        """
        return self._cancel_workflow_handler.cancel_workflow_execution(
            workflow_execution, details)

    def _request_cancel_external_workflow_execution(self, external_workflow_execution):
        """RequestCancelExternalWorkflowExecution sends a cancel request to the target
        external workflow execution. It is up to the target execution whether to
        allow the request to go through or not.

        Returns async call that waits until the request is sent to target external
        execution.
        """
        return self._external_workflow_handler.request_cancel_external_workflow_execution(
            external_workflow_execution)

    def _request_cancel_activity_task(self, workflow_execution, activity_id):
        """RequestCancelActivityTask sends cancel request to activity for given id.

        Returns None; does not wait for a response (TODO - implement async wait path)
        """
        return self._activity_task_handler.request_cancel_activity_task(
            workflow_execution, activity_id)

    def _request_cancel_activity_task_all(self, workflow_execution):
        """RequestCancelActivityTask decision for all open activities of given execution"""
        return self._activity_task_handler.request_cancel_activity_task_all(workflow_execution)

    def _continue_as_new_workflow_execution(self, **kwargs):
        """
        ContinueAsNewWorkflowExecution closes the workflow execution and
        starts a new workflow execution of the same type using the same
        workflow id and a unique run Id. A WorkflowExecutionContinuedAsNew
        event is recorded in the history.
        """
        self._workflow_execution_handler.continue_as_new_workflow_execution(**kwargs)

    def handle_execute_timer(self, seconds):
        return self._timer_handler.handle_execute_timer(seconds)
