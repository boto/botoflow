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
from ..history_events import (DecisionTaskCompleted, DecisionTaskScheduled, DecisionTaskTimedOut,
                              DecisionTaskStarted, DecisionEventBase, CancelWorkflowExecutionFailed)
from ..decisions import DecisionList, CancelWorkflowExecution
from .decision_task_poller import DecisionTaskPoller
from .workflow_execution_handler import WorkflowExecutionHandler
from .activity_task_handler import ActivityTaskHandler
from .child_workflow_execution_handler import ChildWorkflowExecutionHandler
from .timer_handler import TimerHandler
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

        # noinspection PyCallingNonCallable
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
        self._external_workflow_handler = ExternalWorkflowHandler(self, self.task_list)

        self._handlers = (self._workflow_execution_handler, self._activity_task_handler,
                          self._child_workflow_execution_handler, self._timer_handler,
                          self._external_workflow_handler)

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
                    concurrent_to_decision = True
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
                    for ord_event in reordered_events:
                        if ord_event.id >= non_replay_event_id:
                            get_context()._replaying = False
                        self._handle_history_event(ord_event)

                        if self._decisions.has_decision_type(CancelWorkflowExecution):
                            # peak ahead to see if this is a retry
                            if decision_task.events.contains(CancelWorkflowExecutionFailed):
                                self._retry_cancellation(context)
                            else:
                                # first cancel attempt
                                self._process_decisions()
                            return  # cancel decision was made; do not collect further decisions

                    reordered_events = list()
                    decision_completion_to_start_events = list()
                    decision_start_to_completion_events = list()
                    decision_started = False

            self._process_decisions()
        finally:
            set_context(prev_context)

    def _handle_history_event(self, event):
        log.debug("Handling history event: %s", event)

        try:
            handler = next(handler for handler in self._handlers if isinstance(event, handler.responds_to))
            try:
                handler.handle_event(event)
            except StopIteration:  # error raised when event is sent to already closed future
                pass
        except StopIteration:
            warnings.warn("Handler for the event {} not implemented".format(event))

        self._eventloop.execute_all_tasks()

    def _process_decisions(self):
        # drain all tasks before submitting more decisions
        self._eventloop.execute_all_tasks()

        if self._decision_task_token is not None:
            # get the workflow_state (otherwise known as execution context)
            # workflow is attached to context by _workflow_execution_handler
            workflow_state = get_context()._workflow_instance.workflow_state

            log.debug("Sending workflow decisions: %s", self._decisions)
            with swf_exception_wrapper():
                self.worker.client.respond_decision_task_completed(
                    taskToken=self._decision_task_token,
                    decisions=self._decisions.to_swf(),
                    executionContext=workflow_state)

    def _retry_cancellation(self, context):
        """A CancelWorkflowExecutionFailed event occurs when pending decisions leftover;
        this resends the cancel decision, alone, to retry.

        See note on this edge case:
        http://docs.aws.amazon.com/amazonswf/latest/apireference/API_Decision.html
        """
        self._decisions = DecisionList()
        self._decisions.append(CancelWorkflowExecution('retry'))
        with swf_exception_wrapper():
            self.worker.client.respond_decision_task_completed(
                taskToken=self._decision_task_token,
                decisions=self._decisions.to_swf(),
                executionContext=context._workflow_instance.workflow_state)

    def _handle_execute_activity(self, activity_type, decision_dict, args, kwargs):
        return self._activity_task_handler.handle_execute_activity(
            activity_type, decision_dict, args, kwargs)

    def _handle_start_child_workflow_execution(self, workflow_type, workflow_instance, wf_input):
        return self._child_workflow_execution_handler.handle_start_child_workflow_execution(
            workflow_type, workflow_instance, wf_input)

    def _request_cancel_external_workflow_execution(self, external_workflow_execution):
        """RequestCancelExternalWorkflowExecution sends a cancel request to the target
        external workflow execution. It is up to the target execution whether to
        allow the request to go through or not.

        :param external_workflow_execution: target execution for cancellation
        :type external_workflow_execution: botoflow.workflow_definition.WorkflowDefininition
        :return: cancel Future
        :rtype: awsflow.core.Future
        """
        return self._external_workflow_handler.request_cancel_external_workflow_execution(
            external_workflow_execution)

    def _request_cancel_activity_task_all(self):
        """RequestCancelActivityTask decision for all open activities of current execution.

        :return: all futures for cancel requests
        :rtype: awsflow.core.future.AllFuture
        """
        return self._activity_task_handler.request_cancel_activity_task_all()

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
