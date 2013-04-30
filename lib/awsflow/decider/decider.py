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

import sys
import traceback
import itertools
import logging

import six

from ..context import get_context, set_context, DecisionContext
from ..workflow_execution import WorkflowExecution
from ..core import async, async_traceback, Future, Return, AsyncEventLoop
from ..utils import pairwise
from ..constants import USE_WORKER_TASK_LIST

from ..history_events import (
    WorkflowExecutionStarted, ActivityTaskScheduled, ActivityTaskTimedOut,
    ScheduleActivityTaskFailed, ActivityTaskCompleted, ActivityTaskFailed,
    DecisionTaskCompleted, DecisionTaskScheduled, DecisionTaskTimedOut, DecisionTaskStarted,
    DecisionEventBase, StartChildWorkflowExecutionInitiated,
    StartChildWorkflowExecutionFailed, ChildWorkflowExecutionCompleted,
    ChildWorkflowExecutionFailed, ChildWorkflowExecutionTimedOut, StartTimerFailed,
    TimerFired, TimerStarted, TimerCanceled, ChildWorkflowExecutionTerminated,
    WorkflowExecutionSignaled, ChildWorkflowExecutionStarted)

from ..decisions import (
    DecisionList, ScheduleActivityTask, CompleteWorkflowExecution,
    FailWorkflowExecution, ContinueAsNewWorkflowExecution, StartChildWorkflowExecution)

from ..exceptions import (
    ScheduleActivityTaskFailedError, StartChildWorkflowExecutionFailedError,
    ActivityTaskFailedError, ActivityTaskTimedOutError, ChildWorkflowTimedOutError,
    ChildWorkflowFailedError, ChildWorkflowTerminatedError)

from .decision_task_poller import DecisionTaskPoller

log = logging.getLogger(__name__)


class Decider(object):

    def __init__(self, worker, domain, task_list, workflows, identity,
                 _Poller=DecisionTaskPoller):
        self.worker = worker
        self.domain = domain
        self.task_list = task_list
        self.identity = identity
        self.workflows = workflows

        self._poller = _Poller(worker, domain, task_list, identity)
        self._eventloop = AsyncEventLoop()

        self._reset()

    def _reset(self):
        self.execution_started = False

        self._decisions = None
        self._decisions = DecisionList()
        self._decision_id = 0
        self._open_activities = {}
        self._open_child_workflows = {}
        self._open_timers = {}
        self._event_to_id_table = {}
        self._decision_task_token = None
        self._continue_as_new_on_completion = None

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
        decision_start_to_compeltion_events = list()
        decision_completion_to_start_events = list()
        concurrent_to_decision = True
        last_decision_index = -1

        decision_task = self._poller.poll()
        if decision_task is None:
            return

        self._decision_task_token = decision_task.task_token
        non_replay_event_id = decision_task.previous_started_event_id
        context.workflow_id = decision_task.workflow_id
        context.run_id = decision_task.run_id
        context._workflow_execution = WorkflowExecution(
            decision_task.workflow_id, decision_task.run_id)

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
                        get_context()._workflow_time = event.timestamp
                elif isinstance(event, (DecisionTaskScheduled, DecisionTaskTimedOut)):
                    continue
                else:
                    if concurrent_to_decision:
                        decision_start_to_compeltion_events.append(event)
                    else:
                        if isinstance(event, DecisionEventBase):
                            last_decision_index = len(
                                decision_completion_to_start_events)
                        decision_completion_to_start_events.append(event)

                if decision_started:
                    if last_decision_index > -1:
                        reordered_events = decision_completion_to_start_events

                    reordered_events = itertools.chain(
                        reordered_events, decision_start_to_compeltion_events)
                    for event in reordered_events:
                        if event.id >= non_replay_event_id:
                            get_context()._replaying = False
                        self._handle_history_event(event)

                    reordered_events = list()
                    decision_completion_to_start_events = list()
                    decision_start_to_compeltion_events = list()
                    decision_started = False

            self._process_decisions()
        finally:
            set_context(prev_context)

    def _process_decisions(self):
        # drain all tasks before submitting more decisions
        self._eventloop.execute_all_tasks()

        if self._decision_task_token is not None:
            # get the workflow_state (otherwise known as execution context)
            workflow_state = get_context().workflow.workflow_state

            log.debug("Sending workflow decisions: %s", self._decisions)
            self.worker._respond_decision_task_completed_op(
                task_token=self._decision_task_token,
                decisions=self._decisions.to_swf(),
                execution_context=workflow_state)

        # basically garbage collect
        Future.untrack_all_coroutines()

        for val in six.itervalues(self._open_activities):
            val['handler'].close()

    def _handle_history_event(self, event):
        log.debug("Handling history event: %s", event)
        # TODO implemet timers
        try:
            if isinstance(event, WorkflowExecutionStarted):
                self._handle_workflow_execution_started(event)

            elif isinstance(event,
                            (ActivityTaskScheduled,
                             ScheduleActivityTaskFailed)):
                activity_id = event.attributes['activityId']
                self._open_activities[activity_id]['handler'].send(event)

            elif isinstance(event,
                            (ActivityTaskCompleted,
                             ActivityTaskFailed,
                             ActivityTaskTimedOut)):
                scheduled_event_id = event.attributes['scheduledEventId']
                activity_id = self._event_to_id_table[('Activity',
                                                       scheduled_event_id)]
                self._open_activities[activity_id]['handler'].send(event)

            elif isinstance(event,
                            (StartChildWorkflowExecutionInitiated,
                             StartChildWorkflowExecutionFailed)):
                workflow_id = event.attributes['workflowId']
                self._open_child_workflows[workflow_id]['handler'].send(event)

            elif isinstance(event,
                            (ChildWorkflowExecutionStarted,
                             ChildWorkflowExecutionCompleted,
                             ChildWorkflowExecutionFailed,
                             ChildWorkflowExecutionTimedOut,
                             ChildWorkflowExecutionTerminated)):

                scheduled_event_id = event.attributes['initiatedEventId']
                workflow_id = self._event_to_id_table[(
                    'ChildWorkflowExecution', scheduled_event_id)]
                self._open_child_workflows[workflow_id]['handler'].send(event)

            elif isinstance(event,
                            (StartTimerFailed,
                             TimerFired,
                             TimerStarted,
                             TimerCanceled)):
                timer_id = event.attributes['timerId']
                self._open_timers[timer_id]['handler'].send(event)

            elif isinstance(event, WorkflowExecutionSignaled):
                self._signal_workflow_execution(event)
#            else:
#                raise NotImplementedError(event)
        except StopIteration:
            pass
        self._eventloop.execute_all_tasks()

    def _handle_activity_event(self, activity_type, activity_id, activity_future):
        event = (yield)

        if isinstance(event,
                      (ActivityTaskScheduled,
                       ScheduleActivityTaskFailed)):
            self._decisions.delete_decision(ScheduleActivityTask, activity_id)

        if isinstance(event, ActivityTaskScheduled):
            # need to be able to find the activity id as it's not always
            # present in the history
            self._event_to_id_table[('Activity', event.id)] = activity_id

            event = (yield)
            if isinstance(event, ActivityTaskCompleted):
                result = activity_type.data_converter.loads(
                    event.attributes['result'])
                activity_future.set_result(result)

            elif isinstance(event, ActivityTaskFailed):
                exception, _traceback = activity_type.data_converter.loads(
                    event.attributes['details'])
                error = ActivityTaskFailedError(
                    event.id, activity_type, activity_id, cause=exception,
                    _traceback=_traceback)
                activity_future.set_exception(error)

            elif isinstance(event, ActivityTaskTimedOut):
                error = ActivityTaskTimedOutError(
                    event.id, activity_type, activity_id,
                    event.attributes['timeoutType'])
                activity_future.set_exception(error)
            else:
                raise RuntimeError("Unexpected event/state: %s", event)

        elif isinstance(event, ScheduleActivityTaskFailed):
            # set the exception with a cause
            cause = event.attributes['cause']
            activity_future.set_exception(
                ScheduleActivityTaskFailedError(cause))

        else:
            raise RuntimeError("Unexpected event/state: %s", event)

        del self._open_activities[activity_id]  # activity done

    def _handle_child_workflow_event(self, workflow_type, workflow_id,
                                     workflow_future):
        event = (yield)
        # TODO support ChildWorkflowExecutionTerminated event

        if isinstance(event,
                      (StartChildWorkflowExecutionInitiated,
                       StartChildWorkflowExecutionFailed)):
            self._decisions.delete_decision(StartChildWorkflowExecution,
                                            (workflow_type.name,
                                             workflow_type.version,
                                             workflow_id))

        if isinstance(event, StartChildWorkflowExecutionInitiated):
            # need to be able to find the workflow id as it's not always
            # present in the history
            self._event_to_id_table[('ChildWorkflowExecution',
                                     event.id)] = workflow_id

            event = (yield)
            if isinstance(event, ChildWorkflowExecutionStarted):
                # set the workflow_execution information on the instance
                workflow_instance = self._open_child_workflows[workflow_id] \
                                    ['workflow_instance']
                workflow_started_future = self._open_child_workflows \
                                          [workflow_id] \
                                          ['workflow_started_future']


                workflow_id = event.attributes['workflowExecution']['workflowId']
                run_id = event.attributes['workflowExecution']['runId']
                workflow_instance.workflow_execution = WorkflowExecution(workflow_id, run_id)
                workflow_started_future.set_result(workflow_instance)

            event = (yield)
            if isinstance(event, ChildWorkflowExecutionCompleted):
                result = workflow_type.data_converter.loads(
                    event.attributes['result'])
                workflow_future.set_result(result)

            elif isinstance(event, ChildWorkflowExecutionFailed):
                exception, _traceback = workflow_type.data_converter.loads(
                    event.attributes['details'])
                error = None
                # TODO convert the tuples to NamedTuples
                error = ChildWorkflowFailedError(
                    event.id, (workflow_type.name, workflow_type.version),
                    get_context()._workflow_execution, cause=exception,
                    _traceback=_traceback)
                workflow_future.set_exception(error)

            elif isinstance(event, ChildWorkflowExecutionTimedOut):
                # TODO convert the tuples to NamedTuples
                error = ChildWorkflowTimedOutError(
                    event.id, (workflow_type.name, workflow_type.version),
                    get_context()._workflow_execution)
                workflow_future.set_exception(error)

            elif isinstance(event, ChildWorkflowExecutionTerminated):
                # TODO convert the tuples to NamedTuples
                error = ChildWorkflowTerminatedError(
                    event.id, (workflow_type.name, workflow_type.version),
                    get_context()._workflow_execution)
                workflow_future.set_exception(error)
            else:
                raise RuntimeError("Unexpected event/state: %s", event)

        elif isinstance(event, StartChildWorkflowExecutionFailed):
            # set the exception with a cause
            cause = event.attributes['cause']
            workflow_started_future = self._open_child_workflows \
                                      [workflow_id] \
                                      ['workflow_started_future']

            workflow_started_future.set_exception(
                StartChildWorkflowExecutionFailedError(cause))

        else:
            raise RuntimeError("Unexpected event/state: %s", event)

        del self._open_child_workflows[workflow_id]  # child worflow done

    def _signal_workflow_execution(self, event):
        context = get_context()
        args, kwargs = context.workflow._data_converter.loads(
            event.attributes['input'])
        signal_name = event.attributes['signalName']

        # make sure kwargs are non-unicode in 2.6
        if sys.version_info[0:2] == (2, 6):
            kwargs = dict([(str(k), v) for k, v in six.iteritems(kwargs)])

        context.workflow._workflow_signals[signal_name][1](
            context.workflow, *args, **kwargs)

    def _handle_workflow_execution_started(self, event):
        context = get_context()
        # find the workflow we're working with
        workflow_name = event.attributes['workflowType']['name']
        workflow_version = event.attributes['workflowType']['version']

        # find the workflow class based ont the event information
        workflow_definition, workflow_type, func_name = self.workflows[
            (workflow_name, workflow_version)]

        # instantiate workflow

        workflow_instance = workflow_definition(
            WorkflowExecution(context.workflow_id, context.run_id))
        # FIXME: this is temporary for signals to find what data_converter to use
        workflow_instance._data_converter = workflow_type.data_converter
        execute_method = getattr(workflow_instance, func_name)

        context.workflow = workflow_instance

        # get input parameters from the workflow starter
        args, kwargs = workflow_type.data_converter.loads(
            event.attributes['input'])

        # make sure kwargs are non-unicode in 2.6
        if sys.version_info[0:2] == (2, 6):
            kwargs = dict([(str(k), v) for k, v in six.iteritems(kwargs)])

        @async
        def handle_execute():
            try:
                future = execute_method(*args, **kwargs)
                # any subsequent executions will be counted "continue as new"
                self.execution_started = True
                execute_result = yield future

                # XXX should these be the only decisions?
                if self._continue_as_new_on_completion is None:
                    log.debug(
                        "Workflow execute() returned: %s", execute_result)
                    self._decisions.append(CompleteWorkflowExecution(
                        workflow_type.data_converter.dumps(execute_result)))
                else:
                    log.debug("ContinueAsNew: %s",
                              self._continue_as_new_on_completion)
                    self._decisions.append(self._continue_as_new_on_completion)

            except Exception as err:
                tb_list = async_traceback.extract_tb()
                log.debug("Workflow execute() raised an exception:\n%s",
                          "".join(traceback.format_exc()))
                # clean any lingering decisions as we're about to terminate
                # the execution
                # XXX Validate this is the right action
                self._decisions = DecisionList()
                self._decisions.append(FailWorkflowExecution(
                    '', workflow_type.data_converter.dumps([err, tb_list])))

        with self._eventloop:
            handle_execute()  # schedule

        # wait for all the tasks to complete
        self._eventloop.execute_all_tasks()

    def _handle_execute_activity(self, activity_type, decision_dict, args,
                                 kwargs):
        activity_id = self.get_next_id()
        decision_dict['activity_id'] = activity_id

        if decision_dict['task_list']['name'] == USE_WORKER_TASK_LIST:
            decision_dict['task_list']['name'] = self.task_list

        decision_dict['input'] = activity_type.data_converter.dumps([args, kwargs])
        decision = ScheduleActivityTask(**decision_dict)
        self._decisions.append(decision)

        log.debug("Workflow schedule activity execution: %s, %s, %s, %s",
                  decision, args, kwargs, activity_id)

        # set the future that represents the result of our activity
        activity_future = Future()
        handler = self._handle_activity_event(activity_type, activity_id,
                                              activity_future)
        six.next(handler)  # arm
        self._open_activities[activity_id] = {'future': activity_future,
                                              'handler': handler}

        @async
        def wait_activity():
            try:
                result = yield activity_future
            # XXX handle CancellationError
                raise Return(result)
            except GeneratorExit:
                pass

        return wait_activity()

    def _handle_start_child_workflow_execution(self, workflow_type,
                                               workflow_instance, input):
        run_id = get_context().run_id
        workflow_id = "%s:%s" % (run_id, self.get_next_id())

        decision_dict = workflow_type.to_decision_dict(
            input, workflow_id, self.task_list)

        decision = StartChildWorkflowExecution(**decision_dict)
        self._decisions.append(decision)

        log.debug("Workflow start child workflow execution: %s", decision)

        workflow_id = decision_dict['workflow_id']

        # set the future that represents the result of our activity
        workflow_future = Future()
        workflow_started_future = Future()
        handler = self._handle_child_workflow_event(workflow_type,
                                                    workflow_id,
                                                    workflow_future)
        six.next(handler)  # arm
        self._open_child_workflows[workflow_id] = {
            'future': workflow_future, 'handler': handler,
            'workflow_instance': workflow_instance,
            'workflow_started_future': workflow_started_future}

        @async
        def wait_workflow_start():
            try:
                _workflow_instance = yield workflow_started_future
                raise Return(_workflow_instance)
            except GeneratorExit:
                pass

        @async
        def wait_child_workflow_complete():
            try:
                result = yield workflow_future
            # XXX handle CancellationError
                raise Return(result)
            except GeneratorExit:
                pass

        workflow_instance._workflow_result = wait_child_workflow_complete()
        return wait_workflow_start()

    def _continue_as_new_workflow_execution(self, **kwargs):
        """
        ContinueAsNewWorkflowExecution closes the workflow execution and
        starts a new workflow execution of the same type using the same
        workflow id and a unique run Id. A WorkflowExecutionContinuedAsNew
        event is recorded in the history.
        """
        decision = ContinueAsNewWorkflowExecution(**kwargs)
        self._continue_as_new_on_completion = decision
