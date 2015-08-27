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

from ..core import Future, BaseFuture, AllFuture, CancelledError

from ..constants import USE_WORKER_TASK_LIST
from ..exceptions import (ActivityTaskFailedError, ActivityTaskTimedOutError, ScheduleActivityTaskFailedError,
                          ActivityTaskCanceledError, RequestCancelActivityTaskFailedError)
from ..decisions import ScheduleActivityTask, RequestCancelActivityTask
from ..history_events import (ActivityTaskScheduled, ScheduleActivityTaskFailed, ActivityTaskCompleted,
                              ActivityTaskFailed, ActivityTaskTimedOut, ActivityTaskCancelRequested,
                              ActivityTaskCanceled, ActivityTaskStarted, RequestCancelActivityTaskFailed)

from .activity_future import ActivityFuture

log = logging.getLogger(__name__)


class ActivityTaskHandler(object):

    responds_to = (ActivityTaskScheduled, ActivityTaskCompleted, ActivityTaskFailed, ActivityTaskTimedOut,
                   ScheduleActivityTaskFailed, ActivityTaskStarted, ActivityTaskCanceled,
                   ActivityTaskCancelRequested, RequestCancelActivityTaskFailed)

    def __init__(self, decider, task_list):

        self._decider = decider
        self._open_activities = {}
        self._schedule_event_to_activity_id = {}
        self._open_cancels = {}
        self._task_list = task_list

    def __del__(self):
        log.debug("Closing all activity handlers")
        for val in six.itervalues(self._open_activities):
            val['handler'].close()

    def handle_execute_activity(self, activity_type, decision_dict, args, kwargs):
        """Makes ScheduleActivityTask decision and creates associated future.

        :return: activity future
        :rtype: awsflow.decider.activity_future.ActivityFuture
        """
        activity_id = self._decider.get_next_id()
        decision_dict['activity_id'] = activity_id

        if decision_dict['task_list']['name'] == USE_WORKER_TASK_LIST:
            decision_dict['task_list']['name'] = self._task_list

        decision_dict['input'] = activity_type.data_converter.dumps([args, kwargs])
        decision = ScheduleActivityTask(**decision_dict)
        self._decider._decisions.append(decision)

        log.debug("Workflow schedule activity execution: %s, %s, %s, %s",
                  decision, args, kwargs, activity_id)

        # set the future that represents the result of our activity
        activity_future = Future()
        handler = self._handler_fsm(activity_type, activity_id, activity_future)
        six.next(handler)  # arm
        self._open_activities[activity_id] = {'future': activity_future,
                                              'handler': handler}

        return ActivityFuture(activity_future, self, activity_id)

    def request_cancel_activity_task_all(self):
        """Makes RequestCancelActivityTask decisions for all open activities.

        :return: all futures for cancel requests
        :rtype: awsflow.core.future.AllFuture
        """
        futures = set()
        for activity_id, activity_info in self._open_activities.items():
            futures.add(self.request_cancel_activity_task(activity_info['future'], activity_id))
        return AllFuture(*futures)

    def request_cancel_activity_task(self, activity_future, activity_id):
        """Requests to cancel an activity with the given activity_id.

        If the schedule decision for the activity was not yet sent, it's future is set to
        CancelledError and a BaseFuture is returned; otherwise, a RequestCancelActivityTask
        decision is made, and a future that tracks the request is returned.

        :param activity_id: id of the activity to handle
        :type activity_id: str
        :param activity_future: the calling future; target for cancellation
        :type activity_future: awsflow.decider.activity_future.ActivityFuture
        :return: cancel future
        :rtype: awsflow.core.future.Future
        """
        if activity_id in self._open_cancels:
            return self._open_cancels[activity_id]['future']  # duplicate request

        if self._decider._decisions.delete_decision(ScheduleActivityTask, activity_id):
            activity_future.set_exception(CancelledError(
                "Activity was cancelled before being scheduled with SWF"))
            del self._open_activities[activity_id]
            return BaseFuture.with_result(None)

        self._decider._decisions.append(RequestCancelActivityTask(activity_id))

        cancel_activity_future = Future()
        self._open_cancels[activity_id] = {'future': cancel_activity_future}
        return cancel_activity_future

    def handle_event(self, event):
        """Determines activity id associated with event, then forwards event to appropriate handler."""

        activity_id = None

        if isinstance(event, ActivityTaskStarted):
            return

        if isinstance(event, RequestCancelActivityTaskFailed):
            self._resolve_cancel_future(event.attributes['activityId'], failed_event=event)
            return

        if isinstance(event, (ActivityTaskScheduled, ScheduleActivityTaskFailed,
                              ActivityTaskCancelRequested)):
            activity_id = event.attributes['activityId']

        elif isinstance(event, (ActivityTaskCompleted, ActivityTaskFailed, ActivityTaskTimedOut,
                                ActivityTaskCanceled)):
            scheduled_event_id = event.attributes['scheduledEventId']
            activity_id = self._schedule_event_to_activity_id[scheduled_event_id]

        if activity_id is not None:
            self._open_activities[activity_id]['handler'].send(event)
        else:
            log.warn("Tried to handle activity event, but activity_id was None: %r", event)

    def _handler_fsm(self, activity_type, activity_id, activity_future):
        """FSM responsible for yielding through events until setting a result on activity_future.

        This is also responsible for resolving any open cancel futures.

        :param activity_type:
        :type activity_type: awsflow.workflow_types.ActivityType
        :param activity_id: id of the activity to handle
        :type activity_id: int
        :param activity_future: to resolve once activity finishes
        :type activity_future: awsflow.core.future.Future
        :return:
        """
        event = (yield)

        if isinstance(event, (ActivityTaskScheduled, ScheduleActivityTaskFailed)):
            self._decider._decisions.delete_decision(ScheduleActivityTask, activity_id)

        # ActivityTaskScheduled event always preceeds ActivityTaskCancelRequested event,
        # given that no cancels are sent until the ScheduleActivityTask decision is deleted.
        if isinstance(event, (ActivityTaskScheduled, ActivityTaskCancelRequested)):
            if isinstance(event, ActivityTaskScheduled):
                # maintain mapping of schedule event id to activity id for future lookup
                self._schedule_event_to_activity_id[event.id] = activity_id
                event = (yield)

            if isinstance(event, ActivityTaskCancelRequested):
                self._resolve_cancel_future(activity_id)
                event = (yield)  # do not interrupt actual activity future

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

            elif isinstance(event, ActivityTaskCanceled):
                exception, _traceback = CancelledError("Activity was cancelled before being picked up by "
                                                       "activity worker"), None
                if event.attributes.get('details', False):
                    # parse out exception from activity result
                    exception, _traceback = activity_type.data_converter.loads(event.attributes['details'])

                error = ActivityTaskCanceledError(
                    event.id, activity_type, activity_id, cause=exception,
                    latest_cancel_requested_event_id=event.attributes.get('latestCancelRequestedEventId'),
                    scheduled_event_id=event.attributes.get('scheduledEventId'),
                    started_event_id=event.attributes.get('startedEventId'),
                    _traceback=_traceback)
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
        if activity_id in self._open_cancels:
            self._resolve_cancel_future(activity_id)

    def _resolve_cancel_future(self, activity_id, failed_event=None):
        """Resolves a cancel future by setting its result to None or exception if failed_event.

        :param activity_id: id of the activity that was to be cancelled
        :type activity_id: int
        :param failed_event: associated cancel failure event
        :type failed_event: awsflow.history_events.RequestCancelActivityTaskFailed
        :return:
        """
        self._decider._decisions.delete_decision(RequestCancelActivityTask, activity_id)
        if activity_id not in self._open_cancels:
            return  # no future; occurs with cancel all requests
        cancel_future = self._open_cancels[activity_id]['future']
        if failed_event:
            cancel_future.set_exception(RequestCancelActivityTaskFailedError(
                failed_event.id, activity_id, failed_event.attributes['cause'],
                failed_event.attributes['decisionTaskCompletedEventId']))
        else:
            cancel_future.set_result(None)
        del self._open_cancels[activity_id]
