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

from ..core import return_, async, Future

from ..constants import USE_WORKER_TASK_LIST
from ..exceptions import (ActivityTaskFailedError, ActivityTaskTimedOutError, ScheduleActivityTaskFailedError,
                          ActivityTaskCanceledError)
from ..decisions import ScheduleActivityTask, RequestCancelActivityTask
from ..history_events import (ActivityTaskScheduled, ScheduleActivityTaskFailed, ActivityTaskCompleted,
                              ActivityTaskFailed, ActivityTaskTimedOut, ActivityTaskCancelRequested,
                              ActivityTaskCanceled, ActivityTaskStarted, RequestCancelActivityTaskFailed)

log = logging.getLogger(__name__)


class ActivityTaskHandler(object):

    responds_to = (ActivityTaskScheduled, ActivityTaskCompleted, ActivityTaskFailed, ActivityTaskTimedOut,
                   ActivityTaskCancelRequested, ScheduleActivityTaskFailed, ActivityTaskCanceled, ActivityTaskStarted,
                   RequestCancelActivityTaskFailed)

    def __init__(self, decider, task_list):

        self._decider = decider
        self._open_activities = {}
        self._schedule_event_to_activity_id = {}
        self._open_cancels = set()
        self._cancel_event_to_activity_id = {}
        self._task_list = task_list

    def __del__(self):
        log.debug("Closing all activity handlers")
        for val in six.itervalues(self._open_activities):
            val['handler'].close()

    def handle_execute_activity(self, activity_type, decision_dict, args, kwargs):
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

        @async
        def wait_activity():
            try:
                result = yield activity_future
                return_(result)
            except GeneratorExit:
                pass
        return wait_activity()

    def request_cancel_activity_task(self, activity_id):
        if activity_id in self._open_cancels:
            log.warn("Already have open request to cancel activity {} -- ignoring.".format(
                activity_id))
            return

        self._decisions.append(RequestCancelActivityTask(activity_id))
        self._open_cancels.add(activity_id)

    def handle_event(self, event):
        activity_id = None

        if isinstance(event, (ActivityTaskCancelRequested, RequestCancelActivityTaskFailed,
                              ActivityTaskCanceled)):
            self._handle_cancel_event(event)
            return

        if isinstance(event, ActivityTaskStarted):
            return

        if isinstance(event, (ActivityTaskScheduled, ScheduleActivityTaskFailed)):
            activity_id = event.attributes['activityId']

        elif isinstance(event, (ActivityTaskCompleted, ActivityTaskFailed, ActivityTaskTimedOut)):
            scheduled_event_id = event.attributes['scheduledEventId']
            activity_id = self._schedule_event_to_activity_id[scheduled_event_id]

        if activity_id is not None:
            self._open_activities[activity_id]['handler'].send(event)
        else:
            log.warn("Tried to handle activity event, but activity_id was None: %r", event)

    def _handle_cancel_event(self, event):
        attributes = event.attributes

        if isinstance(event, ActivityTaskCancelRequested):
            activity_id = attributes['activityId']
            self._decider._decisions.delete_decision(RequestCancelActivityTask, activity_id)
            if activity_id not in self._open_cancels:  # externally triggered cancel
                self._open_cancels.add(activity_id)
            self._cancel_event_to_activity_id[event.id] = activity_id

        elif isinstance(event, RequestCancelActivityTaskFailed):
            activity_id = attributes['activityId']
            self._decider._decisions.delete_decision(RequestCancelActivityTask, activity_id)
            self._open_cancels.remove(activity_id)

        elif isinstance(event, ActivityTaskCanceled):
            request_event_id = attributes['latestCancelRequestedEventId']
            activity_id = self._cancel_event_to_activity_id[request_event_id]
            self._open_cancels.remove(activity_id)
            del self._cancel_event_to_activity_id[request_event_id]

            activity_type = None  # do i need this?
            error = ActivityTaskCanceledError(
                event.id, activity_type, activity_id, attributes.get('details'),
                attributes.get('latestCancelRequestedEventId'),
                attributes.get('scheduledEventId'),
                attributes.get('startedEventId'))
            self._open_activities[activity_id]['future'].set_exception(error)

    def _handler_fsm(self, activity_type, activity_id, activity_future):
        event = (yield)

        if isinstance(event, (ActivityTaskScheduled, ScheduleActivityTaskFailed)):
            self._decider._decisions.delete_decision(ScheduleActivityTask, activity_id)

        if isinstance(event, ActivityTaskScheduled):
            # need to be able to find the activity id as it's not always
            # present in the history
            self._schedule_event_to_activity_id[event.id] = activity_id

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
