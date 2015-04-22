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

import six
import logging

from ..core import Future, async, CancelledError
from ..decisions import StartTimer
from ..history_events import (StartTimerFailed, TimerFired, TimerStarted, TimerCanceled)
log = logging.getLogger(__name__)


class TimerHandler(object):

    responds_to = (StartTimerFailed, TimerFired, TimerStarted, TimerCanceled)

    def __init__(self, decider, task_list):

        self._decider = decider
        self._open_timers = {}
        self._task_list = task_list

    def handle_execute_timer(self, seconds):
        decision_id = self._decider.get_next_id()
        timer_decision = StartTimer(decision_id, str(int(seconds)))
        self._decider._decisions.append(timer_decision)

        timer_future = Future()

        handler = self._handler_fsm(decision_id, timer_future)
        six.next(handler)  # arm
        self._open_timers[decision_id] = {'future': timer_future, 'handler': handler}

        @async
        def wait_for_timer():
            yield timer_future

        return wait_for_timer()

    def handle_event(self, event):
        if isinstance(event, (StartTimerFailed, TimerFired, TimerStarted, TimerCanceled)):
            timer_id = event.attributes['timerId']
            self._open_timers[timer_id]['handler'].send(event)
        else:
            log.warn("Tried to handle timer event, but a handler is missing: %r", event)

    def _handler_fsm(self, timer_id, timer_future):
        event = (yield)

        if isinstance(event, (StartTimerFailed, TimerStarted)):
            self._decider._decisions.delete_decision(StartTimer, timer_id)

        if isinstance(event, StartTimerFailed):
            # TODO Throw an exception on startTimerFailed
            pass
        elif isinstance(event, TimerStarted):
            event = (yield)

            if isinstance(event, TimerFired):
                timer_future.set_result(None)
            elif isinstance(event, TimerCanceled):
                timer_future.set_exception(CancelledError("Timer Cancelled"))
        else:
            raise RuntimeError("Unexpected event/state: %s", event)

        del self._open_timers[timer_id]
