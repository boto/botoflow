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

__all__ = ('time', 'sleep')

import six

from .core import async, Future, CancelledError
from .context import get_context, DecisionContext
from .decisions import StartTimer
from .history_events import (StartTimerFailed, TimerFired, TimerStarted,
                             TimerCanceled)


def time():
    """
    time() -> integer

    Return the current time in seconds since the Epoch.
    Fractions of a second will not be presented as in the time.time()

    If the method is called not in the DecisionContext, it will throw a
    TyperError
    """
    try:
        context = get_context()
        if isinstance(context, DecisionContext):
            return context._workflow_time
    except AttributeError:
        pass
    raise TypeError("workflow_time.time() should be run inside of a workflow")


def sleep(seconds):
    """
    Value that becomes ready after the specified delay.
    It acts like time.sleep() if used together with a yield.

    This method can throw a TimerCancelledError
    """
    try:
        context = get_context()
        if not isinstance(context, DecisionContext):
            raise AttributeError()
    except AttributeError:
        raise TypeError("flow_time.Timer() should be run inside of a "
                        "workflow")

    decider = context.decider
    decision_id = decider.get_next_id()
    timer_decision = StartTimer(decision_id, str(int(seconds)))
    decider._decisions.append(timer_decision)

    timer_future = Future()

    handler = _handle_timer_event(decision_id, timer_future)
    six.next(handler)  # arm
    decider._open_timers[decision_id] = dict(future=timer_future,
                                             handler=handler)

    @async
    def wait_for_timer():
        yield timer_future

    return wait_for_timer()


def is_replaying():
    """Indicates if the workflow is currently replaying (True) or generating
    (False) new decisions.

    This could be useful for filtering out logs for transitions that have
    already completed. See: ``awsflow.logging_filters.AWSFlowFilter``.

    :returns: True if the current state in the workflow being replayed.
    :rtype: bool
    :raises TypeError: If the method is called not in the DecisionContext.
    """
    try:
        context = get_context()
        if isinstance(context, DecisionContext):
            return context._replaying
    except AttributeError:
        pass
    raise TypeError("workflow_time.time() should be run inside of a workflow")


def _handle_timer_event(timer_id, timer_future):
    event = (yield)

    decider = get_context().decider

    if isinstance(event, StartTimerFailed):
        decider._decisions.delete_decision(StartTimer, timer_id)
        # TODO Throw an exception on startTimerFailed
    elif isinstance(event, TimerStarted):
        decider._decisions.delete_decision(StartTimer, timer_id)
        event = (yield)

        if isinstance(event, TimerFired):
            timer_future.set_result(None)
        elif isinstance(event, TimerCanceled):
            timer_future.set_exception(CancelledError("Timer Cancelled"))
    else:
        raise RuntimeError("Unexpected event/state: %s", event)

    del decider._open_timers[timer_id]
