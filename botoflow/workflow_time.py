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

__all__ = ('time', 'sleep', 'is_replaying')

from time import mktime

from .context import get_context, DecisionContext


def time():
    """
    time() -> integer

    Return the current time in seconds since the Epoch.
    Fractions of a second will not be presented as in the time.time()

    :raises TypeError: If the function is called not in DecisionContext
    :returns: Returns the workflow's time in seconds since epoch.
    :rtype: int
    """
    try:
        context = get_context()
        if isinstance(context, DecisionContext):
            return int(mktime(context._workflow_time.timetuple()))
    except AttributeError:
        pass
    raise TypeError("workflow_time.time() should be run inside of a workflow")


def sleep(seconds):
    """
    Value that becomes ready after the specified delay.
    It acts like time.sleep() if used together with a yield.

    :raises TypeError: If the function is called not in DecisionContext
    :raises botoflow.core.exceptions.CancelledError: If the timer/sleep was cancelled
    :returns: Future representing the timer
    :rtype: botoflow.core.future.Future
    """
    try:
        context = get_context()
        if not isinstance(context, DecisionContext):
            raise AttributeError()
    except AttributeError:
        raise TypeError("flow_time.Timer() should be run inside of a "
                        "workflow")

    decider = context.decider
    return decider.handle_execute_timer(seconds)


def is_replaying():
    """Indicates if the workflow is currently replaying (True) or generating
    (False) new decisions.

    This could be useful for filtering out logs for transitions that have
    already completed. See: ``botoflow.logging_filters.AWSFlowFilter``.

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
