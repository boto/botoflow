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

import logging

import six

from .event_bases import (ActivityEventBase, ChildWorkflowEventBase,
                          DecisionEventBase, ExternalWorkflowEventBase,
                          WorkflowEventBase, TimerEventBase, EventBase,
                          MarkerEventBase, DecisionTaskEventBase)

log = logging.getLogger(__name__)


def swf_event_to_object(event_dict):
    """
    takes an event dictionary from botocore and converts it into a specific
    event instance.
    """
    try:
        event_class = _event_type_name_to_class[event_dict['eventType']]
    except KeyError:
        # we cannot guarantee we do the right thing in the decider if there's an unsupported event type.
        logging.critical("Event type '%' is not implemented. Cannot continue processing decisions!",
                         event_dict['eventType'])
        raise NotImplementedError(
            "Event type '{}' is not implemented. Cannot continue processing decisions!".format(event_dict['eventType']))

    return event_class(event_dict['eventId'],
                       event_dict['eventTimestamp'],
                       event_dict[event_class.attribute_key])


class ActivityTaskCancelRequested(ActivityEventBase, DecisionEventBase):
    attribute_key = 'activityTaskCancelRequestedEventAttributes'


class ActivityTaskCanceled(ActivityEventBase):
    attribute_key = 'activityTaskCanceledEventAttributes'


class ActivityTaskCompleted(ActivityEventBase):
    attribute_key = 'activityTaskCompletedEventAttributes'


class ActivityTaskFailed(ActivityEventBase):
    attribute_key = 'activityTaskFailedEventAttributes'


class ActivityTaskScheduled(ActivityEventBase, DecisionEventBase):
    attribute_key = 'activityTaskScheduledEventAttributes'


class ActivityTaskStarted(ActivityEventBase):
    attribute_key = 'activityTaskStartedEventAttributes'


class ActivityTaskTimedOut(ActivityEventBase):
    attribute_key = 'activityTaskTimedOutEventAttributes'


class CancelWorkflowExecutionFailed(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'cancelWorkflowExecutionFailedEventAttributes'


class CancelTimerFailed(TimerEventBase, DecisionEventBase):
    attribute_key = 'cancelTimerFailedEventAttributes'


class ChildWorkflowExecutionStarted(ChildWorkflowEventBase):
    attribute_key = 'childWorkflowExecutionStartedEventAttributes'


class ChildWorkflowExecutionCompleted(ChildWorkflowEventBase):
    attribute_key = 'childWorkflowExecutionCompletedEventAttributes'


class ChildWorkflowExecutionFailed(ChildWorkflowEventBase):
    attribute_key = 'childWorkflowExecutionFailedEventAttributes'


class ChildWorkflowExecutionTimedOut(ChildWorkflowEventBase):
    attribute_key = 'childWorkflowExecutionTimedOutEventAttributes'


class ChildWorkflowExecutionCanceled(ChildWorkflowEventBase):
    attribute_key = 'childWorkflowExecutionCanceledEventAttributes'


class ChildWorkflowExecutionTerminated(ChildWorkflowEventBase):
    attribute_key = 'childWorkflowExecutionTerminatedEventAttributes'


class CompleteWorkflowExecutionFailed(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'completeWorkflowExecutionFailedEventAttributes'


class ContinueAsNewWorkflowExecutionFailed(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'continueAsNewWorkflowExecutionFailedEventAttributes'


class DecisionTaskScheduled(DecisionTaskEventBase):
    attribute_key = 'decisionTaskScheduledEventAttributes'


class DecisionTaskStarted(DecisionTaskEventBase):
    attribute_key = 'decisionTaskStartedEventAttributes'


class DecisionTaskCompleted(DecisionTaskEventBase):
    attribute_key = 'decisionTaskCompletedEventAttributes'


class DecisionTaskTimedOut(DecisionTaskEventBase):
    attribute_key = 'decisionTaskTimedOutEventAttributes'


class FailWorkflowExecutionFailed(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'failWorkflowExecutionFailedEventAttributes'


class MarkerRecorded(MarkerEventBase, DecisionEventBase):
    attribute_key = 'markerRecordedEventAttributes'


class RequestCancelActivityTaskFailed(ActivityEventBase, DecisionEventBase):
    attribute_key = 'requestCancelActivityTaskFailedEventAttributes'


class RequestCancelExternalWorkflowExecutionFailed(ExternalWorkflowEventBase, DecisionEventBase):
    attribute_key = 'requestCancelExternalWorkflowExecutionFailedEventAttributes'


class RequestCancelExternalWorkflowExecutionInitiated(ExternalWorkflowEventBase, DecisionEventBase):
    attribute_key = 'requestCancelExternalWorkflowExecutionInitiatedEventAttributes'


class ExternalWorkflowExecutionCancelRequested(ExternalWorkflowEventBase, DecisionEventBase):
    attribute_key = 'externalWorkflowExecutionCancelRequestedEventAttributes'


class ScheduleActivityTaskFailed(ActivityEventBase, DecisionEventBase):
    attribute_key = 'scheduleActivityTaskFailedEventAttributes'


class SignalExternalWorkflowExecutionInitiated(ExternalWorkflowEventBase, DecisionEventBase):
    attribute_key = 'signalExternalWorkflowExecutionInitiatedEventAttributes'


class SignalExternalWorkflowExecutionFailed(ExternalWorkflowEventBase, DecisionEventBase):
    attribute_key = 'signalExternalWorkflowExecutionFailedEventAttributes'


class StartChildWorkflowExecutionFailed(ChildWorkflowEventBase, DecisionEventBase):
    attribute_key = 'startChildWorkflowExecutionFailedEventAttributes'


class StartChildWorkflowExecutionInitiated(ChildWorkflowEventBase, DecisionEventBase):
    attribute_key = 'startChildWorkflowExecutionInitiatedEventAttributes'


class StartTimerFailed(TimerEventBase, DecisionEventBase):
    attribute_key = 'startTimerFailedEventAttributes'


class TimerFired(TimerEventBase, DecisionEventBase):
    attribute_key = 'timerFiredEventAttributes'


class TimerCanceled(TimerEventBase, DecisionEventBase):
    attribute_key = 'timerCanceledEventAttributes'


class TimerStarted(TimerEventBase, DecisionEventBase):
    attribute_key = 'timerStartedEventAttributes'


class WorkflowExecutionCanceled(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionCanceledEventAttributes'


class WorkflowExecutionCancelRequested(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionCancelRequestedEventAttributes'


class WorkflowExecutionCompleted(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionCompletedEventAttributes'


class WorkflowExecutionContinuedAsNew(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionContinuedAsNewEventAttributes'


class WorkflowExecutionFailed(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionFailedEventAttributes'


class WorkflowExecutionStarted(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionStartedEventAttributes'


class WorkflowExecutionSignaled(WorkflowEventBase, DecisionEventBase):
    attribute_key = 'workflowExecutionSignaledEventAttributes'


class WorkflowExecutionTimedOut(WorkflowEventBase):
    attribute_key = 'workflowExecutionTimedOutEventAttributes'

# extract event classes from the module into a name dictionary
_event_type_name_to_class = dict()


def _set_event_type_name_to_class():
    for name, value in six.iteritems(globals()):
        try:
            if issubclass(value, EventBase):
                _event_type_name_to_class[name] = value
        except TypeError:
            pass


_set_event_type_name_to_class()
