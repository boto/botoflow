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

from .core.exceptions import CancelledError


class AWSFlowError(Exception):
    """
    base flow exception class
    """
    pass


class ScheduleActivityTaskFailedError(AWSFlowError):
    """This exception is thrown if Amazon SWF fails to schedule an activity
    task. This could happen due to various reasons - for example, the activity
    was deprecated, or an Amazon SWF limit on your account has been
    reached. The message property in the exception specifies the exact
    cause of failure to schedule the activity.
    """
    # FIXME MOVE THIS UNDER ActivityTaskError
    pass


class StartChildWorkflowExecutionFailedError(AWSFlowError):
    """This exception is thrown if Amazon SWF fails to start a child workflow
    execution. This could happen due to various reasons - for example, the type
    of child workflow specified was deprecated, or a Amazon SWF limit on your
    account has been reached. The message property in the exception
    specifies the exact cause of failure to start the child workflow execution.
    """
    pass


class DecisionExceptionWithTracebackMixIn(object):

    def print_exc(self, limit=None, file=None):
        """
        Print exception information and up to limit stack trace entries to file
        """
        if file is None:
            file = sys.stderr

        for line in self.format_exc(limit):
            file.write(line)

    def format_exc(self, limit=None):
        """
        This is like exception.print_exc(limit) but returns a string instead
        of printing to a file.
        """
        result = ["Traceback (most recent call last):\n"]

        tb_list = self._traceback

        if limit is not None:
            tb_list = tb_list[-limit:]

        result.extend(traceback.format_list(tb_list))

        if self.cause is not None:
            result.extend(traceback.format_exception_only(self.cause.__class__,
                                                          self.cause))
            return result
        else:
            return result


class DecisionException(Exception):
    """This is the base class for exceptions that represent failures to enact a
    decision by Amazon SWF. You can catch this exception to generically deal
    with such exceptions.
    """

    def __init__(self, event_id, *args):
        super(DecisionException, self).__init__(event_id, *args)
        self.event_id = event_id

    def __str__(self):
        return self.__repr__()


class ActivityTaskError(DecisionException):
    """This is the base class for activity task failure exceptions:
    :py:exc:`~.ScheduleActivityTaskFailedError`,
    :py:exc:`~.ActivityTaskFailedError`,
    :py:exc:`~.ActivityTaskTimedoutError`.
    It contains the task Id and activity type of the failed task. You can catch
    this exception in your workflow implementation to deal with activity
    failures in a generic way.
    """

    def __init__(self, event_id, activity_type, activity_id):
        super(ActivityTaskError, self).__init__(event_id, activity_type,
                                                activity_id)

        self.activity_type = activity_type
        self.activity_id = activity_id


class ActivityTaskFailedError(ActivityTaskError,
                              DecisionExceptionWithTracebackMixIn):
    """Unhandled exceptions in activities are reported back to the workflow
    implementation by throwing an :py:exc:`~.ActivityTaskFailedError`. The
    original exception can be retrieved from the cause property of this
    exception. The exception also provides other information that is useful for
    debugging purposes, such as the unique activity identifier in the history.
    The framework is able to provide the remote exception by serializing the
    original exception from the activity worker.
    """

    def __init__(self, event_id, activity_type, activity_id, cause,
                 _traceback=None):
        super(ActivityTaskFailedError, self).__init__(event_id, activity_type,
                                                      activity_id)
        self._traceback = _traceback
        self.cause = cause

    def __repr__(self):
        return ("<%s at %s event_id=%s activity_type=%s activity_id=%s "
                "cause=%r>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.activity_type, self.activity_id,
                    self.cause)


class ActivityTaskTimedOutError(ActivityTaskError):
    """This exception is thrown if an activity was timed out by Amazon
    SWF. This could happen if the activity task could not be assigned to the
    worker within the require time period or could not be completed by the
    worker in the required time. You can set these timeouts on the activity
    using the :py:func:`~botoflow.decorators.activity` decorator or using the
    :py:func:`~botoflow.options_overrides.activity_options` context manager
    when calling the activity method.
    """

    def __init__(self, event_id, activity_type, activity_id, timeout_type):
        super(
            ActivityTaskTimedOutError, self).__init__(event_id, activity_type,
                                                      activity_id)
        self.timeout_type = timeout_type

    def __repr__(self):
        return ("<%s at %s event_id=%s activity_type=%s activity_id=%s "
                "timeout_type=%s>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.activity_type, self.activity_id,
                    self.timeout_type)


class ActivityTaskCanceledError(CancelledError, ActivityTaskError, DecisionExceptionWithTracebackMixIn):
    def __init__(self, event_id, activity_type, activity_id, cause,
                 latest_cancel_requested_event_id, scheduled_event_id, started_event_id,
                 _traceback=None):
        super(ActivityTaskCanceledError, self).__init__(event_id, activity_type, activity_id)

        self._cause = cause
        self.latest_cancel_requested_event_id = latest_cancel_requested_event_id
        self.scheduled_event_id = scheduled_event_id
        self.started_event_id = started_event_id

    @property
    def cause(self):
        return self._cause

    def __repr__(self):
        return ("<%s at %s event_id=%s activity_type=%s activity_id=%s "
                "cause=%s latest_cancel_requested_event_id=%s "
                "scheduled_event_id=%s started_event_id=%s >") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.activity_type, self.activity_id,
                    self.cause, self.latest_cancel_requested_event_id,
                    self.scheduled_event_id, self.started_event_id)


class RequestCancelActivityTaskFailedError(DecisionException):
    """Request to cancel an activity task failed"""
    def __init__(self, event_id, activity_id, cause, decision_task_completed_event_id):
        super(RequestCancelActivityTaskFailedError, self).__init__(
            event_id, activity_id, decision_task_completed_event_id)
        self.activity_id = activity_id
        self.cause = cause
        self.decision_task_completed_event_id = decision_task_completed_event_id

    def __repr__(self):
        return ("<%s at %s event_id=%s activity_id=%s cause=%s "
                "decision_task_completed_event_id=%s >") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.activity_id, self.cause,
                    self.decision_task_completed_event_id)


class WorkflowError(DecisionException):
    """Base class for exceptions used to report failure of the originating
    workflow"""
    def __init__(self, event_id, workflow_type, workflow_execution):
        super(WorkflowError, self).__init__(event_id)

        self.workflow_type = workflow_type
        self.workflow_execution = workflow_execution

    def __repr__(self):
        return ("<%s at %s event_id=%s workflow_type=%s "
                "workflow_execution=%s>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.workflow_type,
                    self.workflow_execution)


class WorkflowFailedError(WorkflowError, DecisionExceptionWithTracebackMixIn):
    """The workflow execution closed due to a failure.
    """
    def __init__(self, event_id, workflow_type, workflow_execution, cause,
                 _traceback=None):
        super(WorkflowFailedError, self).__init__(event_id, workflow_type,
                                                  workflow_execution)
        self.cause = cause
        self._traceback = _traceback

    def __repr__(self):
        return ("<%s at %s event_id=%s workflow_type=%s "
                "workflow_execution=%s cause=%r>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.workflow_type,
                    self.workflow_execution, self.cause)


class WorkflowTimedOutError(WorkflowError):
    """The workflow execution was closed because a time out was exceeded.
    """

    def __init__(self, event_id, workflow_type, workflow_execution):
        super(WorkflowTimedOutError, self).__init__(event_id, workflow_type,
                                                    workflow_execution)


class WorkflowTerminatedError(WorkflowError):
    """The workflow execution was terminated.
    """

    def __init__(self, event_id, workflow_type, workflow_execution):
        super(WorkflowTerminatedError, self).__init__(event_id, workflow_type,
                                                      workflow_execution)


class ExternalWorkflowError(Exception):
    """This is the base class for exceptions that represent failures to enact a
    decision on an external workflow by Amazon SWF. You can catch this exception
    to generically deal with such exceptions.
    """

    def __init__(self, decision_task_completed_event_id, initiated_event_id,
                 run_id, workflow_id, cause, *args):
        super(ExternalWorkflowError, self).__init__(
            decision_task_completed_event_id, initiated_event_id, run_id,
            workflow_id, *args)
        self.decision_task_completed_event_id = decision_task_completed_event_id
        self.initiated_event_id = initiated_event_id
        self.run_id = run_id
        self.workflow_id = workflow_id
        self.cause = cause

    def __repr__(self):
        return ("<%s at %s decision_task_completed_event_id=%s "
                "initiated_event_id=%s run_id=%s workflow_id=%s>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.decision_task_completed_event_id,
                    self.initiated_event_id, self.run_id, self.workflow_id)


class RequestCancelExternalWorkflowExecutionFailedError(ExternalWorkflowError):
    """Request to cancel an external workflow failed; likely due to
    invalid workflowID.
    """
    def __init__(self, decision_task_completed_event_id, initiated_event_id,
                 run_id, workflow_id, cause):
        super(RequestCancelExternalWorkflowExecutionFailedError, self).__init__(
            decision_task_completed_event_id, initiated_event_id, run_id,
            workflow_id, cause)


class ChildWorkflowError(DecisionException):
    """Base class for exceptions used to report failure of child workflow
    execution. The exception contains the Ids of the child workflow execution
    as well as its workflow type. You can catch this exception to deal with
    child workflow execution failures in a generic way.
    """

    def __init__(self, event_id, workflow_type, workflow_execution):
        super(ChildWorkflowError, self).__init__(event_id)

        self.workflow_type = workflow_type
        self.workflow_execution = workflow_execution

    def __repr__(self):
        return ("<%s at %s event_id=%s workflow_type=%s "
                "workflow_execution=%s>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.workflow_type,
                    self.workflow_execution)


class ChildWorkflowFailedError(ChildWorkflowError,
                               DecisionExceptionWithTracebackMixIn):
    """Unhandled exceptions in child workflows are reported back to the parent
    workflow implementation by throwing a
    :py:exc:`~.ChildWorkflowFailedError`. The original exception can be
    retrieved from the cause property of this exception. The exception also
    provides other information that is useful for debugging purposes, such as
    the unique identifiers of the child execution.
    """

    def __init__(self, event_id, workflow_type, workflow_execution,
                 cause, _traceback=None):
        """
        Exception used to communicate failure of remote activity.
        """
        super(ChildWorkflowFailedError, self).__init__(event_id, workflow_type,
                                                       workflow_execution)
        self.cause = cause
        self._traceback = _traceback

    def __repr__(self):
        return ("<%s at %s event_id=%s workflow_type=%s "
                "workflow_execution=%s cause=%r>") % (
                    self.__class__.__name__, hex(id(self)),
                    self.event_id, self.workflow_type,
                    self.workflow_execution, self.cause)


class ChildWorkflowTimedOutError(ChildWorkflowError):
    """This exception is thrown in parent workflow execution to report that a
    child workflow execution was timed out and closed by Amazon SWF. You should
    catch this exception if you want to deal with the forced closure of the
    child workflow, for example, to perform cleanup or compensation.
    """

    def __init__(self, event_id, workflow_type, workflow_execution):
        super(
            ChildWorkflowTimedOutError, self).__init__(event_id, workflow_type,
                                                       workflow_execution)


class ChildWorkflowTerminatedError(ChildWorkflowError):
    """This exception is thrown in parent workflow execution to report the
    termination of a child workflow execution. You should catch this exception
    if you want to deal with the termination of the child workflow, for
    example, to perform cleanup or compensation.
    """

    def __init__(self, event_id, workflow_type, workflow_execution):
        super(
            ChildWorkflowTerminatedError, self).__init__(event_id, workflow_type,
                                                         workflow_execution)
