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

# -*- mode:python ; fill-column:120 -*-
from .decision_bases import (ActivityDecisionBase, WorkflowDecisionBase,
                             TimerDecisionBase, RecordMarkerDecisionBase,
                             RequestCancelExternalWorkflowDecisionBase,
                             SignalExternalWorkflowExecutionDecisionBase,
                             StartChildWorkflowExecutionDecisionBase)


class CancelWorkflowExecution(WorkflowDecisionBase):
    def __init__(self, details=None):
        """
        closes the workflow execution and records a WorkflowExecutionCanceled
        event in the history.
        """
        super(CancelWorkflowExecution, self).__init__()
        self.decision['decision_type'] = 'CancelWorkflowExecutions'
        attrs = self.decision[
            'cancel_workflow_executions_decision_attributes'] = {}
        if details is not None:
            attrs['details'] = details


class CancelTimer(TimerDecisionBase):
    def __init__(self, timer_id):
        """
        cancels a previously started timer and records a TimerCanceled event
        in the history.
        """
        super(CancelTimer, self).__init__(timer_id)
        self.decision['decision_type'] = 'CancelTimer'
        attrs = self.decision['cancel_timer_decision_attributes'] = {}
        attrs['timer_id'] = timer_id


class CompleteWorkflowExecution(WorkflowDecisionBase):
    def __init__(self, result=None):
        """
        closes the workflow execution and records a WorkflowExecutionCompleted
        event in the history
        """
        super(CompleteWorkflowExecution, self).__init__()
        self.decision['decision_type'] = 'CompleteWorkflowExecution'
        attrs = self.decision[
            'complete_workflow_execution_decision_attributes'] = {}
        if result is not None:
            attrs['result'] = result


class ContinueAsNewWorkflowExecution(WorkflowDecisionBase):
    def __init__(self,
                 child_policy=None,
                 execution_start_to_close_timeout=None,
                 input=None,
                 tag_list=None,
                 task_list=None,
                 start_to_close_timeout=None,
                 workflow_type_version=None):
        """
        closes the workflow execution and starts a new workflow execution of
        the same type using the same workflow id and a unique run Id. A
        WorkflowExecutionContinuedAsNew event is recorded in the history.
        """
        super(ContinueAsNewWorkflowExecution, self).__init__()
        self.decision['decision_type'] = 'ContinueAsNewWorkflowExecution'
        attrs = self.decision[
            'continue_as_new_workflow_execution_decision_attributes'] = {}
        if child_policy is not None:
            attrs['child_policy'] = child_policy
        if execution_start_to_close_timeout is not None:
            attrs[
                'execution_start_to_close_timeout'] = execution_start_to_close_timeout
        if input is not None:
            attrs['input'] = input
        if tag_list is not None:
            attrs['tag_list'] = tag_list
        if task_list is not None:
            attrs['task_list'] = task_list
        if start_to_close_timeout is not None:
            attrs['start_to_close_timeout'] = start_to_close_timeout
        if workflow_type_version is not None:
            attrs['workflow_type_version'] = workflow_type_version


class FailWorkflowExecution(WorkflowDecisionBase):
    def __init__(self,
                 reason=None,
                 details=None):
        """
        closes the workflow execution and records a WorkflowExecutionFailed event
        in the history.
        """
        super(FailWorkflowExecution, self).__init__()
        self.decision['decision_type'] = 'FailWorkflowExecution'
        attrs = self.decision['fail_workflow_execution_decision_attributes'] = {}
        if reason is not None:
            attrs['reason'] = reason
        if details is not None:
            attrs['details'] = details


class RecordMarker(RecordMarkerDecisionBase):
    def __init__(self, marker_name, details=None):
        """
        records a MarkerRecorded event in the history. Markers can be used for
        adding custom information in the history for instance to let deciders know
        that they do not need to look at the history beyond the marker event.
        """
        super(RecordMarker, self).__init__(marker_name)
        self.decision['decision_type'] = 'RecordMarker'
        attrs = self.decision['record_marker_decision_attributes'] = {}
        attrs['marker_name'] = marker_name
        if details is not None:
            attrs['details'] = details


class RequestCancelActivityTask(ActivityDecisionBase):
    def __init__(self, activity_id):
        """
        attempts to cancel a previously scheduled activity task. If the activity
        task was scheduled but has not been assigned to a worker, then it will
        be canceled. If the activity task was already assigned to a worker, then
        the worker will be informed that cancellation has been requested in the
        response to RecordActivityTaskHeartbeat.
        """
        super(RequestCancelActivityTask, self).__init__(activity_id)
        self.decision['decision_type'] = 'RequestCancelActivityTask'
        attrs = self.decision[
            'request_cancel_activity_task_decision_attributes'] = {}
        attrs['activity_id'] = activity_id


class RequestCancelExternalWorkflowExecution(RequestCancelExternalWorkflowDecisionBase):
    def __init__(self, workflow_id, run_id, control=None):
        """
        requests that a request be made to cancel the specified external workflow
        execution and records a
        RequestCancelExternalWorkflowExecutionInitiated event in the history.
        """
        super(RequestCancelExternalWorkflowExecution,
              self).__init__(workflow_id, run_id)
        self.decision[
            'decision_type'] = 'RequestCancelExternalWorkflowExecution'
        attrs = self.decision[
            'request_cancel_external_workflow_execution_decision_attributes'] = {}
        attrs['workflow_id'] = workflow_id
        attrs['run_id'] = run_id
        if control is not None:
            attrs['control'] = control


class ScheduleActivityTask(ActivityDecisionBase):
    def __init__(self, activity_id, activity_type_name, activity_type_version,
                 task_list=None, control=None, heartbeat_timeout=None,
                 schedule_to_close_timeout=None,
                 schedule_to_start_timeout=None, start_to_close_timeout=None,
                 input=None):
        """
        schedules an activity task

        :type activity_id: string
        :param activity_id: The activityId of the type of the activity
            being scheduled.

        :type activity_type_name: string
        :param activity_type_name: The name of the type of the activity
            being scheduled.

        :type activity_type_version: string
        :param activity_type_version: The version of the type of the
            activity being scheduled.

        :type task_list: string
        :param task_list: If set, specifies the name of the task list in
            which to schedule the activity task. If not specified, the
            defaultTaskList registered with the activity type will be used.
            Note: a task list for this activity task must be specified either
            as a default for the activity type or through this field. If
            neither this field is set nor a default task list was specified
            at registration time then a fault will be returned.
        """
        super(ScheduleActivityTask, self).__init__(activity_id)
        self.decision['decision_type'] = 'ScheduleActivityTask'
        attrs = self.decision['schedule_activity_task_decision_attributes'] = {}
        attrs['activity_id'] = activity_id
        attrs['activity_type'] = {
            'name': activity_type_name,
            'version': activity_type_version,
        }
        if task_list is not None:
            attrs['task_list'] = task_list
        if control is not None:
            attrs['control'] = control
        if heartbeat_timeout is not None:
            attrs['heartbeat_timeout'] = heartbeat_timeout
        if schedule_to_close_timeout is not None:
            attrs['schedule_to_close_timeout'] = schedule_to_close_timeout
        if schedule_to_start_timeout is not None:
            attrs['schedule_to_start_timeout'] = schedule_to_start_timeout
        if start_to_close_timeout is not None:
            attrs['start_to_close_timeout'] = start_to_close_timeout
        if input is not None:
            attrs['input'] = input


class SignalExternalWorkflowExecution(SignalExternalWorkflowExecutionDecisionBase):
    def __init__(self, workflow_id, run_id, signal_name,
                 control=None, input=None):
        """
        requests a signal to be delivered to the specified external workflow
        execution and records a SignalExternalWorkflowExecutionInitiated
        event in the history.
        """
        super(SignalExternalWorkflowExecution, self).__init__(workflow_id,
                                                              run_id, signal_name)
        self.decision['decision_type'] = 'SignalExternalWorkflowExecution'
        attrs = self.decision[
            'signal_external_eorkflow_execution_decision_attributes'] = {}
        attrs['workflow_id'] = workflow_id
        attrs['signal_name'] = signal_name
        if run_id is not None:
            attrs['run_id'] = run_id
        if control is not None:
            attrs['control'] = control
        if input is not None:
            attrs['input'] = input


class StartChildWorkflowExecution(StartChildWorkflowExecutionDecisionBase):
    def __init__(self, workflow_type, workflow_id, child_policy=None,
                 control=None, execution_start_to_close_timeout=None,
                 input=None, tag_list=None, task_list=None,
                 task_start_to_close_timeout=None):
        """
        requests that a child workflow execution be started and records a
        StartChildWorkflowExecutionInitiated event in the history.  The child
        workflow execution is a separate workflow execution with its own history.
        """
        super(StartChildWorkflowExecution, self).__init__(
            workflow_type['name'], workflow_type['version'], workflow_id)

        self.decision['decision_type'] = 'StartChildWorkflowExecution'
        attrs = self.decision[
            'start_child_workflow_execution_decision_attributes'] = {}
        attrs['workflow_type'] = workflow_type
        attrs['workflow_id'] = workflow_id
        if child_policy is not None:
            attrs['child_policy'] = child_policy
        if control is not None:
            attrs['control'] = control
        if execution_start_to_close_timeout is not None:
            attrs[
                'execution_start_to_close_timeout'] = execution_start_to_close_timeout
        if input is not None:
            attrs['input'] = input
        if tag_list is not None:
            attrs['tag_list'] = tag_list
        if task_list is not None:
            attrs['task_list'] = task_list
        if task_start_to_close_timeout is not None:
            attrs['task_start_to_close_timeout'] = task_start_to_close_timeout


class StartTimer(TimerDecisionBase):
    def __init__(self, timer_id, start_to_fire_timeout, control=None):
        """
        starts a timer for this workflow execution and records a TimerStarted
        event in the history.  This timer will fire after the specified delay
        and record a TimerFired event.
        """
        super(StartTimer, self).__init__(timer_id)
        self.decision = {}
        self.decision['decision_type'] = 'StartTimer'
        attrs = self.decision['start_timer_decision_attributes'] = {}
        attrs['start_to_fire_timeout'] = start_to_fire_timeout
        attrs['timer_id'] = timer_id
        if control is not None:
            attrs['control'] = control
