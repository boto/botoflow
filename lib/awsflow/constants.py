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

"""This module contains various constants

:data USE_WORKER_TASK_LIST: Use task list of the ActivityWorker or
    WorkflowWorker that is used to register activity or workflow as the defalt
    task list for the activity or workflow type.

:data NO_DEFAULT_TASK_LIST: Do not specify task list on registration. Which
    means that task list is required when scheduling activity.

:data CHILD_TERMINATE: The child executions will be terminated.

:data CHILD_REQUEST_CANCEL: Request to cancel will be attempted for each child
    execution by recording a 'WorkflowExecutionCancelRequested' event in its
    history. It is up to the decider to take appropriate actions when it
    receives an execution history with this event.

:data CHILD_ABANDON: Child policy to abandon (let the child workflow continue)
    the parent workflow

:data SECONDS: 2*SECONDS = 2
:data MINUTES: 2*MINUTES = 120
:data HOURS: 2*HOURS = 7200
:data DAYS: 2*DAYS = 172800
:data WEEKS: 2*WEEKS = 1209600

"""

# TASK LIST SETTINGS
USE_WORKER_TASK_LIST = "USE_WORKER_TASK_LIST"
NO_DEFAULT_TASK_LIST = "NO_DEFAULT_TASK_LIST"

# CHILD POLICIES
CHILD_TERMINATE = "TERMINATE"
CHILD_REQUEST_CANCEL = "REQUEST_CANCEL"
CHILD_ABANDON = "ABANDON"

# TIME MULTIPLIERS
SECONDS = 1
MINUTES = 60
HOURS = 3600
DAYS = 86400
WEEKS = 604800
