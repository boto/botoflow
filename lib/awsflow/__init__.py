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

from .core import async, Return, return_, Future
from .context import get_context, set_context
from .workers import (GenericWorkflowWorker, WorkflowWorker, ActivityWorker, 
                      ThreadedWorkflowExecutor, ThreadedActivityExecutor, 
                      MultiprocessingWorkflowExecutor, MultiprocessingActivityExecutor)
from .decorators import workflow, execute, activity, manual_activity, activities, signal
from .options import workflow_options, activity_options
from .workflow_definition import WorkflowDefinition
from .workflow_starter import WorkflowStarter
from . import workflow_types as types

from warnings import warn as _warn


def MultiprocessingWorkflowWorker(*args, **kwargs):
    _warn("MultiprocessingWorkflowWorker is no longer supported, please "
          "migrate to MultiprocessingWorkflowExecutor model",
          DeprecationWarning, stacklevel=2)
    return MultiprocessingWorkflowExecutor(WorkflowWorker(*args, **kwargs))


def MultiprocessingActivityWorker(*args, **kwargs):
    _warn("MultiprocessingActivityWorker is no longer supported, please "
          "migrate to MultiprocessingActivityExecutor model",
          DeprecationWarning, stacklevel=2)
    return MultiprocessingActivityExecutor(ActivityWorker(*args, **kwargs))


def ThreadedWorkflowWorker(*args, **kwargs):
    _warn("ThreadedWorkflowWorker is no longer supported, please migrate to "
          "ThreadedWorkflowExecutor model", DeprecationWarning, stacklevel=2)
    return ThreadedWorkflowExecutor(WorkflowWorker(*args, **kwargs))


def ThreadedActivityWorker(*args, **kwargs):
    _warn("ThreadedActivityWorker is no longer supported, please migrate to "
          "ThreadedActivityExecutor model", DeprecationWarning, stacklevel=2)
    return ThreadedActivityExecutor(ActivityWorker(*args, **kwargs))