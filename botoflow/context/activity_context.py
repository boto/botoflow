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

from botoflow.core.exceptions import CancellationError

from .context_base import ContextBase


class ActivityContext(ContextBase):
    """ActivityContext is accessible from within activities via
    :py:func:`botoflow.get_context` and provides the ability to
    retrieve information about the workflow as well as access to the
    :py:meth:`heartbeat` for heartbeating the execution of activity
    """

    def __init__(self, worker, task):
        """

        :param worker:
        :type worker: botoflow.workers.activity_worker.ActivityWorker
        :param task:
        :type task: botoflow.workers.activity_task.ActivityTask
        """

        self.worker = worker
        self.task = task

    def heartbeat(self, details=None):
        """Heartbeats current activity, raising CancellationError if cancel requested.

        Ignore request by catching the exception, or let it raise to cancel.

        :param details: If specified, contains details about the progress of the task.
        :type details: str
        :raises CancellationError: if uncaught, will record this activity as cancelled
            in SWF history, and bubble up to the decider, where it will cancel the
            workflow.
        """
        result = self.worker.request_heartbeat(self.task, details)
        if result['cancelRequested']:
            raise CancellationError('Cancel was requested during activity heartbeat')

    @property
    def workflow_execution(self):
        """
        :returns: the information about current workflow that the activity is handling
        :rtype: botoflow.workflow_execution.WorkflowExecution
        """
        return self.task.workflow_execution
