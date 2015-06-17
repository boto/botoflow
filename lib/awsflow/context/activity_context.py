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

from awsflow.core.exceptions import CancellationError

from .context_base import ContextBase


class ActivityContext(ContextBase):
    # TODO document

    def __init__(self, worker, task):
        """

        :param worker:
        :type worker: awsflow.workers.activity_worker.ActivityWorker
        :param task:
        :type task: awsflow.workers.activity_task.ActivityTask
        :return:
        """

        self.worker = worker
        self.task = task

    def heartbeat(self, details):
        """Heartbeats current activity, raising CancellationError if cancel requested.

        Ignore request by catching teh exception; honor it by letting it raise or by
        re-raising as subclass.
        """
        result = self.worker.request_heartbeat(details, self.task)
        if result['cancelRequested']:
            raise CancellationError('Cancel was requested during heartbeat.')
