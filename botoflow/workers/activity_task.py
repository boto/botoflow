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

from ..workflow_execution import WorkflowExecution


class ActivityTask(object):
    """**INTERNAL**
    Unit of work sent to an activity worker.
    """

    def __init__(self, task_dict):
        """
        Used to construct the object from SWF task dictionary
        """
        self.id = task_dict['activityId']
        self.name = task_dict['activityType']['name']
        self.version = task_dict['activityType']['version']
        self.input = task_dict['input']
        self.started_event_id = task_dict['startedEventId']
        self.token = task_dict['taskToken']
        self.workflow_execution = WorkflowExecution(
            task_dict['workflowExecution']['workflowId'],
            task_dict['workflowExecution']['runId'])


    # TODO implement __repr__
