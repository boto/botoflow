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

from .context_base import ContextBase
from ..workflow_execution import WorkflowExecution

class DecisionContext(ContextBase):

    def __init__(self, decider):
        self.decider = decider

        self._workflow_time = 0
        self._replaying = True

        self._activity_options_overrides = dict()
        self._workflow_options_overrides = dict()

        self._workflow_instance = None
        self._workflow_execution = WorkflowExecution(None, None)

    @property
    def _replaying(self):
        """Do not use directly, instead please use
        ``botoflow.workflow_time.is_replaying``
        """
        return self.__replaying

    @_replaying.setter
    def _replaying(self, value):
        self.__replaying = value

    @property
    def workflow_execution(self):
        """Returns the current workflow execution information
        :rtype: botoflow.workflow_execution.WorkflowExecution
        """
        return self.__workflow_execution

    @workflow_execution.setter
    def workflow_execution(self, value):
        self.__workflow_execution = value

    @property
    def _workflow_time(self):
        """Do not use directly, instead please use
        ``botoflow.workflow_time.time``

        :returns: workflow time
        :rtype: datetime.datetime
        """
        return self.__time

    @_workflow_time.setter
    def _workflow_time(self, value):
        """INTERNAL: Never set the time yourself
        """
        self.__time = value

    @property
    def _workflow_instance(self):
        """Returns the currently executing workflow instance

        :rtype: awsflow.workflow_definition.WorkflowDefinition
        """
        return self.__workflow_instance

    @_workflow_instance.setter
    def _workflow_instance(self, value):
        self.__workflow_instance = value

