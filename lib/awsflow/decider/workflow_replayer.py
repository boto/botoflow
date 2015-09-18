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

import json

from .decider import Decider
from .decision_task_poller import DecisionTaskPoller
from ..utils import extract_workflows_dict


class ReplayingDecisionTaskPoller(DecisionTaskPoller):

    def single_poll(self, next_page_token=None):
        try:
            result = json.loads(self.history_pages[self.position])
            result['startedEventId'] = '1'
            result['previousStartedEventId'] = '0'
            result['taskToken'] = 'test_token'
            result['workflowExecution'] = self.workflow_description[
                'executionInfo']['execution']
            result['workflowType'] = self.workflow_description[
                'executionInfo']['workflowType']
            return result
        finally:
            self.position += 1


class WorkflowReplayer(object):

    def from_history_dump(self, workflows, workflow_description,
                          history_pages):
        self._history_pages = history_pages
        self._workflows = extract_workflows_dict(workflows)

        self._workflow_description = json.loads(workflow_description)

        self._poller = ReplayingDecisionTaskPoller
        self._poller.workflow_description = self._workflow_description
        self._poller.history_pages = history_pages
        self._poller.position = 0
        return self

    def replay(self):
        decider = Decider(
            swf_client=None,
            domain='replaying_domain',
            task_list=self._workflow_description[
                'executionConfiguration']['taskList']['name'],
            workflows=self._workflows,
            identity='replaying_decider_ident',
            _Poller=self._poller
        )

        while True:
            decider.decide()
