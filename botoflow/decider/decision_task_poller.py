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
import time

import six

from ..history_events import swf_event_to_object


class EventsIterator(six.Iterator):

    def __init__(self, poller, decision_dict):
        self.poller = poller
        self.decision_dict = decision_dict
        self.cur_event_pos = -1
        self.event_len = len(decision_dict['events'])

    def __next__(self):
        self.cur_event_pos += 1
        if self.cur_event_pos >= self.event_len:
            if 'nextPageToken' in self.decision_dict:
                self.decision_dict = self.poller.single_poll(
                    self.decision_dict['nextPageToken'])
                self.cur_event_pos = 0
                self.event_len = len(self.decision_dict['events'])
            else:
                raise StopIteration()

        return swf_event_to_object(
            self.decision_dict['events'][self.cur_event_pos])

    def contains(self, event_type):
        """
        :param event_type: type of event to search for
        :type event_type: awsflow.history_events.event_bases.EventBase
        :return: True if given event type exists among events
        :rtype: bool
        """
        return any(isinstance(swf_event_to_object(event), event_type)
                   for event in self.decision_dict['events'])


class DecisionTask(object):

    def __init__(self, poller, decision_dict):
        self._poller = poller

        self._decision_dict = decision_dict
        self.started_event_id = decision_dict['startedEventId']
        self.task_token = decision_dict['taskToken']
        self.previous_started_event_id = decision_dict[
            'previousStartedEventId']

        self.workflow_id = decision_dict['workflowExecution']['workflowId']
        self.run_id = decision_dict['workflowExecution']['runId']
        self.workflow_name = decision_dict['workflowType']['name']
        self.workflow_version = decision_dict['workflowType']['version']

    def __repr__(self):
        return ("<{0} workflow_name={1.workflow_name}, workflow_version={1.workflow_version}, "
                "started_event_id={1.started_event_id}, previous_started_event_id={1.previous_started_event_id} "
                "workflow_id={1.workflow_id}, run_id={1.run_id}>").format(self.__class__.__name__, self)

    @property
    def events(self):
        return EventsIterator(self._poller, self._decision_dict)


class DecisionTaskPoller(object):
    """
    Polls for decisions
    """

    def __init__(self, worker, domain, task_list, identity):
        self.worker = worker
        self.domain = domain
        self.task_list = task_list
        self.identity = identity

    def single_poll(self, next_page_token=None):
        poll_time = time.time()
        try:
            kwargs = {'domain': self.domain,
                      'taskList': {'name': self.task_list},
                      'identity': self.identity}
            if next_page_token is not None:
                kwargs['nextPageToken'] = next_page_token
            # old botocore throws TypeError when unable to establish SWF connection
            return self.worker.client.poll_for_decision_task(**kwargs)

        except KeyboardInterrupt:
            # sleep before actually exiting as the connection is not yet closed
            # on the other end
            sleep_time = 60 - (time.time() - poll_time)
            six.print_("Exiting in {0}...".format(sleep_time), file=sys.stderr)
            time.sleep(sleep_time)
            raise

    def poll(self):
        """
        Returns a paginating DecisionTask generator
        """
        decision_dict = self.single_poll()
        # from pprint import pprint
        # pprint(decision_dict)
        if decision_dict['startedEventId'] == 0:
            return None
        else:
            return DecisionTask(self, decision_dict)
