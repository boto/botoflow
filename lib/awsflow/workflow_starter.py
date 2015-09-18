# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
#
import time
import logging

from .context import StartWorkflowContext, get_context, set_context
from .utils import random_sha1_hash
from .swf_exceptions import swf_exception_wrapper
from .exceptions import (
    WorkflowFailedError, WorkflowTimedOutError, WorkflowTerminatedError)

log = logging.getLogger(__name__)


class WorkflowStarter(object):
    """Use this context manager to start a new workflow execution

    Example:

    .. code-block:: python

        # start the workflow using botocore session and ExampleWorkflow class
        # with a random workflow_id
        with WorkflowStarter(session, "us-east-1", "SOMEDOMAIN", "DEFAULT_TASKLIST"):
            instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
            print instance.workflow_execution.workflow_id
            # will print the workflow execution ID
    """

    def __init__(self, session, aws_region, domain, default_task_list):
        """

        :param session: BotoCore session.
        :type session: botocore.session.Session
        :param aws_region:
        :type aws_region: str
        :param domain:
        :type domain: str
        :param default_task_list:
        :type default_task_list: str
        :return:
        """
        self.domain = domain
        self.task_list = default_task_list
        self.client = session.create_client(
            service_name='swf', region_name=aws_region)

    def __enter__(self):
        try:
            self._other_context = get_context()
        except AttributeError:
            self._other_context = None

        set_context(StartWorkflowContext(self))
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, value, traceback):
        set_context(self._other_context)

    def wait_for_completion(self, workflow_instance, poll_sleep_time, attempt_count=None):
        workflow_execution = workflow_instance.workflow_execution
        data_converter = workflow_instance._data_converter

        attempt_nr = 0
        while attempt_count is None or attempt_nr < attempt_count:
            # we sleep first since there's usually not much point in trying
            # to check for completion right after the workflow started
            time.sleep(poll_sleep_time)

            attempt_nr += 1

            execution_status, close_status, workflow_type = self._get_workflow_execution_status(
                workflow_instance.workflow_execution)

            if execution_status == 'OPEN':
                continue

            if close_status == 'COMPLETED':
                return self._load_workflow_execution_result(
                    workflow_execution,data_converter)

            elif close_status == 'FAILED':
                return self._load_failed_workflow_execution_result(
                    workflow_execution, workflow_type, data_converter)

            elif close_status == 'TIMED_OUT':
                last_event = self._get_last_event(workflow_execution)

                raise WorkflowTimedOutError(last_event['eventId'], workflow_type,
                                            workflow_execution)
            elif close_status == 'TERMINATED':
                last_event = self._get_last_event(workflow_execution)
                raise WorkflowTerminatedError(last_event['eventId'], workflow_type,
                                              workflow_execution)

    def _get_workflow_execution_status(self, workflow_execution):
        with swf_exception_wrapper():
            workflow_execution = self.client.describe_workflow_execution(
                domain=self.domain,
                execution={'workflowId': workflow_execution.workflow_id,
                           'runId': workflow_execution.run_id})

        execution_status = workflow_execution['executionInfo']['executionStatus']
        if execution_status != 'OPEN':
            return (execution_status, workflow_execution['executionInfo']['closeStatus'],
                    workflow_execution['executionInfo']['workflowType'])

        return execution_status, None, workflow_execution['executionInfo']['workflowType']

    def _load_workflow_execution_result(self, workflow_execution, data_converter):
        last_event = self._get_last_event(workflow_execution)

        return data_converter.loads(
            last_event['workflowExecutionCompletedEventAttributes']['result'])

    def _load_failed_workflow_execution_result(self, workflow_execution, workflow_type,
                                               data_converter):
        last_event = self._get_last_event(workflow_execution)

        exc, traceback = data_converter.loads(
            last_event['workflowExecutionFailedEventAttributes']['details'])

        # raise the unmarshalled wokflow execution failure
        raise WorkflowFailedError(last_event['eventId'],
                                  (workflow_type['name'], workflow_type['version']),
                                  workflow_execution, exc, traceback)

    def _get_last_event(self, workflow_execution):
        workflow_execution_history = None
        try:
            with swf_exception_wrapper():
                # start with no token to request the first page of events
                next_page_token = None
                while True:
                    kwargs = {'domain': self.domain,
                              'execution':
                                {'workflowId': workflow_execution.workflow_id,
                                 'runId': workflow_execution.run_id}}
                    if next_page_token:
                        # if we have a token, request the next page of events
                        kwargs['nextPageToken'] = next_page_token

                    workflow_execution_history = self.client.get_workflow_execution_history(**kwargs)

                    try:
                        # paginated results: record token to request next page
                        next_page_token = workflow_execution_history['nextPageToken']
                    except KeyError:
                        # no more pages
                        break
        except Exception as err:
            log.error("Failed to retrieve the workflow execution history from SWF, error %r", err, exc_info=True)
            raise err

        # return the final event in the workflow execution history
        if workflow_execution_history is not None and 'events' in workflow_execution_history:
            return workflow_execution_history['events'][-1]

    def _start_workflow_execution(self, workflow_type, *args, **kwargs):
        """Calls SWF to start the workflow using our workflow_type"""
        decision_dict = workflow_type.to_decision_dict(
            [args, kwargs], random_sha1_hash(), self.task_list,
            self.domain)

        log.debug("Starting workflow execution with args: %s",
                  decision_dict)

        with swf_exception_wrapper():
            response = self.client.start_workflow_execution(**decision_dict)
        return decision_dict['workflowId'], response['runId']

