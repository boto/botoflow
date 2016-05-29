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

__all__ = ('activity_options', 'workflow_options')

import six

from .context import get_context
from .utils import str_or_NONE


# noinspection PyPep8Naming
class _NOT_SET(object):
    pass


# noinspection PyPep8Naming
class activity_options(object):
    """This context manager helps override activity task options.

    Please see :py:func:`botoflow.decorators.activity` for available keyword
    arguments and their descriptions.

    Example usage::

        class ExampleWorkflow(WorkflowDefinition):

            @execute(version='1.0', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):

                # override the start_to_close_timeout in all activities in this
                # context
                with activity_options(start_to_close_timeout=2*MINUTES):
                    arg_sum = yield BunchOfActivities.sum(arg1, arg2)
                return_(arg_sum)
    """

    def __init__(self, task_list=_NOT_SET, version=_NOT_SET, name=_NOT_SET, heartbeat_timeout=_NOT_SET,
                 schedule_to_start_timeout=_NOT_SET, start_to_close_timeout=_NOT_SET,
                 schedule_to_close_timeout=_NOT_SET):
        """Override activity task options

        :param str task_list: Specifies the task list to be registered with Amazon SWF for this activity type.
        :param str version: Specifies the version of the activity type.
        :param str name: Specifies the name of the activity type.
        :param int heartbeat_timeout: Specifies the heartbeat timeout override. Activity workers must provide heartbeat
            within this duration; otherwise, the task will be timed out. Set to `None` by default, which is a special
            value that indicates this timeout should be disabled. See Amazon SWF API reference for more details.
        :param int schedule_to_start_timeout: Specifies the schedule to start timeout for this activity type. This is
            the maximum time a task of this activity type is allowed to wait before it is assigned to a worker.
        :param int start_to_close_timeout: Specifies the start to close timeout for this activity type. This timeout
            determines the maximum time a worker can take to process an activity task of this type.
        :param int schedule_to_close_timeout: Specifies the schedule to close timeout for this activity type. This
            timeout determines the total duration that the task can stay in open state.
        """
        self._overrides = dict()

        if task_list != _NOT_SET:
            self._overrides['task_list'] = {'name': str_or_NONE(task_list)}

        if version != _NOT_SET:
            self._overrides['version'] = str_or_NONE(version)
        if name != _NOT_SET:
            self._overrides['name'] = str_or_NONE(name)
        if heartbeat_timeout != _NOT_SET:
            self._overrides['heartbeat_timeout'] = str_or_NONE(heartbeat_timeout)
        if schedule_to_start_timeout != _NOT_SET:
            self._overrides['schedule_to_start_timeout'] = str_or_NONE(schedule_to_start_timeout)
        if start_to_close_timeout != _NOT_SET:
            self._overrides['start_to_close_timeout'] = str_or_NONE(start_to_close_timeout)
        if schedule_to_close_timeout != _NOT_SET:
            self._overrides['schedule_to_close_timeout'] = str_or_NONE(schedule_to_close_timeout)

    def __enter__(self):
        context = get_context()
        self._prev_opts = context._activity_options_overrides
        context._activity_options_overrides = self._overrides

    # noinspection PyUnusedLocal
    def __exit__(self, type, err, tb):
        get_context()._activity_options_overrides = self._prev_opts


# noinspection PyPep8Naming
class workflow_options(object):
    """Using this context manager you can override workflow execution options.

    Example::

        class MasterWorkflow(WorkflowDefinition):

            @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
            def execute(self, arg1, arg2):

                # override the default execution_start_to_close_timeout when
                # starting this child workflow
                with workflow_options(execution_start_to_close_timeout=4*MINUTES):
                    arg_sum = yield ChildWorkflow.execute(arg1, arg2)
                return_(arg_sum)
    """

    def __init__(self, task_list=_NOT_SET, workflow_id=_NOT_SET, version=_NOT_SET,
                 execution_start_to_close_timeout=_NOT_SET,
                 task_start_to_close_timeout=_NOT_SET, child_policy=_NOT_SET,
                 name=_NOT_SET, data_converter=_NOT_SET, tag_list=_NOT_SET):
        """Override workflow execution options

        :param str task_list: The task list for the decision tasks for executions of this workflow type.
        :param str workflow_id: Set explicit workflow ID
        :param str version: workflow version
        :param int execution_start_to_close_timeout: This specifies the total time a workflow execution of
            this type may take to complete.
        :param int task_start_to_close_timeout: This specifies the time a single decision task for a
            workflow execution of this type may take to complete.
        :param str child_policy: Specifies the policy to use for the child
            workflows if an execution of this type is terminated.
        :param data_converter: Specifies the type of the DataConverter to use for serializing/deserializing data when
            sending requests to and receiving results from workflow executions of this workflow type.
        :type data_converter: awsflow.data_converter.abstract_data_converter.AbstractDataConverter
        :param list tag_list: List of tags to associate with the workflow.
        """
        self._overrides = dict()

        if task_list != _NOT_SET:
            self._overrides['task_list'] = {'name': str_or_NONE(task_list)}

        if workflow_id != _NOT_SET:
            self._overrides['workflow_id'] = str_or_NONE(workflow_id)
        if version != _NOT_SET:
            self._overrides['version'] = str_or_NONE(version)
        if execution_start_to_close_timeout != _NOT_SET:
            self._overrides['execution_start_to_close_timeout'] = str_or_NONE(execution_start_to_close_timeout)
        if task_start_to_close_timeout != _NOT_SET:
            self._overrides['task_start_to_close_timeout'] = str_or_NONE(task_start_to_close_timeout)
        if child_policy != _NOT_SET:
            self._overrides['child_policy'] = str_or_NONE(child_policy)
        if name != _NOT_SET:
            self._overrides['name'] = str_or_NONE(name)
        if data_converter != _NOT_SET:
            self._overrides['data_converter'] = str_or_NONE(data_converter)
        if tag_list != _NOT_SET:
            self._overrides['tag_list'] = [str_or_NONE(tag) for tag in tag_list]

    def __enter__(self):
        context = get_context()
        self._prev_opts = context._workflow_options_overrides
        context._workflow_options_overrides = self._overrides

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, err, tb):
        get_context()._workflow_options_overrides = self._prev_opts
