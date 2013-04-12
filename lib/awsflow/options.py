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

import six

from .context import get_context
from .utils import str_or_NONE


class activity_options(object):
    """This context manager helps override activity task options.

    Please see :py:func:`awsflow.decorators.activity` for available keyword
    arguments and their descriptions.

    Example usage::

        class ExampleWorkflow(WorkflowDefinition):

            @execute(version='1.0', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):

                # override the start_to_close_timeout in all activities in this
                # context
                with activity_options(start_to_close_timeout=2*MINUTES):
                    arg_sum = yield BunchOfActivities.sum(arg1, arg2)
                raise Return(arg_sum)
    """

    keys = frozenset(('version', 'name', 'heartbeat_timeout',
                     'schedule_to_start_timeout',
                     'start_to_close_timeout',
                     'schedule_to_close_timeout'))

    def __init__(self, **kwargs):
        self._overrides = dict()

        for key, val in six.iteritems(kwargs):
            if key == 'task_list':
                self._overrides[key] = {'name': str_or_NONE(val)}
            elif key in self.keys:
                self._overrides[key] = str_or_NONE(val)
            else:
                raise AttributeError("Unknown keyword argument: %s" % key)

    def __enter__(self):
        context = get_context()
        self._prev_opts = context._activity_options_overrides
        context._activity_options_overrides = self._overrides

    def __exit__(self, type, err, tb):
        get_context()._activity_options_overrides = self._prev_opts


class workflow_options(object):
    """Using this context manager you can override workflow execution options.

    Please see :py:func:`awsflow.decorators.execute` and
    :py:func:`awsflow.decorators.workflow` for available keyword arguments and
    their descriptions.

    Example::

        class MasterWorkflow(WorkflowDefinition):

            @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
            def execute(self, arg1, arg2):

                # override the default execution_start_to_close_timeout when
                # starting this child workflow
                with workflow_options(execution_start_to_close_timeout=4*MINUTES):
                    arg_sum = yield ChildWorkflow.execute(arg1, arg2)
                raise Return(arg_sum)
    """
    keys = frozenset(('workflow_id', 'version',
                      'execution_start_to_close_timeout', 'task_list',
                      'task_start_to_close_timeout', 'child_policy',
                      'name', 'data_converter'))

    def __init__(self, **kwargs):
        self._overrides = dict()

        for key, val in six.iteritems(kwargs):
            if key == 'task_list':
                self._overrides[key] = {'name': str_or_NONE(val)}
            elif key in self.keys:
                self._overrides[key] = str_or_NONE(val)
            else:
                raise AttributeError("Unknown keyword argument: %s" % key)

    def __enter__(self):
        context = get_context()
        self._prev_opts = context._workflow_options_overrides
        context._workflow_options_overrides = self._overrides

    def __exit__(self, type, err, tb):
        get_context()._workflow_options_overrides = self._prev_opts
