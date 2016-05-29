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

"""Internal: for use by decorators
"""
import functools
from .context import get_context, StartWorkflowContext, DecisionContext
from .core import async
from .test import WorkflowTestingContext


class SignalFunc(object):
    """This class follows the descriptor protocol and allows us
    to catch workflow instance the signal is being executed in.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        """
        if instance is None, it's called as classmethod
        """
        if instance is None:
            return self.func

        signal_type = self.func.swf_options['signal_type']
        # hard to know serde ahead of time, so we get it from our workflow
        # instance and set it onto the signal
        if signal_type.data_converter is None:
            signal_type.data_converter = instance._data_converter
        if signal_type.workflow_execution is None:
            signal_type.workflow_execution = instance.workflow_execution

        return signal_type


class ActivityFunc(object):
    """This class follows the descriptor protocol and allows us
    to properly detect if the activity is being called on an instance,
    or a class.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        """
        if instance is None, it's called as classmethod
        """
        # if in the workflow context, return future
        # otherwise, return the function

        activity_type = self.func.swf_options['activity_type']
        activity_type.retrying = self.func.swf_options.get('activity_retrying', None)

        activity_name_prefix = ""
        if hasattr(self.func.swf_options, 'activity_name_prefix'):
            activity_name_prefix = self.func.swf_options[
                'activity_name_prefix']
        activity_type._reset_name(cls, self.func, activity_name_prefix)

        context = None
        try:
            context = get_context()
        except AttributeError:  # not in context
            pass

        if isinstance(context, DecisionContext):
            # activity_type is callable, so no worries
            return activity_type
        elif isinstance(context, WorkflowTestingContext):
            # when you're unit-testing the workflow bits, you should not be
            # calling the activities, ever (it won't work and
            # produce a confusing message). Instead, we set the context to
            # WorkflowTestingContext during the test and if an activity is
            # called, we raise a better worded exception here.
            raise NotImplementedError(
                "Activity {0} must be stubbed/mocked when unit-testing "
                "decider. You cannot run actual activities when testing "
                "WorkflowDefinition/@async methods".format(activity_type.name))

        if instance is None:
            return self.func
        else:
            func = functools.partial(self.func, instance)
            functools.update_wrapper(func, self.func)
            return func


class WorkflowExecuteFunc(object):
    """This class follows the descriptor protocol and allows us
    to properly detect if the workflow execute is being called on an instance,
    or a class.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        """
        if instance is None, it's called as classmethod
        """
        # if in the workflow context, return future
        # otherwise, return the function

        workflow_type = self.func.swf_options['workflow_type']

        context = None
        try:
            context = get_context()
        except AttributeError:  # not in context
            pass

        if isinstance(context, StartWorkflowContext) \
           or (isinstance(context, DecisionContext)
               and context.decider.execution_started):

            # called on a class or instance does not matter
            func = functools.partial(workflow_type, (cls, instance))
            functools.update_wrapper(func, self.func)
            return func

        else:
            # async function
            func = functools.partial(async(self.func), instance)
            functools.update_wrapper(func, self.func)
            return func
