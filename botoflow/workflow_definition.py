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

__all__ = ('WorkflowDefinition', )

from copy import copy

import six
from botoflow import get_context, async, return_
from botoflow.context import DecisionContext
from botoflow.exceptions import CancelledError


class _WorkflowDefinitionMeta(type):

    def __new__(mcs, name, bases, dct):
        newdct = dict(dct)

        # copy workflow and signal methods from our bases if we don't have a
        # method with the same name. This is needed to make sure subclassing
        # works well as these methods contain metadata that will change between
        # super and subclass
        _workflow_types, _signals = _WorkflowDefinitionMeta \
            ._extract_workflows_and_signals(newdct)

        for base in bases:
            base_workflow_types, base_signals = _WorkflowDefinitionMeta \
                ._extract_workflows_and_signals(base.__dict__)

            # signals first
            for signal_name, val_func in six.iteritems(base_signals):
                if signal_name not in _signals and \
                   val_func[1].__name__ not in newdct:

                    newdct[val_func[1].__name__] = copy(val_func[0])
                    _signals[signal_name] = val_func

            for workflow_type, val_func in six.iteritems(base_workflow_types):
                if workflow_type not in _workflow_types and \
                   val_func[1].__name__ not in newdct:

                    newdct[val_func[1].__name__] = copy(val_func[0])
                    _workflow_types[workflow_type] = val_func

        newdct['_workflow_signals'] = _signals

        workflow_types = {}
        for workflow_type, workflow_func in six.iteritems(_workflow_types):
            workflow_type._reset_name(name, force=True)
            workflow_types[workflow_type] = workflow_func[1].__name__

        if not hasattr(mcs, '_workflow_types'):
            newdct['_workflow_types'] = workflow_types

        return type.__new__(mcs, name, bases, newdct)

    @staticmethod
    def _extract_workflows_and_signals(dct):
        workflow_types = {}
        signals = {}
        for val in six.itervalues(dct):
            if hasattr(val, 'func'):
                func = val.func
                if hasattr(func, 'swf_options'):
                    if 'signal_type' in func.swf_options:
                        signal_type = func.swf_options['signal_type']
                        signals[signal_type.name] = (val, func)
                    elif 'workflow_type' in func.swf_options:
                        workflow_types[
                            func.swf_options['workflow_type']] = (val, func)

        return workflow_types, signals


class WorkflowDefinition(six.with_metaclass(_WorkflowDefinitionMeta, object)):
    """Every workflow implementation needs to be a subclass of this class.

    Usually there should be no need to instantiate the class manually, as
    instead, the @execute method is called to start the workflow (you can think
    of ths as having factory class methods).

    Here's an example workflow implementation that has an @execute decorated
    method and a @signal:

    .. code-block:: python

        from botoflow import execute, Return, WorkflowDefinition
        from botoflow.constants import MINUTES

        from my_activities import MyActivities


        class MyWorkflow(WorkflowDefinition):

            @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
            def start_my_workflow(self, some_input):
                # execute the activity and wait for it's result
                result = yield MyActivities.activity1(some_input)

                # return the result from the workflow
                return_(result)

            @signal()  # has to have () parentheses
            def signal1(self, signal_input):
                self.signal_input = signal_input

    As with the @async decorated methods, returning values from the workflow is
    a little bit inconvenient on Python 2 as instead of using the familiar
    return keyword, the return value is "raised" like this: `raise
    Return("Value")`.
    """

    def __init__(self, workflow_execution):
        self.workflow_execution = workflow_execution
        self.workflow_state = ""
        self._workflow_result = None

    @property
    def workflow_execution(self):
        """Will contain the
        :py:class:`botoflow.workflow_execution.WorkflowExecution` named tuple
        of the currently running workflow.

        An example of the workflow_execution after starting a workflow:

        .. code-block:: python

            # create the workflow using boto swf_client and ExampleWorkflow class
            wf_worker = WorkflowWorker(swf_client, "SOMEDOMAIN", "MYTASKLIST"
                                       ExampleWorkflow)

            # start the workflow with a random workflow_id
            with wf_worker:
                instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
                print instance.workflow_execution
                # prints:
                # WorkflowExecution(
                #      workflow_id='73faf493fece67fefb1142739611c391a03bc23b',
                #      run_id='12Eg0ETHpm17rSWssUZKqAvEZVd5Ap0RELs8kE7U6igB8=')

        """
        return self.__workflow_execution

    @workflow_execution.setter
    def workflow_execution(self, workflow_execution):
        self.__workflow_execution = workflow_execution

    @property
    def workflow_state(self):
        """This property is used to retrieve current workflow state.
        The property is expected to perform read only access of the workflow
        implementation object and is invoked synchronously which disallows
        use of any asynchronous operations (like calling it with `yield`).

        The latest state reported by the workflow execution is returned to
        you through visibility calls to the Amazon SWF service and in the
        Amazon SWF console.

        Example of setting the state between `yield` s:

        .. code-block:: python

            class MyWorkflow(WorkflowDefinition):

                @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
                def start_workflow(self):
                    self.workflow_state = "Workflow started"
                    yield SomeActivity.method(1, 2)
                    self.workflow_state = "Workflow completing"
        """
        return self.__workflow_state

    @workflow_state.setter
    def workflow_state(self, state):
        self.__workflow_state = state

    @property
    def workflow_result(self):
        """This property returns the future associated with the result of the
        workflow execution.

        The main use-case is when you have subworkflows, which results you'd
        like to `yield` on and still be able to call signals on that
        sub-workflow.

        :returns: `botoflow.core.future.Future`, or None if the workflow has
            not been started.
        """
        return self._workflow_result

    def cancel(self, details=""):
        """Cancels the workflow execution

        If cancel initiated from context of the workflow itself, raises CancelledError. This
        will cascade the cancel request to all open activities and then cancel the workflow
        itself.

        If cancel initiated from a different (external) context, a cancel request decision is
        sent to SWF so the proper context can process the request.

        :param details: of request; is recorded in SWF history
        :type details: str
        :return: cancel Future that is empty until the message was succesfully delivered
            to target execution. Does not indicate whether the target execution accepted
            the request or not.
        :rtype: awsflow.core.Future
        :raises CancelledError: if workflow to cancel is of current context
        """
        context = self._get_decision_context(self.cancel.__name__)
        if self.workflow_execution == context._workflow_execution:
            raise CancelledError(details)
        return context.decider._request_cancel_external_workflow_execution(self.workflow_execution)

    @async
    def cancellation_handler(self):
        """Override to take cleanup actions before workflow cancels"""
        return_(None)

    def _get_decision_context(self, calling_func):
        """Validates in decision context and returns it.

        :param calling_func: name of calling function
        :type calling_func: str
        :return: decision context
        :rtype: awsflow.context.DecisionContext
        :raises TypeError: if not in decision context
        """
        context = None
        try:
            context = get_context()
        except AttributeError:  # not in context
            pass

        if not isinstance(context, DecisionContext):
            raise TypeError("{}.{} can only be called in the decision "
                            "context".format(self.__class__.__name__, calling_func))
        return context
