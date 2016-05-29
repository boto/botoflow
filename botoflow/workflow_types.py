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


__all__ = ('WorkflowType', 'ActivityType', 'SignalType')

import abc

from copy import copy

from .utils import snake_keys_to_camel_case
from .swf_exceptions import swf_exception_wrapper
from .constants import USE_WORKER_TASK_LIST, CHILD_TERMINATE
from .utils import str_or_NONE
from .data_converter import AbstractDataConverter, JSONDataConverter
from .workflow_execution import WorkflowExecution
from .context import (get_context, DecisionContext,
                      StartWorkflowContext, ActivityContext)


class BaseFlowType(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def to_decision_dict(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def to_registration_options_dict(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def __call__(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def _reset_name(self):
        raise NotImplementedError()


class WorkflowType(BaseFlowType):

    _continue_as_new_keys = (('taskStartToCloseTimeout', 'task_start_to_close_timeout'),
                             ('childPolicy', 'child_policy'),
                             ('taskList', 'task_list'),
                             ('executionStartToCloseTimeout', 'execution_start_to_close_timeout'),
                             ('version', 'version'),
                             ('input', 'input'))

    DEFAULT_DATA_CONVERTER = JSONDataConverter()

    def __init__(self,
                 version,
                 execution_start_to_close_timeout,
                 task_list=USE_WORKER_TASK_LIST,
                 task_start_to_close_timeout=30,  # as in java flow
                 child_policy=CHILD_TERMINATE,
                 description="",
                 name=None,
                 data_converter=None,
                 skip_registration=False):

        self.version = version
        self.name = name
        self.task_list = task_list
        self.child_policy = child_policy
        self.execution_start_to_close_timeout = execution_start_to_close_timeout
        self.task_start_to_close_timeout = task_start_to_close_timeout
        self.description = description
        self.skip_registration = skip_registration
        self.workflow_id = None
        self.data_converter = data_converter

    @property
    def data_converter(self):
        return self._data_converter

    @data_converter.setter
    def data_converter(self, converter):
        if converter is None:  # set the default
            self._data_converter = self.DEFAULT_DATA_CONVERTER
            return

        if isinstance(converter, AbstractDataConverter):
            self._data_converter = converter
            return
        raise TypeError("Converter {0!r} must be a subclass of {1}"
                        .format(converter, AbstractDataConverter.__name__))

    def to_decision_dict(self, input, workflow_id=None, worker_task_list=None, domain=None):
        task_list = self.task_list
        if task_list == USE_WORKER_TASK_LIST:
            task_list = worker_task_list

        serialized_input = self.data_converter.dumps(input)

        decision_dict = {
            'workflowType': {'version': self.version,
                              'name': self.name},
            'taskList': {'name': str_or_NONE(task_list)},
            'childPolicy': str_or_NONE(self.child_policy),
            'executionStartToCloseTimeout': str_or_NONE(
                self.execution_start_to_close_timeout),
            'taskStartToCloseTimeout': str_or_NONE(
                self.task_start_to_close_timeout),
            'input': serialized_input}

        # for child workflows
        if workflow_id is not None and self.workflow_id is None:
            decision_dict['workflowId'] = workflow_id

        if domain is not None:
            decision_dict['domain'] = domain

        # apply any overrides
        context = get_context()

        _decision_dict = {}
        _decision_dict.update(decision_dict)
        _decision_dict.update(snake_keys_to_camel_case(context._workflow_options_overrides))

        return _decision_dict

    def to_continue_as_new_dict(self, input, worker_task_list):
        decision_dict = self.to_decision_dict(
            input, worker_task_list=worker_task_list)
        continue_as_new_dict = {}
        for key, continue_as_new_key in self._continue_as_new_keys:
            try:
                continue_as_new_dict[continue_as_new_key] = decision_dict[key]
            except KeyError:
                pass
        return continue_as_new_dict

    def to_registration_options_dict(self, domain, worker_task_list):
        if self.skip_registration:
            return None

        task_list = self.task_list
        if task_list == USE_WORKER_TASK_LIST:
            task_list = worker_task_list

        registration_options = {
            'domain': domain,
            'version': self.version,
            'name': self.name,
            'defaultTaskList': {'name': str_or_NONE(task_list)},
            'defaultChildPolicy': str_or_NONE(self.child_policy),
            'defaultExecutionStartToCloseTimeout': str_or_NONE(
                self.execution_start_to_close_timeout),
            'defaultTaskStartToCloseTimeout': str_or_NONE(
                self.task_start_to_close_timeout),
            'description': str_or_NONE(self.description)
        }
        return registration_options

    def _reset_name(self, name, force=False):
        # generate workflow name
        if self.name is None or force:
            self.name = name

    def __call__(self, __class_and_instance, *args, **kwargs):
        _class, _instance = __class_and_instance
        context = get_context()

        if isinstance(context, StartWorkflowContext):
            workflow_id, run_id = context.worker._start_workflow_execution(
                self, *args, **kwargs)
            # create an instance with our new workflow execution info
            workflow_instance = _class(WorkflowExecution(workflow_id, run_id))
            workflow_instance._data_converter = self.data_converter
            return workflow_instance

        elif isinstance(context, DecisionContext):
            if context.decider.execution_started:

                if context._workflow_instance == _instance:
                    continue_as_new_dict = self.to_continue_as_new_dict(
                        [args, kwargs], context.decider.task_list)

                    return context.decider._continue_as_new_workflow_execution(
                        **continue_as_new_dict)
                else:
                    # create an instance with our new workflow execution info
                    # but don't set the workflow_id and run_id as we don't yet
                    # know them
                    workflow_instance = _class(WorkflowExecution(None,
                                                                 None))
                    workflow_instance._data_converter = self.data_converter
                    future = context.decider._handle_start_child_workflow_execution(
                        self, workflow_instance, [args, kwargs])
                    return future
        else:
            raise NotImplementedError("Unsupported context")

    def __hash__(self):
        return hash("{0}{1}".format(self.name, self.version))

    def __repr__(self):
        return "<{} (name={}, version={})>".format(self.__class__.__name__,
                                                   self.name, self.version)


class ActivityType(BaseFlowType):

    DEFAULT_DATA_CONVERTER = JSONDataConverter()

    def __init__(self,
                 version,
                 name=None,
                 task_list=USE_WORKER_TASK_LIST,
                 heartbeat_timeout=None,
                 schedule_to_start_timeout=None,
                 start_to_close_timeout=None,
                 schedule_to_close_timeout=None,
                 description=None,
                 data_converter=None,
                 skip_registration=False,
                 manual=False):

        self.version = version
        self.name = name
        self.task_list = task_list
        self.heartbeat_timeout = heartbeat_timeout
        self.schedule_to_start_timeout = schedule_to_start_timeout
        self.start_to_close_timeout = start_to_close_timeout
        self.schedule_to_close_timeout = schedule_to_close_timeout
        self.description = description
        self.skip_registration = skip_registration
        self.manual = manual

        # retrying will be set by ActivityFunc, as it's a separate decorator
        # and we want to not care about the decorator order
        self.retrying = None

        if data_converter is None:
            self.data_converter = self.DEFAULT_DATA_CONVERTER
        else:
            self.data_converter = data_converter

    def __getstate__(self):
        """Prepare the activity type for serialization as it's included in ActivityTaskFailed exception, and it
        may be passed around by our clients for exception handling.

        But only serialize basic information about the activity, not to bloat serialized object

        :return: A serializable dict
        :rtype: dict
        """
        dct = copy(self.__dict__)
        if 'retrying' in dct:
            del dct['retrying']

        if 'data_converter' in dct:
            del dct['data_converter']

        if 'description' in dct:
            del dct['description']

        if 'skip_registration' in dct:
            del dct['skip_registration']

        return dct

    def __setstate__(self, dct):
        """Recreate our activity type based on the dct
        :param dct: a deserialized dictionary to recreate our state
        :type dct: dict
        """
        self.__dict__ = dct

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__getstate__() == other.__getstate__()
        return False

    def _set_activities_value(self, key, value):
        if getattr(self, key) is None:
            setattr(self, key, value)

    def _reset_name(self, cls, func, activity_name_prefix):
        # generate activity name
        _name = "%s%s" % (activity_name_prefix, func.__name__)
        if self.name is None:
            _name = "%s.%s" % (cls.__name__, func.__name__)

        else:
            _name = "%s%s" % (activity_name_prefix, self.name)
        self.name = _name

    def to_decision_dict(self):
        decision_dict = {
            'activity_type_version': self.version,
            'activity_type_name': self.name,
            'task_list': {'name': str_or_NONE(self.task_list)},
            'heartbeat_timeout': str_or_NONE(self.heartbeat_timeout),
            'schedule_to_start_timeout': str_or_NONE(
                self.schedule_to_start_timeout),
            'start_to_close_timeout': str_or_NONE(self.start_to_close_timeout),
            'schedule_to_close_timeout': str_or_NONE(
                self.schedule_to_close_timeout),
        }
        return decision_dict

    def to_registration_options_dict(self, domain, worker_task_list):
        if self.skip_registration:
            return None

        task_list = self.task_list
        if task_list == USE_WORKER_TASK_LIST:
            task_list = worker_task_list

        registration_options = {
            'domain': domain,
            'version': self.version,
            'name': self.name,
            'defaultTaskList': {'name': str_or_NONE(task_list)},
            'defaultTaskHeartbeatTimeout': str_or_NONE(
                self.heartbeat_timeout),
            'defaultTaskScheduleToStartTimeout': str_or_NONE(
                self.schedule_to_start_timeout),
            'defaultTaskStartToCloseTimeout': str_or_NONE(
                self.start_to_close_timeout),
            'defaultTaskScheduleToCloseTimeout': str_or_NONE(
                self.schedule_to_close_timeout),
            'description': str_or_NONE(self.description)
        }
        return registration_options

    def __call__(self, *args, **kwargs):
        """
        You can call this directly to support dynamic activities.
        """
        context = None
        try:
            context = get_context()
        except AttributeError:  # not in context
            pass

        if not isinstance(context, DecisionContext):
            raise TypeError("ActivityType can only be called in the decision "
                            "context")

        decision_dict = self.to_decision_dict()

        # apply any options overrides
        _decision_dict = {}
        _decision_dict.update(decision_dict)
        _decision_dict.update(context._activity_options_overrides.items())

        if self.retrying is not None:
            return self.retrying.call(context.decider._handle_execute_activity,
                                      self, _decision_dict, args, kwargs)

        return context.decider._handle_execute_activity(
            self, _decision_dict, args, kwargs)


class SignalType(BaseFlowType):

    def __init__(self, name, data_converter=None):
        """
        :param data_converter: (optional) Serializer to use for serializing inputs
        :type: botoflow.data_converter.AbstractDataConverter
        """
        self.name = name
        self.data_converter = data_converter
        self.workflow_execution = None

    def to_decision_dict(self):
        raise NotImplementedError("Not applicable to SignalType")

    def to_registration_options_dict(self):
        raise NotImplementedError("Not applicable to SignalType")

    def _reset_name(self):
        raise NotImplementedError("Not applicable to SignalType")

    def __call__(self, *args, **kwargs):
        """
        Records a WorkflowExecutionSignaled event in the workflow execution
        history and creates a decision task for the workflow execution
        identified by the given domain, workflow_execution.
        The event is recorded with the specified user defined name
        and input (if provided).

        :returns: Signals do not return anything
        :rtype: None

        :raises: UnknownResourceFault, OperationNotPermittedFault, RuntimeError
        """
        serialized_input = self.data_converter.dumps([args, kwargs])
        workflow_execution = self.workflow_execution

        context = get_context()
        if not isinstance(context, (StartWorkflowContext, ActivityContext)):
            raise RuntimeError(
                "Unsupported context for this call: %r" % context)

        with swf_exception_wrapper():
            context.worker.client.signal_workflow_execution(
                domain=context.worker.domain, signalName=self.name,
                workflowId=workflow_execution.workflow_id,
                runId=workflow_execution.run_id,
                input=serialized_input)

    def __repr__(self):
        return "<{0}(name={1.name}, data_converter={1.data_converter}, " \
               "workflow_execution={1.workflow_execution}".format(self.__class__.__name__, self)
