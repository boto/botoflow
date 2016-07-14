# Copyright 2016 Darjus Loktevic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ..data_converter import JSONDataConverter, AbstractDataConverter
from ..constants import USE_WORKER_TASK_LIST, CHILD_TERMINATE
from ..utils import str_or_NONE, snake_keys_to_camel_case
from ..context import get_context, DecisionContext, StartWorkflowContext
from ..workflow_execution import WorkflowExecution

from .base_flow_type import BaseFlowType


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

    # noinspection PyShadowingBuiltins
    def to_decision_dict(self, input, workflow_id=None, worker_task_list=None, domain=None):
        task_list = self.task_list
        if task_list == USE_WORKER_TASK_LIST:
            task_list = worker_task_list

        serialized_input = self.data_converter.dumps(input)

        decision_dict = {
            'workflowType': {'version': self.version, 'name': self.name},
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

    # noinspection PyShadowingBuiltins
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
