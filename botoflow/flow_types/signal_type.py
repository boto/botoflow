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

from ..context import get_context, StartWorkflowContext, ActivityContext
from ..swf_exceptions import swf_exception_wrapper

from .base_flow_type import BaseFlowType


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
