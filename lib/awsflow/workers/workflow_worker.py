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

import copy
import logging

from ..utils import extract_workflows_dict
from ..decider import Decider
from ..swf_exceptions import TypeAlreadyExistsError

from .base_worker import BaseWorker

log = logging.getLogger(__name__)


class WorkflowWorker(BaseWorker):
    """As the name suggests, this worker class is intended for use by the
    workflow implementation. It is configured with a task list and the workflow
    implementation type. The worker class runs a loop to poll for decision
    tasks in the specified task list. When a decision task is received, it
    creates an instance of the workflow implementation and calls the @
    :py:func:`~awsflow.decorators.execute` decorated method to process the
    task.

    :param endpoint: botocore Endpoint object.
    :type endpoint: botocore.endpoint.endpoint
    :param str domain: SWF domain to operate on.
    :param str task_list: default task list on which to put all the workflow
        requests.
    :param workflow_definitions: WorkflowDefinition subclass(es)

    This worker also acts as a context manager for starting new workflow
    executions. See the following example on how to start a workflow:

    .. code-block:: python

        # create the workflow worker using botocore endpoint and register
        # ExampleWorkflow class
        wf_worker = WorkflowWorker(endpoint, "SOMEDOMAIN", "MYTASKLIST",
                                   ExampleWorkflow)
        wf_worker.run()

    """

    def __init__(self, endpoint, domain, task_list,
                 *workflow_definitions):

        super(WorkflowWorker, self).__init__(endpoint, domain, task_list)

        # holds all of our workflows
        self._workflow_definitions = workflow_definitions

        self._register_workflow_type_op = self._build_swf_op(
            "RegisterWorkflowType")
        self._poll_for_decision_task_op = self._build_swf_op(
            "PollForDecisionTask")
        self._respond_decision_task_completed_op = self._build_swf_op(
            "RespondDecisionTaskCompleted")
        self._signal_workflow_execution_op = self._build_swf_op(
            "SignalWorkflowExecution")

        self._setup_workflow_definitions()
        self._setup()

    def _setup(self):
        self._decider = Decider(self, self.domain, self.task_list,
                                self._workflows, self.identity)

    def __getstate__(self):
        newdict = copy.copy(self.__dict__)
        del newdict['_workflows']
        del newdict['_decider']
        return newdict

    def __setstate__(self, newdict):
        self.__dict__ = newdict
        # register=False as we're assuming the object was already created
        self._setup_workflow_definitions(register=False)
        self._setup()

    def _setup_workflow_definitions(self, register=True):
        self._workflows = extract_workflows_dict(self._workflow_definitions)

        if register:
            for _, workflow_type, _ in self._workflows.itervalues():
                self._register_workflow_type(workflow_type)

    def _register_workflow_type(self, workflow_type):
        if workflow_type.skip_registration:
            log.debug("Skipping workflow '%s %s' registration",
                      workflow_type.name, workflow_type.version)
            return

        options = workflow_type.to_registration_options_dict(
            self.domain, self.task_list)

        log.debug("Registering workflow with the following "
                  "options: %s", options)

        try:
            self._register_workflow_type_op(**options)
        except TypeAlreadyExistsError:
            log.debug("Workflow '%s %s' already registered",
                      workflow_type.name, workflow_type.version)

    def run(self):
        """Run this worker forever (or till SIGINT).
        """
        while True:
            self.run_once()

    def run_once(self):
        """Run this worker once (perform one decision loop).
        """
        self._decider.decide()

