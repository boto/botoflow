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

import logging

import six

from ..utils import extract_workflows_dict
from ..decider import Decider
from ..swf_exceptions import swf_exception_wrapper, TypeAlreadyExistsError

from .base_worker import BaseWorker

log = logging.getLogger(__name__)

def get_workflow_entrypoint(definition_class, workflow_name, workflow_version):
    """Get the entry point information from *workflow_class*.

    This function provides a convenient way to extract the parameters
    that need to be returned the *get_workflow* argument to
    :py:class:`~.GenericWorkflowWorker`

    :param definition_class: Class which defines the workflow
    :type definition_class: child class of botoflow.workflow_definition.WorkflowDefinition
    :param str workflow_name: The name of the workflow
    :param str workflow_version: The version of the workflow
    :return: Return a tuple of (*definition_class*, *workflow_type", *entrypoint_func_name") 
    """
    return extract_workflows_dict([definition_class])[workflow_name, workflow_version]


class GenericWorkflowWorker(BaseWorker):
    """As the name suggests, this worker class is intended for use by
    the workflow implementation. It is configured with a workflow
    finding function.

    The worker class runs a loop to poll for decision tasks in the
    specified task list. When a decision task is received, it first
    looks up the workflow definition class using *get_workflow* and then
    creates an instance of it and calls the @
    :py:func:`~botoflow.decorators.execute` decorated method to process
    the task.

    :param session: botocore session object.
    :type session: botocore.session.Session
    :param aws_region: aws region to connect to
    :type aws_region: str
    :param str domain: SWF domain to operate on.
    :param str task_list: default task list on which to put all the workflow
        requests.
    :param func get_workflow:
        Callable that returns workflow information. This function takes
        (*workflow_name*, *workflow_version*) and returns a tuple of
        (*workflow_definition*, *workflow_type_object*, *function_name*).
        (see also :py:func:`~.get_workflow_entrypoint`) 

    This worker also acts as a context manager for starting new workflow
    executions. See the following example on how to start a workflow:
    """
    def __init__(self, session, aws_region, domain, task_list, get_workflow):
        super(GenericWorkflowWorker, self).__init__(session, aws_region, domain, task_list)

        self._get_workflow = get_workflow
        self._setup()

    def __getstate__(self):
        newdict = BaseWorker.__getstate__(self)
        del newdict['_decider']
        return newdict
    
    def __setstate__(self, newdict):
        BaseWorker.__setstate__(self, newdict)
        self.__dict__ = newdict
        self._setup()

    def _setup(self):
        get_workflow = self._get_workflow_finder()
        self._decider = Decider(self, self.domain, self.task_list,
                                get_workflow, self.identity)
    
    def _get_workflow_finder(self):
        return self._get_workflow

    # This is generally useful, though not used by this class.
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
            with swf_exception_wrapper():
                self.client.register_workflow_type(**options)
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


class WorkflowWorker(GenericWorkflowWorker):
    """As the name suggests, this worker class is intended for use by the
    workflow implementation. It is configured with a task list and the workflow
    implementation type. The worker class runs a loop to poll for decision
    tasks in the specified task list. When a decision task is received, it
    creates an instance of the workflow implementation and calls the @
    :py:func:`~botoflow.decorators.execute` decorated method to process the
    task.

    :param session: botocore session object.
    :type session: botocore.session.Session
    :param aws_region: aws region to connect to
    :type aws_region: str
    :param str domain: SWF domain to operate on.
    :param str task_list: default task list on which to put all the workflow
        requests.
    :param workflow_definitions: WorkflowDefinition subclass(es)

    This worker also acts as a context manager for starting new workflow
    executions. See the following example on how to start a workflow:

    .. code-block:: python

        # create the workflow worker using botocore endpoint and register
        # ExampleWorkflow class
        wf_worker = WorkflowWorker(session, "us-east-1", "SOMEDOMAIN", "MYTASKLIST",
                                   ExampleWorkflow)
        wf_worker.run()

    """

    def __init__(self, session, aws_region, domain, task_list,
                 *workflow_definitions):

        # holds all of our workflows
        self._workflow_definitions = workflow_definitions

        super(WorkflowWorker, self).__init__(session, aws_region, domain, task_list, None)

        self._register_all_workflows()
    
    def __getstate__(self):
        newdict = super(WorkflowWorker, self).__getstate__()
        del newdict['_workflows']
        return newdict

    def _register_all_workflows(self):
        if not getattr(self, '_workflows', None):
            self._setup_workflow_definitions()

        for _, workflow_type, _ in six.itervalues(self._workflows):
            self._register_workflow_type(workflow_type)

    def _get_workflow_finder(self):
        self._setup_workflow_definitions()
        return lambda name, version: self._workflows[(name, version)]

    def _setup_workflow_definitions(self):
        self._workflows = extract_workflows_dict(self._workflow_definitions)

