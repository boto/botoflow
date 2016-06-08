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

from collections import namedtuple


class WorkflowExecution(namedtuple('WorkflowExecution', 'workflow_id run_id')):
    """Contains workflow execution information provided by SWF.

    .. py:attribute:: workflow_id

        Either provided or randomly generated Workflow ID. There can only be one workflow running with the same
        Workflow ID.

        :rtype: str

    .. py:attribute:: run_id

        SWF generated and provided Run ID associated with a particular workflow execution

        :rtype: str

    """


def workflow_execution_from_swf_event(event):
    attributes = event.attributes
    if 'workflowExecution' in attributes:
        attributes = attributes['workflowExecution']
    return WorkflowExecution(attributes['workflowId'], attributes['runId'])
