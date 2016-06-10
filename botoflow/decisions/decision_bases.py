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
"""
Decision bases are base classes for various decision classes, essentially grouping and reusing common bits between
actual decisions.

"""


class DecisionBase(object):
    """Every decision or decision base should inherit from this class

    .. py:data:: decision

        Contains a dictionary of decision attributes
    """

    def __init__(self):
        self.decision = {}


class ActivityDecisionBase(DecisionBase):
    """Base for Activity decisions
    """

    def __init__(self, decision_id):
        """
        :param str decision_id: Decision ID
        """
        super(ActivityDecisionBase, self).__init__()
        self.decision_id = decision_id

    def __repr__(self):
        return "<%s decision activity_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id, self.decision)


class RequestCancelExternalWorkflowDecisionBase(DecisionBase):
    """Base for requesting a cancellation of external workflow
    """

    def __init__(self, workflow_id, run_id):
        """
        :param str workflow_id: Workflow ID
        :param str run_id: Run ID
        """
        super(RequestCancelExternalWorkflowDecisionBase, self).__init__()
        self.decision_id = (workflow_id, run_id)

    def __repr__(self):
        return "<%s decision workflow_id=%s run_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id[0],
            self.decision_id[1], self.decision)


class RecordMarkerDecisionBase(DecisionBase):
    """Record marker decision base"""

    def __init__(self, decision_id):
        """
        :param str decision_id: Decision ID
        """
        super(RecordMarkerDecisionBase, self).__init__()
        self.decision_id = decision_id

    def __repr__(self):
        return "<%s decision marker_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id, self.decision)


class SignalExternalWorkflowExecutionDecisionBase(DecisionBase):
    """Signalling for external workflow
    """

    def __init__(self, workflow_id, run_id, signal_name):
        """
        :param str workflow_id: Workflow ID
        :param str run_id: Run ID
        :param str signal_name: name of the signal to execute
        """
        super(SignalExternalWorkflowExecutionDecisionBase, self).__init__()
        self.decision_id = (workflow_id, run_id, signal_name)

    def __repr__(self):
        return ("<%s decision workflow_id=%s run_id=%s signal_name "
                "%s details=%s>" % (
                    self.__class__.__name__, self.decision_id[0],
                    self.decision_id[1], self.decision_id[2], self.decision))


class StartChildWorkflowExecutionDecisionBase(DecisionBase):
    """Starting child workflow base
    """

    def __init__(self, workflow_type_name, workflow_type_version, workflow_id):
        """
        :param str workflow_type_name: Workflow type name
        :param str workflow_type_version: version of the workflow
        :param str workflow_id: Workflow ID
        """
        super(StartChildWorkflowExecutionDecisionBase, self).__init__()
        self.decision_id = (workflow_type_name, workflow_type_version,
                            workflow_id)

    def __repr__(self):
        return ("<%s decision workflow_type_name=%s, "
                "workflow_type_version=%s, workflow_id=%s details=%s>" % (
                    self.__class__.__name__, self.decision_id[0],
                    self.decision_id[1], self.decision_id[2], self.decision))


class TimerDecisionBase(DecisionBase):
    """Timer decisions base
    """

    def __init__(self, decision_id):
        """
        :param str decision_id: Decision ID
        """
        super(TimerDecisionBase, self).__init__()
        self.decision_id = decision_id

    def __repr__(self):
        return "<%s decision timer_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id, self.decision)


class WorkflowDecisionBase(DecisionBase):
    """Workflow decision base"""

    def __init__(self):
        super(WorkflowDecisionBase, self).__init__()

    def __repr__(self):
        return "<%s decision details=%s>" % (
            self.__class__.__name__, self.decision)
