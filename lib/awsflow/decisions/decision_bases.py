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

class DecisionBase(object):
    def __init__(self):
        self.decision = dict()


class ActivityDecisionBase(DecisionBase):

    def __init__(self, decision_id):
        super(ActivityDecisionBase, self).__init__()
        self.decision_id = decision_id

    def __repr__(self):
        return "<%s decision activity_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id, self.decision)


class RequestCancelExternalWorkflowDecisionBase(DecisionBase):
    def __init__(self, workflow_id, run_id):
        super(RequestCancelExternalWorkflowDecisionBase, self).__init__()
        self.decision_id = (workflow_id, run_id)

    def __repr__(self):
        return "<%s decision workflow_id=%s run_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id[0],
            self.decision_id[1], self.decision)


class RecordMarkerDecisionBase(DecisionBase):
    def __init__(self, decision_id):
        super(RecordMarkerDecisionBase, self).__init__()
        self.decision_id = decision_id

    def __repr__(self):
        return "<%s decision marker_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id, self.decision)


class SignalExternalWorkflowExecutionDecisionBase(DecisionBase):
    def __init__(self, workflow_id, run_id, signal_name):
        super(SignalExternalWorkflowExecutionDecisionBase, self).__init__()
        self.decision_id = (workflow_id, run_id, signal_name)

    def __repr__(self):
        return ("<%s decision workflow_id=%s run_id=%s signal_name "
                "%s details=%s>" % (
                    self.__class__.__name__, self.decision_id[0],
                    self.decision_id[1], self.decision_id[2], self.decision))


class StartChildWorkflowExecutionDecisionBase(DecisionBase):
    def __init__(self, workflow_type_name, workflow_type_version, workflow_id):
        super(StartChildWorkflowExecutionDecisionBase, self).__init__()
        self.decision_id = (workflow_type_name, workflow_type_version,
                            workflow_id)

    def __repr__(self):
        return ("<%s decision workflow_type_name=%s, "
                "workflow_type_version=%s, workflow_id=%s details=%s>" % (
                    self.__class__.__name__, self.decision_id[0],
                    self.decision_id[1], self.decision_id[2], self.decision))


class TimerDecisionBase(DecisionBase):

    def __init__(self, decision_id):
        super(TimerDecisionBase, self).__init__()
        self.decision_id = decision_id

    def __repr__(self):
        return "<%s decision timer_id=%s details=%s>" % (
            self.__class__.__name__, self.decision_id, self.decision)


class WorkflowDecisionBase(DecisionBase):

    def __init__(self):
        super(WorkflowDecisionBase, self).__init__()

    def __repr__(self):
        return "<%s decision details=%s>" % (
            self.__class__.__name__, self.decision)
