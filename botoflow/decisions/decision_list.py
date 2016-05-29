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


class DecisionList(list):
    """
    DecisionList is just like a regular list with a few additional methods
    """

    # TODO: validation of inputs

    def delete_decision(self, decision_type, decision_id):
        """delete a decision

        :returns: True if the decision was found and removed, False otherwise
        :rtype: bool
        """
        for decision in self:
            if not isinstance(decision, decision_type):
                continue

            if decision.decision_id == decision_id:
                self.remove(decision)
                return True

        return False

    def has_decision_type(self, *args):
        for decision in self:
            if isinstance(decision, args):
                return True
        return False

    def to_swf(self):
        """
        Returns a list of decisions ready to be consumend by swf api
        """
        swf_decisions = list()
        for decision in self:
            swf_decisions.append(decision.decision)
        return swf_decisions
