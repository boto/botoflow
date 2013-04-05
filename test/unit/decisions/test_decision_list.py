import unittest

from awsflow.decisions import decision_list, decisions
class TestDecisionList(unittest.TestCase):

    def test_delete_decision(self):
        dlist = decision_list.DecisionList()
        dlist.append(decisions.CancelTimer(123))

        self.assertTrue(dlist)
        dlist.delete_decision(decisions.CancelTimer, 999)
        self.assertTrue(dlist)
        dlist.delete_decision(decisions.CancelTimer, 123)
        self.assertFalse(dlist)

    def test_to_swf(self):
        dlist = decision_list.DecisionList()
        dlist.append(decisions.CancelTimer(123))

        swf_list = dlist.to_swf()
        self.assertTrue(swf_list)
        self.assertEqual(swf_list, [{'cancel_timer_decision_attributes':
                                     {'timer_id': 123},
                                     'decision_type': 'CancelTimer'}])

if __name__ == '__main__':
    unittest.main()
