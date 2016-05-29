import unittest
from botoflow.context import set_context, DecisionContext

from botoflow.options import activity_options, workflow_options

class TestOptions(unittest.TestCase):

    def tearDown(self):
        set_context(None)

    def test_activity_overrides(self):
        context = DecisionContext(None)
        set_context(context)

        self.assertFalse(context._activity_options_overrides)
        with activity_options(task_list='Test'):
            self.assertEqual(context._activity_options_overrides['task_list'],
                             {'name':'Test'})
        self.assertFalse(context._activity_options_overrides)

    def test_workflow_overrides(self):
        context = DecisionContext(None)
        set_context(context)

        self.assertFalse(context._workflow_options_overrides)
        with workflow_options(child_policy='TERMINATE'):
            self.assertEqual(
                context._workflow_options_overrides['child_policy'],
                'TERMINATE')
        self.assertFalse(context._workflow_options_overrides)

if __name__ == '__main__':
    unittest.main()
