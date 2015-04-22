import unittest

from awsflow.utils import camel_keys_to_snake_case


class TestUtils(unittest.TestCase):

    def test_translate_kwargs(self):
        d = {
            'workflowType': 'A',
            'taskList': 'B',
            'childPolicy': 'C',
            'executionStartToCloseTimeout': 'D',
            'taskStartToCloseTimeout': 'E',
            'input': 'F',
            'workflowId': 'G',
            'domain': 'H'
        }

        self.assertDictEqual(camel_keys_to_snake_case(d), {
            'workflow_type': 'A',
            'task_list': 'B',
            'child_policy': 'C',
            'execution_start_to_close_timeout': 'D',
            'task_start_to_close_timeout': 'E',
            'input': 'F',
            'workflow_id': 'G',
            'domain': 'H'
        })

if __name__ == '__main__':
    unittest.main()
