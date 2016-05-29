import unittest

from botoflow.utils import camel_keys_to_snake_case, snake_keys_to_camel_case


class TestUtils(unittest.TestCase):

    def test_camel_keys_to_snake_case(self):
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


    def test_snake_keys_to_camel_case(self):
        d = {
            'workflow_type': 'A',
            'task_list': 'B',
            'child_policy': 'C',
            'execution_start_to_close_timeout': 'D',
            'task_start_to_close_timeout': 'E',
            'input': 'F',
            'workflow_id': 'G',
            'domain': 'H'
        }

        self.assertDictEqual(snake_keys_to_camel_case(d), {
            'workflowType': 'A',
            'taskList': 'B',
            'childPolicy': 'C',
            'executionStartToCloseTimeout': 'D',
            'taskStartToCloseTimeout': 'E',
            'input': 'F',
            'workflowId': 'G',
            'domain': 'H'
        })


if __name__ == '__main__':
    unittest.main()
