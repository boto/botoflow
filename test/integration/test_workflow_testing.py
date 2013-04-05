import unittest
import time

from awsflow.flow import *

from various_activities import BunchOfActivities

class MasterWorkflow(WorkflowDefinition):
    @execute(version='1.2', execution_start_to_close_timeout=60)
    def execute(self, arg1, arg2):
        arg_sum = yield BunchOfActivities.sum(arg1, arg2)
        raise Return(arg_sum)




class TestWorkfowTesting(unittest.TestCase):


    def test_workflow(self):
        MasterWorkflow.execute(1, 2)


if __name__ == '__main__':
    unittest.main()