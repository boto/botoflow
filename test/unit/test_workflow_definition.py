
from botoflow import execute, signal
from botoflow import workflow_definition as wodef


class SpamWorkflow(wodef.WorkflowDefinition):

    @execute('1.0', 1)
    def execute0(self):
        pass

    @execute('1.0', 1)
    def execute1(self):
        pass

    @signal()
    def signal0(self):
        pass

    @signal()
    def signal1(self):
        pass


class SubSpamWorkflow(SpamWorkflow):

    @execute('1.1', 2)
    def execute0(self):
        pass

    @signal()
    def signal0(self):
        pass


def test_meta_subclass():
    assert set(SubSpamWorkflow._workflow_types.values()) == {'execute0', 'execute1'}
    assert SubSpamWorkflow._workflow_signals['signal0'][1] == SubSpamWorkflow.signal0
    assert SubSpamWorkflow._workflow_signals['signal1'][1] == SpamWorkflow.signal1
