import json

from mock import Mock, patch
import pytest

from awsflow.decider.workflow_execution_handler import WorkflowExecutionHandler

@pytest.fixture
def handler():
    return WorkflowExecutionHandler(Mock(name='decider'), 'task-list')

kwarg_dict = {'keyword': 'argument'}
arg_list = ['arg', 'list']
arg_tuple = tuple(arg_list)

valid_kwargs = (
    [[], kwarg_dict],
    ([], kwarg_dict),
    [(), kwarg_dict],
    ((), kwarg_dict),
    kwarg_dict,
)
@pytest.mark.parametrize('input', valid_kwargs)
def test_load_kwarg_input(handler, input):
    event = Mock()
    event.attributes = {'input': json.dumps(input)}
    print(event.attributes)
    assert handler._load_input(event)[0] == []
    assert handler._load_input(event)[1] == kwarg_dict


@pytest.mark.parametrize('args, kwargs', (
    [{'__tuple': ()}, kwarg_dict],
    [{'__tuple': ('a',)}, kwarg_dict],
))
def test_arg_return_values(handler, args, kwargs):
    event = Mock()
    event.attributes = {'input': json.dumps([args, kwargs])}
    assert handler._load_input(event)[0] == args['__tuple']
    assert handler._load_input(event)[1] == kwargs

def test_load_null_input(handler):
    event = Mock()
    event.attributes = {}
    assert handler._load_input(event) == ([], {})
