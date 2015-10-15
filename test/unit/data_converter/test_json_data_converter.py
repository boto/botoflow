import copy
import zlib
import six

from decimal import Decimal
from collections import namedtuple, OrderedDict

import pytest

from awsflow import WorkflowDefinition, execute
from awsflow.data_converter import JSONDataConverter


class SimpleObj(object):
    def __init__(self, input):
        self.input = input


class StateObj(object):
    def __init__(self, input1, input2):
        self.input1 = input1
        self.input2 = input2

    def __getstate__(self):
        newdct = copy.copy(self.__dict__)
        del newdct['input2']
        return newdct

    def __setstate__(self, dct):
        self.__dict__ = dct
        self.input2 = 'blah'


class DictSubclass(dict):
    attr = 'dictsubattr'

    def myval(self):
        return self['testval']


class ListSubclass(list):
    attr = 'listsubattr'

    def secondval(self):
        return self[1]

NamedTuple = namedtuple('NamedTuple', 'a b')


class MyWorkflowDefinition(WorkflowDefinition):
    """For testing pickling metaclassed stuff
    """

    @execute('1.0', 10)
    def execute_test(self):
        pass

class MyCustomException(Exception):
    def __init__(self, message, other):
        super(MyCustomException, self).__init__(message)
        self.other = other


@pytest.fixture
def serde():
    return JSONDataConverter()


def dumps_loads(serde, obj):
    return serde.loads(serde.dumps(obj))


def test_tuples(serde):
    assert serde.dumps(tuple()) == '{"__tuple":[]}'
    assert type(dumps_loads(serde, tuple())) == tuple

    nested_tuple = tuple([(1,2), (3, 4)])
    assert dumps_loads(serde, nested_tuple) == nested_tuple

def test_sets(serde):
    assert serde.dumps(set([1,2,3])) == '{"__set":[1,2,3]}'
    assert type(dumps_loads(serde, set())) == set

    nested_set = set([frozenset([1, 2]), frozenset([3, 4])])
    assert dumps_loads(serde, nested_set) == nested_set

def test_frozensets(serde):
    assert serde.dumps(frozenset([1,2,3])) == '{"__frozenset":[1,2,3]}'
    assert type(dumps_loads(serde, frozenset())) == frozenset

    nested_frozenset = frozenset([frozenset([1, 2]), frozenset([3, 4])])
    assert dumps_loads(serde, nested_frozenset) == nested_frozenset

def test_namedtuples(serde):
    named_tuple = NamedTuple(1, 'a')
    nested_ntuple = NamedTuple(1, NamedTuple(2, 'b'))

    assert serde.dumps(named_tuple) == \
        '{"__namedtuple":["test_json_data_converter:NamedTuple",[1,"a"]]}'


    assert type(dumps_loads(serde, named_tuple)) == NamedTuple
    assert dumps_loads(serde, named_tuple) == named_tuple
    assert dumps_loads(serde, nested_ntuple) == nested_ntuple

def test_decimal(serde):
    number = Decimal(1.1)
    assert number == dumps_loads(serde, number)

def test_decimal_inf(serde):
    inf = Decimal('inf')
    assert inf == dumps_loads(serde, inf)

def test_objects(serde):
    assert dumps_loads(serde, SimpleObj('test')).input == 'test'

def test_class(serde):
    assert dumps_loads(serde, SimpleObj) == SimpleObj

def test_string(serde):
    assert dumps_loads(serde, 'test') == 'test'

def test_unicode(serde):
    ustring = six.unichr(40960) + u'abcd' + six.unichr(1972)
    assert dumps_loads(serde, ustring) == ustring

def test_zlib(serde):
    # This test is really about ensuring that binary data isn't corrupted
    data = six.b('compress me')
    compressed = zlib.compress(data)
    assert zlib.decompress(dumps_loads(serde, compressed)) == data

def test_states_objects(serde):
    assert 'present' not in serde.dumps(StateObj('test', 'present'))

    assert dumps_loads(serde, StateObj('test', 'present')).input2 == 'blah'

def test_serialize_self(serde):
    dumps_loads(serde, serde)

def test_dict(serde):
    assert serde.dumps({'spam': 'eggs'}) == '{"spam":"eggs"}'

def test_dict_subclass(serde):
    subdct = DictSubclass()
    subdct['testval'] = 'test'
    subdct.attr = 'testattr'

    result = dumps_loads(serde, subdct)
    assert result.myval() == 'test'
    assert result.attr == 'testattr'
    assert type(result) == DictSubclass

def test_list_subclass(serde):
    sublst = ListSubclass()
    sublst.extend(('testone', 'testtwo'))
    sublst.attr = 'testattr'

    result = dumps_loads(serde, sublst)
    assert result.secondval() == 'testtwo'
    assert result.attr == 'testattr'
    assert type(result) == ListSubclass

def test_nested_subclass(serde):
    subdct = DictSubclass()
    subdct['testval'] = 'test'
    subdct.sublst = ListSubclass()
    subdct.sublst.extend(('testone', 'testtwo'))

    result = dumps_loads(serde, subdct)
    assert result.myval() == 'test'
    assert result.attr == DictSubclass.attr
    assert type(result) == DictSubclass

    assert result.sublst.secondval() == 'testtwo'
    assert result.sublst.attr == ListSubclass.attr
    assert type(result.sublst) == ListSubclass

def test_ordereddict(serde):
    inner_dct = OrderedDict(((3, 'c'), (4, 'd')))
    dct = OrderedDict(((1, 'a'), (2, inner_dct)))

    assert dumps_loads(serde, dct) == dct
    assert serde.dumps(dct) == '{"__ordereddict":[[1,"a"],[2,{"__ordereddict":[[3,"c"],[4,"d"]]}]]}'

def test_workflow_definition(serde):
    assert serde.loads(serde.dumps(MyWorkflowDefinition)) == MyWorkflowDefinition

def test_exceptions(serde):
    e = MyCustomException(u'some error message', 'someparam')
    r = dumps_loads(serde, e)

    assert str(r) == str(e)
    assert r.other == e.other

def test_unimportable_exception(serde):
    raw = '{"__obj":["unknown_module:MyUnknownException",{"other":"someparam"}],"__exc":[["some error message"],"some error message"]}'
    r = serde.loads(raw)
    assert isinstance(r, ImportError)
    assert 'unknown_module.MyUnknownException: some error message' == r.message
    assert 'someparam' == r.other

def test_unimportable_object(serde):
    raw = '{"__obj":["unknown_module:MyUnknownObject",{"other":"someparam"}]}'
    with pytest.raises(ImportError):
        serde.loads(raw)
