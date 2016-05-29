import json
import copy
import zlib
import six
import datetime

from decimal import Decimal
from collections import namedtuple, OrderedDict

import pytest

from botoflow import WorkflowDefinition, execute
from botoflow.data_converter import JSONDataConverter


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


named_tuple = NamedTuple(1, 'a')

identity_object_parameters = [
    (tuple(), '__tuple'),
    (tuple([(1,2), (3, 4)]), '__tuple'),
    (set([1,2,3]), '__set'),
    (set([frozenset([1, 2]), frozenset([3, 4])]), '__set'),
    (frozenset([1,2,3]), '__frozenset'),
    (frozenset([frozenset([1, 2]), frozenset([3, 4])]), '__frozenset'),
    (NamedTuple(1, 'b'), '__namedtuple'),
    (NamedTuple(1, NamedTuple(2, 'b')), '__namedtuple'),
    (Decimal(1.1), '__decimal'),
    (Decimal('inf'), '__decimal'),
    (SimpleObj, '__class'),
    ('test', None),
    (six.unichr(40960) + u'abcd' + six.unichr(1972), None),
    (WorkflowDefinition, None),
    (OrderedDict(((3, 'c'), (4, 'd'))), '__ordereddict'),
    (OrderedDict(((1, 'a'), (2, OrderedDict(((3, 'c'), (4, 'd')))))), '__ordereddict'),
    (datetime.datetime.utcnow(), '__datetime'),
    (datetime.timedelta(days=2, seconds=3, microseconds=4, milliseconds=5, minutes=6, hours=7, weeks=8), '__timedelta')
]

@pytest.mark.parametrize('obj', [p[0] for p in identity_object_parameters])
def test_json_conversion_identity(serde, obj):
    assert dumps_loads(serde, obj) == obj, "Dumped: {}".format(serde.dumps(obj))


@pytest.mark.parametrize('obj, root_key', [(p[0], p[1]) for p in identity_object_parameters if p[1] is not None])
def test_json_dump_type_key(serde, obj, root_key):
    assert list(json.loads(serde.dumps(obj)).keys())[0] == root_key


def test_objects(serde):
    assert dumps_loads(serde, SimpleObj('test')).input == 'test'


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
