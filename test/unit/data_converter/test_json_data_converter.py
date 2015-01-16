import copy
import unittest
import zlib
import six

from collections import namedtuple, OrderedDict

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


class TestJSONDataConverter(unittest.TestCase):

    def setUp(self):
        self.serde = JSONDataConverter()

    def dumps_loads(self, obj):
        return self.serde.loads(self.serde.dumps(obj))

    def test_tuples(self):
        self.assertEqual('{"__tuple":[]}', self.serde.dumps(tuple()))
        self.assertEqual(tuple, type(self.dumps_loads(tuple())))

        nested_tuple = tuple([(1,2), (3, 4)])
        self.assertEqual(nested_tuple, self.dumps_loads(nested_tuple))

    def test_sets(self):
        self.assertEqual('{"__set":[1,2,3]}',
                         self.serde.dumps(set([1,2,3])))
        self.assertEqual(set, type(self.dumps_loads(set())))

        nested_set = set([frozenset([1, 2]), frozenset([3, 4])])
        self.assertEqual(nested_set, self.dumps_loads(nested_set))

    def test_frozensets(self):
        self.assertEqual('{"__frozenset":[1,2,3]}',
                         self.serde.dumps(frozenset([1,2,3])))
        self.assertEqual(frozenset, type(self.dumps_loads(frozenset())))

        nested_frozenset = frozenset([frozenset([1, 2]), frozenset([3, 4])])
        self.assertEqual(nested_frozenset, self.dumps_loads(nested_frozenset))

    def test_namedtuples(self):
        named_tuple = NamedTuple(1, 'a')
        nested_ntuple = NamedTuple(1, NamedTuple(2, 'b'))

        self.assertEqual('{"__namedtuple":["test_json_data_converter:NamedTuple"'
                         ',[1,"a"]]}',
                         self.serde.dumps(named_tuple))

        self.assertEqual(NamedTuple, type(self.dumps_loads(named_tuple)))
        self.assertEqual(named_tuple, self.dumps_loads(named_tuple))
        self.assertEqual(nested_ntuple, self.dumps_loads(nested_ntuple))

    def test_objects(self):

        self.assertEqual('test', self.dumps_loads(SimpleObj('test')).input)

    def test_class(self):

        self.assertEqual(SimpleObj, self.dumps_loads(SimpleObj))

    def test_string(self):
        self.assertEqual('test', self.dumps_loads('test'))

    def test_unicode(self):
        ustring = six.unichr(40960) + u'abcd' + six.unichr(1972)
        self.assertEqual(ustring, self.dumps_loads(ustring))

    def test_zlib(self):
        # This test is really about ensuring that binary data isn't corrupted
        data = six.b('compress me')
        compressed = zlib.compress(data)
        self.assertEqual(data, zlib.decompress(self.dumps_loads(compressed)))

    def test_states_objects(self):
        self.assertFalse('present' in \
                         self.serde.dumps(StateObj('test', 'present')))

        self.assertEqual('blah',
                         self.dumps_loads(StateObj('test', 'present')).input2)

    def test_serialize_self(self):
        self.dumps_loads(self.serde)

    def test_dict(self):
        self.assertEqual('{"spam":"eggs"}', self.serde.dumps({'spam': 'eggs'}))

    def test_dict_subclass(self):
        subdct = DictSubclass()
        subdct['testval'] = 'test'
        subdct.attr = 'testattr'

        result = self.dumps_loads(subdct)
        self.assertEqual('test', result.myval())
        self.assertEqual('testattr', result.attr)
        self.assertEqual(DictSubclass, type(result))

    def test_list_subclass(self):
        sublst = ListSubclass()
        sublst.extend(('testone', 'testtwo'))
        sublst.attr = 'testattr'

        result = self.dumps_loads(sublst)
        self.assertEqual('testtwo', result.secondval())
        self.assertEqual('testattr', result.attr)
        self.assertEqual(ListSubclass, type(result))

    def test_nested_subclass(self):
        subdct = DictSubclass()
        subdct['testval'] = 'test'
        subdct.sublst = ListSubclass()
        subdct.sublst.extend(('testone', 'testtwo'))

        result = self.dumps_loads(subdct)
        self.assertEqual('test', result.myval())
        self.assertEqual(DictSubclass.attr, result.attr)
        self.assertEqual(DictSubclass, type(result))

        self.assertEqual('testtwo', result.sublst.secondval())
        self.assertEqual(ListSubclass.attr, result.sublst.attr)
        self.assertEqual(ListSubclass, type(result.sublst))

    def test_ordereddict(self):
        inner_dct = OrderedDict(((3, 'c'), (4, 'd')))
        dct = OrderedDict(((1, 'a'), (2, inner_dct)))

        self.assertEqual(dct, self.dumps_loads(dct))
        self.assertEqual('{"__ordereddict":[[1,"a"],[2,'
                         '{"__ordereddict":[[3,"c"],[4,"d"]]}]]}',
                         self.serde.dumps(dct))

    def test_workflow_definition(self):
        self.assertEqual(MyWorkflowDefinition,
                         self.serde.loads(
                             self.serde.dumps(MyWorkflowDefinition)))

    def test_exceptions(self):
        e = MyCustomException(u'some error message', 'someparam')
        r = self.dumps_loads(e)

        self.assertEqual(e.message, r.message)
        self.assertEqual(e.other, r.other)

    def test_unimportable_exception(self):
        raw = '{"__obj":["unknown_module:MyUnknownException",{"other":"someparam"}],"__exc":[["some error message"],"some error message"]}'
        r = self.serde.loads(raw)
        self.assertTrue(isinstance(r, ImportError))
        self.assertEqual(r.message, 'unknown_module.MyUnknownException: some error message')
        self.assertEqual(r.other, 'someparam')

    def test_unimportable_object(self):
        raw = '{"__obj":["unknown_module:MyUnknownObject",{"other":"someparam"}]}'
        with self.assertRaises(ImportError):
            self.serde.loads(raw)

if __name__ == '__main__':
    unittest.main()

