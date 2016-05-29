# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import base64
import copy
import json

import datetime
from decimal import Decimal

import six

try:
    from collections import OrderedDict
except (ImportError, AttributeError):
    class OrderedDict(object):
        pass

from .abstract_data_converter import AbstractDataConverter

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


class _FlowObjEncoder(json.JSONEncoder):
    """
    This custom JSON encoder tries to RPCify the objects into a special format,
    that can be read by botoflow decoder. In addition to regular new-style
    class based objects, it tries to encode tuples separately from lists so
    that on decode tuples are recreated as tuples (not list).
    """

    def _flowify_obj(self, obj):
        obj_type = type(obj)

        if obj_type == str and six.PY2:
            try:
                obj.decode('utf8')
            except UnicodeDecodeError:
                # If it can't be decoded as Unicode, it's probably binary data,
                # and the JSON Encoder won't cope, so we base64-encode it.
                return {'__base64str': base64.b64encode(obj)}
            return obj
        # python3 binary data
        elif six.PY3 and obj_type == six.binary_type:
            return {'__bin': base64.b64encode(obj).decode('latin-1')}
        elif obj_type == tuple:
            return {'__tuple': [self._flowify_obj(o) for o in obj]}
        elif obj_type == set:
            return {'__set': [self._flowify_obj(o) for o in obj]}
        elif obj_type == frozenset:
            return {'__frozenset': [self._flowify_obj(o) for o in obj]}
        elif obj_type == Decimal:
            return {'__decimal': [self._flowify_obj(o) for o in obj.as_tuple()]}
        elif obj_type == datetime.datetime:
            return {'__datetime': self._flowify_obj(obj.strftime(DATETIME_FORMAT))}
        elif obj_type == datetime.timedelta:
            return {'__timedelta': self._flowify_obj([obj.days, obj.seconds, obj.microseconds])}
        elif obj_type == type:
            clsname = "%s:%s" % (obj.__module__, obj.__name__)
            return {'__class': clsname}

        # handle subclasses of list
        # issubclass(list, list) -> True (!)
        elif issubclass(obj_type, list):
            flow_list = [self._flowify_obj(o) for o in obj]
            if obj_type == list:
                return flow_list
            else:
                flow_list_class = "%s:%s" % (obj_type.__module__,
                                             obj_type.__name__)
                return {
                    '__listclass': [flow_list_class, flow_list],
                    '__dict__': {key: self._flowify_obj(value)
                                 for key, value in six.iteritems(obj.__dict__)}}

        elif obj_type == OrderedDict:
            flowified = []
            for key, val in six.iteritems(obj):
                flowified.append((key, self._flowify_obj(val)))
            return {'__ordereddict': flowified}

        # handle subclasses of dict
        # issubclass(dict, dict) -> True (!)
        elif issubclass(obj_type, dict):
            if obj_type == dict:
                flow_dict = copy.copy(obj)
                for key, val in six.iteritems(flow_dict):
                    flow_dict[key] = self._flowify_obj(val)

                return flow_dict

            else:
                if hasattr(obj, '__getstate__'):
                    flow_dict = obj.__getstate__()
                else:
                    flow_dict = copy.copy(obj)

                for key, val in six.iteritems(flow_dict):
                    flow_dict[key] = self._flowify_obj(val)

                flow_dict_class = "%s:%s" % (obj_type.__module__,
                                             obj_type.__name__)

                return {
                    '__dictclass': [flow_dict_class, flow_dict],
                    '__dict__': {key: self._flowify_obj(value)
                                 for key, value in six.iteritems(obj.__dict__)}}

        # namedtuple (!)
        # http://bugs.python.org/issue7796
        elif isinstance(obj, tuple) \
                and obj_type != tuple and hasattr(obj, '_fields'):

            clsname = "%s:%s" % (obj_type.__module__, obj_type.__name__)
            flowified = [self._flowify_obj(o) for o in obj]
            return {'__namedtuple': [clsname, flowified]}

        return obj

    def encode(self, obj):
        return super(_FlowObjEncoder, self).encode(self._flowify_obj(obj))

    def default(self, obj):
        obj_cls = type(obj)

        if obj_cls in (set, frozenset, type, six.binary_type) or isinstance(obj, tuple):
            return self._flowify_obj(obj)

        if hasattr(obj, '__getstate__'):
            flow_dict = obj.__getstate__()
        else:
            # do not pickle metaclasses
            if isinstance(obj, type):
                clsname = "%s:%s" % (obj.__module__, obj.__name__)
                return {'__class': clsname}
            else:
                flow_dict = copy.copy(obj.__dict__)

        for key, val in six.iteritems(flow_dict):
            flow_dict[key] = self._flowify_obj(val)

        flow_obj_class = "%s:%s" % (obj_cls.__module__,
                                    obj_cls.__name__)

        retval = {'__obj': [flow_obj_class, flow_dict]}

        # extra special love for exceptions :(
        if issubclass(obj_cls, BaseException):
            if six.PY3:
                retval['__exc'] = [obj.args, None]
            else:
                retval['__exc'] = [obj.args, obj.message]

        return retval


def _flow_obj_decoder(dct):
    if '__tuple' in dct:
        return tuple(dct['__tuple'])
    elif '__set' in dct:
        return set(dct['__set'])
    elif '__frozenset' in dct:
        return frozenset(dct['__frozenset'])
    elif '__ordereddict' in dct:
        return OrderedDict(dct['__ordereddict'])
    elif '__base64str' in dct:
        return base64.b64decode(dct['__base64str'])
    elif '__bin' in dct:
        return base64.b64decode(dct['__bin'])
    elif '__decimal' in dct:
        return Decimal(dct['__decimal'])
    elif '__datetime' in dct:
        return datetime.datetime.strptime(dct['__datetime'], DATETIME_FORMAT)
    elif '__timedelta' in dct:
        return datetime.timedelta(*dct['__timedelta'])

    module_name, attr_name = None, None

    if '__obj' in dct:
        module_name, attr_name = str(dct['__obj'][0]).split(':', 1)
    elif '__class' in dct:
        module_name, attr_name = str(dct['__class']).split(':', 1)
    elif '__listclass' in dct:
        module_name, attr_name = str(dct['__listclass'][0]).split(':', 1)
    elif '__dictclass' in dct:
        module_name, attr_name = str(dct['__dictclass'][0]).split(':', 1)
    elif '__namedtuple' in dct:
        module_name, attr_name = str(dct['__namedtuple'][0]).split(':', 1)
    else:
        return dct

    try:
        # attempt to import the module and cls of the object specified
        module = __import__(module_name, globals(), locals(), [attr_name], 0)
        cls = getattr(module, attr_name)

    # if an import error is raised
    except ImportError:
        # try and rescue objects with exception information by bundling them
        # into a ImportError
        if '__exc' in dct:
            cls = ImportError
            # prepend the original exception class to the exception message
            dct['__exc'][1] = "%s.%s: %s" % (module_name, attr_name, dct['__exc'][1])
        else:
            # otherwise there isn't much we can do
            raise

    # don't need an object at all
    if '__class' in dct:
        return cls
    elif '__namedtuple' in dct:
        return cls(*dct['__namedtuple'][1])

    obj = cls.__new__(cls)  # recreate an instance without calling __init__

    if hasattr(obj, '__setstate__'):
        obj.__setstate__(dct['__obj'][1])

    elif '__listclass' in dct:
        obj.extend(dct['__listclass'][1])
        obj.__dict__ = dct['__dict__']
    elif '__dictclass' in dct:
        obj.update(dct['__dictclass'][1])
        obj.__dict__ = dct['__dict__']
    else:
        obj.__dict__ = dct['__obj'][1]

    if '__exc' in dct:
        obj.args = dct['__exc'][0]
        if dct['__exc'][1] is not None:
            obj.message = dct['__exc'][1]

    return obj


class JSONDataConverter(AbstractDataConverter):
    """This is a custom JSON serializer. It tries to serialize objects from
    new-style classes, moreover, it tries to follow the pickle's __setstate__
    and __getstate__ methods for customizing serialization and deserialization.

    Because it is not pickle, it will *NOT* work in all the situations that
    Pickle does and vice-versa. For example, bytes have to be encoded manually
    in Python2 (as there's no easy way to know what to do).

    We try to support most of the base types for ease of use, but the
    serialized data might be a bit 'chatty' and bump into SWF data limitations.

    WARNING: this data converter does not support old-style classes.
    """

    def __init__(self):
        self._setup()

    def _setup(self):
        self._encoder = _FlowObjEncoder(indent=None, separators=(',', ':'))
        self._decoder = json.JSONDecoder(object_hook=_flow_obj_decoder)

    def dumps(self, obj):
        """Serialize to object to JSON str format

        :param obj: Any serializable object (including classes)
        :type obj: object
        :returns: JSON string
        :rtype: str
        """
        return self._encoder.encode(obj)

    def loads(self, data):
        """Deserialize a JSON string into Python object(s)

        :param data: Input streang
        :type data: str, unicode
        :returns: deserialized object
        :rtype: object
        """
        return self._decoder.decode(data)

    def __getstate__(self):
        # make sure we handle (de)serialization of serializers well :)
        newdct = copy.copy(self.__dict__)
        del newdct['_encoder']
        del newdct['_decoder']
        return newdct

    def __setstate__(self, dct):
        self.__dict__ = dct
        self._setup()
