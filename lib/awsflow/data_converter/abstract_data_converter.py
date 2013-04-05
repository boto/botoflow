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

import abc


class AbstractDataConverter(object):
    """
    Subclasses of this data converter are used by the framework to
    serialize/deserialize method parameters that need to be sent over the wire.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def dumps(self, obj):
        """
        Should return serialized string data
        """
        raise NotImplementedError

    @abc.abstractmethod
    def loads(self, data):
        """
        Should return deserialized string data
        """
        raise NotImplementedError
