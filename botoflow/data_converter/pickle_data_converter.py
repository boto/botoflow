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

import pickle

from base64 import b64encode, b64decode

from .abstract_data_converter import AbstractDataConverter


class PickleDataConverter(AbstractDataConverter):
    """WARNING: This data converter is NOT recomended as it does not serialize
    exceptions well and can cause various hard to debug issues.

    This is a "pickling" data converter. The data passed around with SWF will
    be in the pickle format. In addition, if the protocol version is not 0,
    the data will be base64 encoded (as any version other than 0 is binary).
    """

    def __init__(self, protocol=0):
        self._protocol = protocol

    def dumps(self, obj):
        if self._protocol == 0:
            return pickle.dumps(obj, 0)
        return b64encode(pickle.dumps(obj, self._protocol))

    def loads(self, data):
        if self._protocol == 0:
            return pickle.loads(data)
        return pickle.loads(b64decode(data))
