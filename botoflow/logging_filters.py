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

import logging

import botoflow


class AWSFlowFilter(logging.Filter):
    """You can use this filter with Python's `logging` module to filter out
    botoflow logs that are being replayed by the decider.

    For example::

        import logging
        from botoflow.logging_filters import AWSFlowFilter

        logging.basicConfig(level=logging.DEBUG,
            format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')

        logging.getLogger('botoflow').addFilter(AWSFlowFilter())

    """

    def __init__(self, name='', filter_replaying=True):
        super(AWSFlowFilter, self).__init__(name)
        self._filter_replaying = filter_replaying

    def filter(self, record):
        try:
            if self._filter_replaying and botoflow.get_context().replaying:
                return 0
        except AttributeError:
            pass
        return 1
