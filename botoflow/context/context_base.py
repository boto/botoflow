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

import threading
import logging

log = logging.getLogger(__name__)

DEBUG = False


class ContextBase(object):

    thread_local = threading.local()

    # Python has no good support for class properties
    @classmethod
    def get_context(cls):
        context = cls.thread_local.flow_current_context
        if DEBUG:
            log.debug("Current context: %s", context)
        return context

    @classmethod
    def set_context(cls, context):
        if DEBUG:
            log.debug("Setting context: %s", context)
        cls.thread_local.flow_current_context = context
