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

import os
import socket
import threading
import logging

from botocore.endpoint import Endpoint

from ..core import async_traceback
from .swf_op_callable import SWFOp

log = logging.getLogger(__name__)


class BaseWorker(object):
    """
    Base for the Workflow and Activity workers
    """

    def __init__(self, endpoint, domain, task_list):
        if not isinstance(endpoint, Endpoint):
            raise TypeError("endpoint must be an instance "
                            "of botocore.endpoint.Endpoint")

        self._identity = None
        self._endpoint = endpoint

        self._domain = domain
        self._task_list = task_list

    def __repr__(self):
        return "<%s at %s domain=%s task_list=%s>" % (
            self.__class__.__name__, hex(id(self)), self.domain,
            self.task_list)

    @property
    def endpoint(self):
        """Returns the botocore SWF Endpoint"""
        return self._endpoint

    @property
    def domain(self):
        """Returns the worker's domain"""
        return self._domain

    @property
    def task_list(self):
        """Returns the task list"""
        return self._task_list

    @property
    def unhandled_exception_handler(self):
        """Returns the current unhandled exception handler.

        Handler notified about poll request and other unexpected failures. The
        default implementation logs the failures using ERROR level.
        """
        return self._unhandled_exception_handler

    @unhandled_exception_handler.setter
    def unhandled_exception_handler(self, func):
        self._unhandled_exception_handler = func

    @property
    def identity(self):
        """Returns the worker's worker's identity

        This value ends up stored in the identity field of the corresponding
        Start history event. Default is "pid":"host".
        """
        if self._identity is None:
            self._identity = "%d:%s" % (os.getpid(), socket.gethostname())
        return self._identity

    @identity.setter
    def identity(self, value):
        self._identity = value

    def run(self):
        """Should be implemented by the worker

        :raises: NotImplementedError
        """
        raise NotImplementedError()

    def run_once(self):
        """Should be implemented by the worker

        :raises: NotImplementedError
        """
        raise NotImplementedError()

    @staticmethod
    def _unhandled_exception_handler(exc, tb_list):
        """Handler notified about poll request and other unexpected failures.

        This default implementation logs the failures using ERROR level.
        """
        thread_name = threading.current_thread().name
        tb_str = async_traceback.format_exc(None, exc, tb_list)
        log.error("Unhandled exception raised in thread %s:\n%s",
                  thread_name, "\n".join(tb_str))

    def _build_swf_op(self, op_name):
        op = self._endpoint.service.get_operation(op_name)
        return SWFOp(self._endpoint, op)
