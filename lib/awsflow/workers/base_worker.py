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

from copy import copy

from botocore.session import Session

from ..core import async_traceback

log = logging.getLogger(__name__)


class BaseWorker(object):
    """
    Base for the Workflow and Activity workers
    """

    def __init__(self, session, aws_region, domain, task_list):
        if not isinstance(session, Session):
            raise TypeError("session must be an instance "
                            "of botocore.session.Session")

        self._identity = None
        self._session = session
        self._aws_region = aws_region

        self._domain = domain
        self._task_list = task_list

        self._fix_endpoint()

    def __repr__(self):
        return "<%s at %s domain=%s task_list=%s>" % (
            self.__class__.__name__, hex(id(self)), self.domain,
            self.task_list)

    def _fix_endpoint(self):
        timeout = self.client._endpoint.timeout

        # newer versions of botocore create a timeout tuple of the form (connect_timeout, read_timeout)
        # older versions just use a scalar int
        if isinstance(timeout, tuple) and timeout[1] < 65:
            self.client._endpoint.timeout = (timeout[0], 65)
        elif not isinstance(timeout, tuple) and timeout < 65:
            self.client._endpoint.timeout = 65

    def __setstate__(self, dct):
        self.__dict__ = dct
        self._fix_endpoint()

    def __getstate__(self):
        dct = copy(self.__dict__)
        try:
            del dct['_client']  # for pickling
        except KeyError:
            pass
        return dct

    @property
    def client(self):
        """Returns the botocore SWF client
        :rtype: botocore.client.swf
        """
        try:
            return self._client
        except AttributeError:  # create a new client
            self._client = self._session.create_client(
                service_name='swf', region_name=self._aws_region)
            return self._client

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
