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

import six

from .exceptions import CancelledError


# change this to enable a ton of debug printing
DEBUG = False

# BaseFuture states
PENDING = 'PENDING'
RUNNING = 'RUNNING'
CANCELLED = 'CANCELLED'
FINISHED = 'FINISHED'

log = logging.getLogger(__name__)


class Return(BaseException):
    """
    This mimics the semantics of PEP-380 should be used instead of "return" on
    Pythons that do not implement PEP-380 or do not use 'yield from'
    """
    def __init__(self, *args):
        if len(args) > 0:
            self.value = args[0]
        else:
            self.value = None
        BaseException.__init__(self, *args)


def return_(*args):
    """
    This mimics the semantics of PEP-380 (see Return for more details)
    """
    raise Return(*args)


class BaseFuture(object):
    """
    This class mimics the BaseFuture from PEP-3148. It is not exactly same and
    they are NOT interchangeable.
    """

    def __init__(self):
        self._state = PENDING
        self._result = None
        self._exception = None
        self._traceback = None

        self._tasks = list()

    def set_result(self, result):
        """
        Internal - Set result on the future
        """
        if self.done():
            return

        if DEBUG:
            log.debug("Setting result: %s", result)
        self._result = result
        self._state = FINISHED
        self._execute_tasks()

    def set_exception(self, exception, traceback=None):
        """
        Internal - Set exception on the future
        """
        if self.done():
            return

        if DEBUG:
            log.debug("Setting exception: %s", exception)
        self._state = FINISHED
        self._exception = exception
        self._traceback = traceback
        self._execute_tasks()

    def _execute_tasks(self):
        for task in self._tasks:
            task.execute()

    def running(self):
        """
        Returns True if the task is runnning
        """
        return self._state == RUNNING

    def done(self):
        """
        Returns True if the future finished or cancelled
        """
        return self._state in (CANCELLED, FINISHED)

    def cancelled(self):
        """
        Returns True if the task was cancelled
        """
        return self._state == CANCELLED

    def exception(self):
        """
        Returns the exception if available, or a ValueError if a result is
        not yet set
        """
        if self._state == CANCELLED:
            raise CancelledError()
        elif self._state == FINISHED:
            return self._exception
        else:
            raise ValueError("Exception was not yet set")

    def traceback(self):
        if self._state == CANCELLED:
            raise CancelledError()
        elif self._state == FINISHED:
            return self._traceback
        else:
            raise ValueError("Exception is not yet set")

    def cancel(self):
        """
        Tries to cancel the future if possible

        :returns: True if the future was cancelled
        """
        if self._state == FINISHED:
            return False
        elif self._state == CANCELLED:
            return True

        self._state = CANCELLED
        self._execute_tasks()
        return True

    def _get_result(self):
        if self._exception is not None:
            six.reraise(self._exception.__class__, self._exception, self._traceback)
        return self._result

    def result(self):
        """
        Return the result

        :raises CancelledError: If the future was cancelled.
        :raises Exception: Any exception raised from the call will be raised.
        :raises ValueError: if a result was not yet set
        """
        if self._state == CANCELLED:
            raise CancelledError()

        elif self._state == FINISHED:
            return self._get_result()
        else:
            raise ValueError("Result is not yet set")

    def __repr__(self):
        if self._state == FINISHED:
            if self._exception:
                return "<%s at %s state=%s raised %s>" % (
                    self.__class__.__name__, hex(id(self)),
                    self._state.lower(), repr(self._exception))
            else:
                return "<%s at %s state=%s returned %s>" % (
                    self.__class__.__name__, hex(id(self)),
                    self._state.lower(), self._result)

        return "<%s at %s state=%s>" % (
            self.__class__.__name__, hex(id(self)), self._state.lower())

    def add_task(self, task):
        if not self.done():
            self._tasks.append(task)
            return
        task.execute()

    # does not really notify, but we're keeping the name like in the PEP
    def set_running_or_notify_cancel(self):
        """
        Internal - used by Executor/Manager
        """
        if self._state == CANCELLED:
            return False

        elif self._state == PENDING:
            self._state = RUNNING
            return True

        log.critical("%s at %s in unexpected state: %s",
                     self.__class__.__name__, hex(id(self)),
                     self._state)
        raise RuntimeError("%s at %s in unexpected state: %s" % (
            self.__class__.__name__, hex(id(self)), self._state))

    @classmethod
    def with_result(cls, result):
        future = cls()
        future.set_result(result)
        return future

    @classmethod
    def with_exception(cls, exception, traceback=None):
        future = cls()
        future.set_exception(exception, traceback)
        return future

    @classmethod
    def with_cancel(cls):
        future = cls()
        future.cancel()
        return future
