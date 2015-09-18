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

import sys
import traceback
import logging

import six

from .async_context import get_async_context, set_async_context
from .async_task_context import AsyncTaskContext
from .exceptions import CancellationError
from .utils import extract_stacks_from_contexts, filter_framework_frames

log = logging.getLogger(__name__)

# change this to enable a ton of debug printing
DEBUG = False


class AsyncTask(object):
    """
    AsyncTask contains a method with arguments to be run at some time
    """

    def __init__(self, function, args=tuple(), kwargs=None, daemon=False, context=None, name=None):
        """
        :param function: function to call at some time.
        :type function: FunctionType
        :param args: arguments to pass into the function
        :type args: tuple
        :param kwargs: keyword arguments to pass into the function
        :type kwargs: dict
        :param daemon: If True, task is a daemon task
        :type daemon: True/False
        :param context: context to use instead of creating a new
        :type context: AsyncTaskContext

        Example::
          task = Task(sum, (1,2))
          task.run()
        """
        if not kwargs:
            kwargs = {}

        self.cancellable = True
        self.cancelled = False
        self.exception = None
        self.done = False
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.name = name

        self.daemon = daemon
        if context:
            self.context = context
        else:
            self.context = AsyncTaskContext(self.daemon, get_async_context())

    def __enter__(self):
        set_async_context(self.context)
        return self.context

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, err, tb):
        set_async_context(self.context.parent)

    def _run(self):
        if self.cancelled:
            raise CancellationError()
        self.function(*self.args, **self.kwargs)
        if self.cancelled:
            raise CancellationError()

    def run(self):
        """
        Executes the function. This method does not return values from the
        functionran.
        """
        with self:
            try:
                self._run()
            except Exception as err:
                self.exception = err
                _, _, tb = sys.exc_info()
                stacks = extract_stacks_from_contexts(self.context)
                tb_list = list()

                for stack in stacks:
                    tb_list.extend(stack)
                    tb_list.append((None, 1, 'flow.core',
                                    '---continuation---'))

                tb_list.extend(
                    filter_framework_frames(traceback.extract_tb(tb)))

                self.context.handle_exception(err, tb_list)
            finally:
                self.context.remove_child(self)
                self.done = True
        self.context = None

    def execute(self):
        self.context.add_child(self)
        self.schedule()

    def execute_now(self):
        self.context.add_child(self)
        self.schedule(now=True)

    def schedule(self, now=False):
        if not self.done or self.exception is None:
            self.context.schedule_task(self, now)

    def cancel(self):
        if DEBUG:
            log.debug("Task canceling: %r", self)

        if self.exception is None and self.cancellable:
            self.cancelled = True

    def __repr__(self):
        func_str = ''
        if self.name is not None:
            func_str = self.name

        args_str = ''
        if hasattr(self.function, 'im_self'):
            if self.function.im_self:
                args_str = 'self'
                if self.args or self.kwargs:
                    args_str = 'self, '

        if self.args:
            args_str += ", ".join((repr(arg) for arg in self.args))

        if self.args and self.kwargs:
            args_str += ', '

        if self.kwargs:
            args_str += ", ".join(('%s=%s' % (key, repr(val))
                                   for key, val in six.iteritems(self.kwargs)))

        return "<%s at %s %s(%s)>" % (self.__class__.__name__, hex(id(self)),
                                      func_str, args_str)
