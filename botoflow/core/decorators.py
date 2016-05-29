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

import functools
import inspect
import traceback
import logging

from .async_task import AsyncTask
from .async_task_context import AsyncTaskContext
from .async_context import get_async_context
from .future import Future
from .base_future import Return

log = logging.getLogger(__name__)

# change this to enable a ton of debug printing
DEBUG = False


def _task(daemon=False):

    def __task(func):
        def inner_task(*args, **kwargs):
            task = AsyncTask(func, args, kwargs, daemon)
            task.context.except_func = inner_task.except_func
            task.context.finally_func = inner_task.finally_func
            task.context.set_stack(traceback.extract_stack())
            task.execute()

        inner_task.except_func = None
        inner_task.finally_func = None

        def _do_except_wrapper(except_func):
            inner_task.except_func = except_func
            return inner_task

        def _do_finally_wrapper(finally_func):
            inner_task.finally_func = finally_func
            return inner_task

        inner_task.do_except = _do_except_wrapper
        inner_task.do_finally = _do_finally_wrapper

        return inner_task
    return __task

task = _task(daemon=False)
daemon_task = _task(daemon=True)


class AsyncDecorator(object):

    def __init__(self, func, daemon):
        self.future = None
        self.func = [func]
        self.daemon = daemon
        self._except_func = None
        self._finally_func = None
        self.result = None
        self.exception = None

        functools.update_wrapper(self, func)

    def _progress_function(self, future, *args, **kwargs):
        # in case anyone uses raise Return in non-coroutine
        try:
            future.set_result(self.func[0](*args, **kwargs))
        except Return as retval:
            future.set_result(retval.value)

    def _progress_except(self, future, except_func, err):
        if DEBUG:
            log.debug("Progressing except %r %r",
                      except_func, future)

        if except_func is not None:
            future.set_result(except_func[0](err))
            return
        future.set_exception(err)

    def __get__(self, instance, cls):
        """
        Follow the descriptor protocol to allow @async methods in objects
        """
        if instance is None:
            return self
        else:
            func = functools.partial(self, instance)
            functools.update_wrapper(func, self)
            return func

    def __call__(self, *args, **kwargs):
        future = Future()
        context = AsyncTaskContext(self.daemon, get_async_context(),
                                   name=self.func[0].__name__)
        future.context = context
        if inspect.isgeneratorfunction(self.func[0]):
            # wrap in extra context that catches coroutine errors

            coroutine = self.func[0](*args, **kwargs)
            atask = AsyncTask(future._progress_coroutine,
                              (coroutine,), context=context,
                              name=self.func[0].__name__)
            func = functools.partial(future._progress_coroutine_except,
                                     coroutine)
            func.__name__ = future._progress_coroutine_except.__name__
            context.except_func = func

        else:
            func = functools.partial(self._progress_function, future)
            func.__name__ = self._progress_function.__name__
            atask = AsyncTask(func, args, kwargs, context=context,
                              name=self.func[0].__name__)
            func = functools.partial(self._progress_except, future,
                                     self._except_func)
            func.__name__ = self._progress_except.__name__
            context.except_func = func

        atask.context.set_stack(traceback.extract_stack())
        atask.execute()

        future.set_running_or_notify_cancel()
        return future


def _async(daemon=False):

    # This is a specially constructed decorator that can accept act with or
    # without parentheses the same: @async == @async()

    def __async(func=None):
        """

        :type func: __builtin__.function or __builtin__.NoneType
        :return:
        :rtype: awsflow.core.future.Future
        """
        def ___async(func):
            return AsyncDecorator(func, daemon)

        if func is None:
            return ___async
        else:
            return ___async(func)
    return __async

# shortcuts
async = _async(daemon=False)
async_daemon = _async(daemon=True)
