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
import logging

from weakref import WeakSet

from .async_context import get_async_context, set_async_context
from .exceptions import CancellationError

from .utils import split_stack, log_task_context

# change this to enable a ton of debug printing
DEBUG = False

log = logging.getLogger(__name__)


def AsyncTask(func, *args, **kwargs):
    # avoid looping imports
    from .async_task import AsyncTask
    return AsyncTask(func, *args, **kwargs)


class AbstractAsyncTaskContext(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __enter__(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def __exit__(self, exc_type, err, tb):
        raise NotImplementedError()

    @abc.abstractmethod
    def cancel(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def add_child(self, child):
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_child(self, child):
        raise NotImplementedError()

    @abc.abstractmethod
    def schedule_task(self, task):
        raise NotImplementedError()

    @abc.abstractmethod
    def handle_exception(self, error):
        raise NotImplementedError()


class AsyncTaskContext(AbstractAsyncTaskContext):

    def __init__(self, daemon=False, parent=None, name=None):
        self._setup()

        self.daemon = daemon
        self.parent = parent
        self.name = name
        self.eventloop = parent.eventloop

        self.parent.add_child(self)

    def _setup(self):
        self.children = WeakSet()
        self.daemon_children = WeakSet()
        self.exception = None
        self.tb_list = None
        self.stack_list = None
        self.except_func = None
        self.finally_func = None

    def add_child(self, child):
        if DEBUG:
            log.debug("Adding child:%r ", child)

        if child.daemon:
            self.daemon_children.add(child)
        else:
            self.children.add(child)

    def remove_child(self, child):
        if DEBUG:
            log.debug("Removing child %r %r", child, self)
            log.debug("--------------------BEFORE REMOVE---------------------")
            log_task_context(self, log)
            log.debug("------------------------------------------------------")

        if child.daemon:
            self.daemon_children.remove(child)
        else:
            self.children.remove(child)
            if self.daemon_children and not self.children:
                self.parent.cancel()

        if not self.children and not self.daemon_children:
            if self.exception is not None and self.except_func is not None:
                self._execute_except(self.exception)
            else:
                self._execute_finally()

    def handle_exception(self, exception, tb_list=None):
        if DEBUG:
            log.debug("Handling exception %r %r", self, exception)

        if self.exception is None \
           or not isinstance(exception, CancellationError):
            self.exception = exception
            self.tb_list = tb_list

        for child in self.children.union(self.daemon_children):
            child.cancel()

    def cancel(self):
        if DEBUG:
            log.debug("Cancelling %r", self)

        self.handle_exception(CancellationError(), self.tb_list)

    def update_parent(self):
        if DEBUG:
            log.debug("updating parent %r of self %r", self.parent, self)
        if self.parent is None:
            return

        if self.exception is not None:
            self.parent.handle_exception(self.exception, self.tb_list)

        self.parent.remove_child(self)
        self.parent = None  # gc

    def schedule_task(self, task, now=False):
        if DEBUG:
            log.debug("Scheduling task %r", task)

        if now:
            self.eventloop.execute_now(task)
        else:
            self.eventloop.execute(task)

    def set_stack(self, stack_list):
        stack_before, stack_after = split_stack(stack_list)
        if self.parent.stack_list is None:
            self.parent.stack_list = stack_before

        self.stack_list = stack_after

    def _execute_except(self, err):
        if DEBUG:
            log.debug("Executing except %r %r", self, err)

        if self.except_func is not None:
            with self:
                except_func = self.except_func
                self.except_func = None
                task = AsyncTask(except_func, (err,), context=self,
                                 name=except_func.__name__)
                task.cancellable = False
                task.daemon = self.daemon
                # execute exceptions asap to have a chance to cancel any
                # pending tasks
                task.execute_now()
        else:
            self._execute_finally()

    def _execute_finally(self):
        if DEBUG:
            log.debug("Executiong finally %r with exception %r", self,
                      self.exception)

        if isinstance(self.exception, CancellationError):
            self.exception = None

        if self.finally_func is not None:
            with self:
                task = AsyncTask(self.finally_func, context=self,
                                 name=self.finally_func.__name__)
                task.daemon = self.daemon
                task.cancellable = False
                task.execute()
                self.finally_func = None
        else:
            self.update_parent()

    def __enter__(self):
        self._parent_context = get_async_context()
        set_async_context(self)
        return self

    def __exit__(self, exc_type, err, tb):
        set_async_context(self._parent_context)
        self._parent_context = None  # gc

    def __repr__(self):
        args = []
        if self.name is not None:
            args.append("name=%s" % self.name)
        if self.parent:
            args.append("parent=%s" % self.parent)
        if self.children:
            args.append("children=%d" % len(self.children))
        if self.daemon_children:
            args.append("daemon_children=%d" % len(self.daemon_children))
        if self.except_func:
            args.append("except_func=%r" % self.except_func.__name__)
        if self.finally_func:
            args.append("finally_func=%r" % self.finally_func.__name__)
        if self.exception:
            args.append("exception=%r" % self.exception)

        if args:
            args.insert(0, "")

        return "<%s at %s%s>" % (
            self.__class__.__name__, hex(id(self)), " ".join(args))

    def __str__(self):
        return "<%s at %s>" % (self.__class__.__name__, hex(id(self)))
