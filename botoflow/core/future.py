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
import threading
import logging

from weakref import WeakSet

from .async_task import AsyncTask
from .base_future import BaseFuture, Return

try:  # PY3k
    import collections.abc
    # noinspection PyUnresolvedReferences
    Iterable = collections.abc.Iterable
except ImportError:
    import collections
    Iterable = collections.Iterable


log = logging.getLogger(__name__)

# change this to enable a ton of debug printing
DEBUG = False


class Future(BaseFuture):

    thread_local = threading.local()

    def __init__(self):
        super(Future, self).__init__()
        self._next = None  # stack
        self.context = None

    @classmethod
    def track_coroutine(cls, coroutine):
        # for tracking of all open coroutines in the current thread
        if not hasattr(cls.thread_local, 'coroutines'):
            cls.thread_local.coroutines = WeakSet()

        cls.thread_local.coroutines.add(coroutine)

    @classmethod
    def untrack_coroutine(cls, coroutine):
        if not hasattr(cls.thread_local, 'coroutines'):
            return

        try:
            coroutine.close()
        except Exception:
            pass
        try:
            cls.thread_local.coroutines.remove(coroutine)
        except KeyError:
            pass

    @classmethod
    def untrack_all_coroutines(cls):
        if not hasattr(cls.thread_local, 'coroutines'):
            return

        # manually gc the open coroutines
        for coro in cls.thread_local.coroutines:
            try:
                coro.close()
            # ignore any exceptions arising from the closing, nothing we can do
            except Exception:
                pass

        cls.thread_local.coroutines = WeakSet()

    def cancel(self):
        if not self.done():
            self.context.cancel()

    def __or__(self, other):
        if isinstance(other, BaseFuture):
            return AnyFuture(self, other)
        elif isinstance(other, AnyFuture):
            other.add_future(self)
            return other

        raise TypeError("unsupported operand type(s) for "
                        "|: '%s' and '%s'" % (self.__class__.__name__,
                                              other.__class__.__name__))

    def __and__(self, other):
        if isinstance(other, BaseFuture):
            return AllFuture(self, other)
        elif isinstance(other, AllFuture):
            other.add_future(self)
            return other

        raise TypeError("unsupported operand type(s) for "
                        "&: '%s' and '%s'" % (self.__class__.__name__,
                                              other.__class__.__name__))

    def _progress_coroutine(self, coroutine, value=None, exception=None,
                            traceback=None):
        if DEBUG:
            log.debug('Future._progress_coroutine: %s, %s, %s, %s',
                      coroutine, value, exception, traceback)

        self.track_coroutine(coroutine)
        try:
            with self.context:
                if exception is not None:
                    covalue = coroutine.throw(exception.__class__, exception,
                                              traceback)

                else:
                    covalue = coroutine.send(value)

        except Return as err:
            self.set_result(err.value)
            self.context = None  # gc
            self.untrack_coroutine(coroutine)
            return
        except StopIteration:
            self.set_result(None)
            self.context = None  # gc
            self.untrack_coroutine(coroutine)
            return
        except GeneratorExit:
            self.set_result(None)
            self.context = None  # gc
            self.untrack_coroutine(coroutine)
        except Exception as err:
            _, _, tb = sys.exc_info()
            self.set_exception(err, tb)
            self.context = None  # gc
            self.untrack_coroutine(coroutine)
            return

        else:
            with self.context:
                if isinstance(covalue, BaseFuture):
                    self._next = covalue
                    task = AsyncTask(self._on_future_completion,
                                     (covalue, coroutine),
                                     name=self._on_future_completion)
                    task.cancellable = False
                    covalue.add_task(task)
                    return
                elif isinstance(covalue, Iterable):
                    all_future = AllFuture(*covalue)
                    task = AsyncTask(self._on_future_completion,
                                     (all_future, coroutine),
                                     name=self._on_future_completion)
                    task.cancellable = False
                    all_future.add_task(task)
                    return

            log.critical("%s at %s in unexpected state: %s - %s",
                         self.__class__.__name__, hex(id(self)), covalue,
                         coroutine)
            raise RuntimeError("%s at %s in unexpected state: %s - %s" % (
                self.__class__.__name__, hex(id(self)), covalue, coroutine))

    def _on_future_completion(self, future, coroutine):
        if DEBUG:
            log.debug("Future._on_completion: %s, %s, %s",
                      self, future, coroutine)
        if self._next == future:
            self._next = None

        exception = future.exception()
        if exception is not None:
            self._progress_coroutine(coroutine, exception=exception,
                                     traceback=future.traceback())
        else:
            val = future.result()
            self._progress_coroutine(coroutine, val)

    def _progress_coroutine_except(self, coroutine, err):
        if DEBUG:
            log.debug("Future._progress_coroutine_except %r", self)

        self.track_coroutine(coroutine)

        with self.context:
            task = AsyncTask(self._progress_coroutine,
                             (coroutine, None, err, None))
            task.execute()
        self.context = None


class AnyFuture(BaseFuture):

    def __init__(self, *futures):
        super(AnyFuture, self).__init__()
        self._futures = futures
        for future in futures:
            self.add_future(future)

        # if an empty list was supplied, immediately set an empty tuple
        # as the result since there is nothing to await and we are
        # returning iterable results
        if not futures:
            self.set_result(tuple())

    def add_future(self, future):
        task = AsyncTask(self._future_callback, (future,),
                         name=self._future_callback.__name__)
        task.cancellable = False
        future.add_task(task)

    def _future_callback(self, future):
        if self.done():
            return

        if future.exception() is not None:
            self.set_exception(future.exception(),
                               future.traceback())
        else:
            self.set_result(future.result())


class AllFuture(AnyFuture):

    def __init__(self, *futures):
        super(AllFuture, self).__init__(*futures)
        
        # if an empty list was supplied, immediately set an empty tuple
        # as the result since there is nothing to await and we are
        # returning iterable results
        if not futures:
            self.set_result(tuple())

    def _future_callback(self, future):
        results = list()
        for _future in self._futures:
            if _future.done():
                if _future.exception():
                    self.set_exception(_future.exception(),
                                       _future.traceback())
                    return
                else:
                    results.append(_future.result())
            else:
                return
        self.set_result(tuple(results))
