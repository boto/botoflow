# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

__all__ = ('RetryError', 'Retrying')

import sys
from math import ceil

import retrying

from . import workflow_time
from .exceptions import ActivityTaskFailedError
from .core import async, return_


RetryError = retrying.RetryError


def retry_on_exception(*args):
    """Takes a list of exceptions to retry on and retries, will automatically unpack ActivityTaskFailedError exceptions

    :param *args: exception classes to retry on
    :type *args: Iterable
    :rtype: function
    :return: callable function to be used with retry_on_exception in Retrying
    """
    def _retry_exceptions(exception):

        if isinstance(exception, ActivityTaskFailedError):
            exception = exception.cause

        if isinstance(exception, args):
            return True

        return False

    return _retry_exceptions


class Retrying(retrying.Retrying):

    def __init__(self, stop_max_attempt_number=None, stop_max_delay=None, wait_fixed=None,
                 wait_random_min=None, wait_random_max=None, wait_incrementing_start=None,
                 wait_incrementing_increment=None, wait_exponential_multiplier=None, wait_exponential_max=None,
                 retry_on_exception=None, retry_on_result=None, wrap_exception=False, stop_func=None, wait_func=None):

        super(Retrying, self).__init__(stop=stop_func, wait=wait_func, stop_max_attempt_number=stop_max_attempt_number,
                                       stop_max_delay=stop_max_delay, wait_fixed=wait_fixed, wait_random_min=wait_random_min,
                                       wait_random_max=wait_random_max, wait_incrementing_start=wait_incrementing_start,
                                       wait_incrementing_increment=wait_incrementing_increment,
                                       wait_exponential_multiplier=wait_exponential_multiplier,
                                       wait_exponential_max=wait_exponential_max, retry_on_exception=retry_on_exception,
                                       retry_on_result=retry_on_result,
                                       wrap_exception=wrap_exception,
                                       stop_func=stop_func, wait_func=wait_func)

        # unfortunately retrying uses ms everywhere and we are using seconds (as in floats 0.5 is valid)
        # to remain consistent with botoflow, we fix all the times before passing the to retrying

        self._stop_max_attempt_number = 3 if stop_max_attempt_number is None else stop_max_attempt_number
        self._stop_max_delay = 1000 if stop_max_delay is None else int(stop_max_delay * 1000)
        self._wait_fixed = 1000 if wait_fixed is None else int(wait_fixed * 1000)
        self._wait_random_min = 0 if wait_random_min is None else int(wait_random_min * 1000)
        self._wait_random_max = 1000 if wait_random_max is None else int(wait_random_max * 1000)
        self._wait_incrementing_start = 0 if wait_incrementing_start is None else int(wait_incrementing_start * 1000)
        self._wait_incrementing_increment = 1000 if wait_incrementing_increment is None else int(wait_incrementing_increment * 1000)
        self._wait_exponential_multiplier = 1 if wait_exponential_multiplier is None else int(wait_exponential_multiplier * 1000)
        self._wait_exponential_max = retrying.MAX_WAIT if wait_exponential_max is None else int(wait_exponential_max * 1000)

    @async
    def call(self, fn, *args, **kwargs):
        start_time = int(round(workflow_time.time() * 1000))
        attempt_number = 1
        while True:
            try:
                val = yield fn(*args, **kwargs)
                attempt = retrying.Attempt(val, attempt_number, False)
            except Exception:
                val = sys.exc_info()
                attempt = retrying.Attempt(val, attempt_number, True)

            if not self.should_reject(attempt):
                return_(attempt.get(self._wrap_exception))

            delay_since_first_attempt_ms = int(round(workflow_time.time() * 1000)) - start_time
            if self.stop(attempt_number, delay_since_first_attempt_ms):
                if not self._wrap_exception and attempt.has_exception:
                    # get() on an attempt with an exception should cause it to be raised, but raise just in case
                    raise attempt.get()
                else:
                    raise RetryError(attempt)
            else:
                # use ceil since SWF timer resolution is in seconds
                sleep = self.wait(attempt_number, delay_since_first_attempt_ms)
                yield workflow_time.sleep(ceil(sleep / 1000.0))

            attempt_number += 1