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

"""
This module simulates some functions from the original traceback module
specifically for printing tracebacks. It tidies up the traceback, hiding the
underlying asynchronous framework, so the tracebacks are much more readable.
"""

import sys
import traceback
from .async_context import get_async_context
from .utils import get_context_with_traceback


def print_exc(limit=None, file=None):
    """
    Print exception information and up to limit stack trace entries to file
    """
    if file is None:
        file = sys.stderr

    for line in format_exc(limit):
        file.write(line)


def format_exc(limit=None, exception=None, tb_list=None):
    """
    This is like print_exc(limit) but returns a string instead of printing to a
    file.
    """
    result = ["Traceback (most recent call last):\n"]
    if exception is None:
        exception = get_context_with_traceback(get_async_context()).exception

    if tb_list is None:
        tb_list = extract_tb(limit)

    if tb_list:
        result.extend(traceback.format_list(tb_list))
        result.extend(traceback.format_exception_only(exception.__class__,
                                                      exception))
        return result
    else:
        return None


def extract_tb(limit=None):
    """
    Return list of up to limit pre-processed entries from traceback.

    This is useful for alternate formatting of stack traces.  If
    'limit' is omitted or None, all entries are extracted.  A
    pre-processed stack trace entry is a quadruple (filename, line
    number, function name, text) representing the information that is
    usually printed for a stack trace.  The text is a string with
    leading and trailing whitespace stripped; if the source is not
    available it is None.
    """
    prev_tb = None
    try:
        prev_tb = sys.exc_info()[2]
    except AttributeError:
        pass

    try:
        context = get_context_with_traceback(get_async_context())
    except Exception:
        context = None

    if context is not None:
        tb_list = context.tb_list
        if tb_list is not None:
            if limit is not None:
                return tb_list[-limit:]
            return tb_list
    else:
        return traceback.extract_tb(prev_tb)
