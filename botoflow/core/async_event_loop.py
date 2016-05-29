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
import weakref

from collections import deque

from .async_root_task_context import AsyncRootTaskContext
from .async_context import set_async_context

log = logging.getLogger(__name__)

# change this to enable a ton of debug printing
DEBUG = False

class AsyncEventLoop(object):
    """
    TODO Document
    """

    def __init__(self):
        self.tasks = deque()
        self.root_context = AsyncRootTaskContext(weakref.proxy(self))

    def __enter__(self):
        set_async_context(self.root_context)
        return self.root_context

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, err, tb):
        set_async_context(None)

    def execute(self, task):
        if DEBUG:
            log.debug("Adding task: %s", task)
        self.tasks.append(task)

    def execute_now(self, task):
        if DEBUG:
            log.debug("Prepending task: %s", task)
        self.tasks.appendleft(task)

    def execute_all_tasks(self):
        while self.execute_queued_task():
            pass

    def execute_queued_task(self):
        if DEBUG:
            log.debug("Task queue: %s", self.tasks)
        try:
            task = self.tasks.popleft()
            if not task.done:
                if DEBUG:
                    log.debug("Running task: %s", task)
                task.run()
        except IndexError:  # no more tasks
            return False
        return True
