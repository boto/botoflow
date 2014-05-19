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

from six.moves import queue
import logging

log = logging.getLogger(__name__)


class ThreadedExecutor(object):
    """This will execute a worker using multiple threads."""

    def __init__(self, worker):
        self._worker = worker

    def start(self):
        """Start the worker. This method does not block."""
        log.debug("Starting worker %s", self)

        self._worker_shutdown = False  # when True will indicate shutdown
        self._thread_queue = queue.Queue()  # to track the active threads
        self._running = True

    def stop(self):
        """Stops the worker threads.
        To wait for all the threads to terminate, call:

        .. code-block:: python

            worker.start()
            time.sleep(360)
            worker.stop()
            worker.join()  # will block

        """
        log.debug("Stopping worker %s", self)
        if not self.is_running:
            return False

        self._worker_shutdown = True

    def join(self):
        """Will wait till all the threads are terminated
        """
        self._thread_queue.join()
        self._running = False

    @property
    def initializer(self):
        """If set, the initializer function will be called after the thread
        is started with the worker object as the first argument.
        """
        try:
            return self.__initializer
        except AttributeError:
            return lambda obj: None

    @initializer.setter
    def initializer(self, func):
        self.__initializer = func

    @property
    def is_running(self):
        """Returns True if the worker is running"""
        if hasattr(self, '_running'):
            return self._running
        return False
