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
import multiprocessing
import multiprocessing.managers
import signal
import logging

import six


log = logging.getLogger(__name__)


def _manager_initializer():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class _ProcessManager(object):
    def __init__(self):
        self._process_manager = None

    def process_manager(self):
        if self._process_manager is None:
            self._process_manager = multiprocessing.managers.SyncManager()
            self._process_manager.start(_manager_initializer)
        return self._process_manager

process_manager = _ProcessManager().process_manager


class MultiprocessingExecutor(object):
    """A base for all multiprocessing executors"""

    def __init__(self, worker):
        self._worker = worker

    def start(self):
        """Start the worker. This method does not block."""
        log.debug("Starting worker %s", self)

        # use this simple queue (and .empty) to check if we should shut down
        self._worker_shutdown = self._process_manager().Queue()
        # to track the active processes
        self._process_queue = self._process_manager().JoinableQueue()
        self._running = True

    def stop(self):
        """Stops the worker processes.
        To wait for all the processes to terminate, call:

        .. code-block:: python

            worker.start()
            time.sleep(360)
            worker.stop()
            worker.join()  # will block

        """
        log.debug("Stopping worker %s", self)
        if not self.is_running:
            return False

        self._worker_shutdown.put(1)

    def join(self):
        """Will wait till all the processes are terminated
        """
        try:
            self._process_queue.join()
        except KeyboardInterrupt:
            six.print_("\nTerminating, please wait...", file=sys.stderr)
            self._process_queue.join()
        self._running = False

    @property
    def initializer(self):
        """If set, the initializer function will be called after the subprocess
        is started with the worker object as the first argument.

        You can use this to, for example, set the process name suffix, to
        distinguish between activity and workflow workers (when starting them
        from the same process):

        .. code-block:: python

            from setproctitle import getproctitle, setproctitle

            def set_worker_title(worker):
                name = getproctitle()
                if isinstance(worker, WorkflowWorker):
                    setproctitle(name + ' (WorkflowWorker)')
                elif isinstance(worker, ActivityWorker):
                    setproctitle(name + ' (ActivityWorker)')

            worker.initializer = set_worker_title
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

    @classmethod
    def _process_manager(cls):
        return process_manager()


