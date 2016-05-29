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

import threading
import logging

from ..core import async_traceback

from .threaded_executor import ThreadedExecutor

log = logging.getLogger(__name__)


class ThreadedWorkflowExecutor (ThreadedExecutor):
    """This is a threaded workflow executor. 

    It will execute a :py:class:`~WorkflowWorker` in multiple threads.

    As in the case with the :py:class:`~.ThreadedActivityWorker` it is not
    recomended to use it on CPython because of the GIL unless the poller/worker
    count is low (less than 5).
    """

    def start(self, pollers=1):
        """Start the worker.

        :param int pollers: Poller/worker count to start. Because the expected
            lifetime of the decider is short (should be seconds at most), we
            don't need a separate worker queue.

        Example of starting and terminating the worker:

        .. code-block:: python

            worker.start(pollers=2)
            time.sleep(360)
            worker.stop()
            worker.join()  # will block

        """
        if pollers < 1:
            raise ValueError("poller_threads count must be greater than 0")

        super(ThreadedWorkflowExecutor, self).start()

        start_condition = threading.Condition()

        def run_decider(self):
            self._thread_queue.get()
            thread = threading.current_thread()
            log.debug("Poller/decider %s started", thread.name)
            initializer = self.initializer
            initializer(self)

            try:
                while not self._worker_shutdown:
                    with start_condition:
                        start_condition.notifyAll()
                    self._worker.run_once()
            except Exception as err:
                tb_list = async_traceback.extract_tb()
                handler = self._worker.unhandled_exception_handler
                handler(err, tb_list)
            finally:
                log.debug("Poller/decider %s terminating", thread.name)
                self._thread_queue.task_done()

        for i in range(pollers):
            with start_condition:
                self._thread_queue.put(i)
                thread = threading.Thread(target=run_decider, args=(self,))
                thread.daemon = True
                thread.name = "%r Thread-%d" % (self, i)
                thread.start()
                # wait for the thread to "ready" before starting next one
                # or returning
                start_condition.wait()
