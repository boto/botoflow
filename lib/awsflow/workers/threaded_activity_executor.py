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
import traceback
import logging

from .threaded_executor import ThreadedExecutor

log = logging.getLogger(__name__)


class ThreadedActivityExecutor (ThreadedExecutor):
    """This is an executor for :py:class:`~.ActivityWorker` that uses threads to parallelize
    the activity work.

    Because of the GIL in CPython, it is recomended to use this worker only on
    Jython or IronPython.
    """

    def start(self, pollers=1, workers=1):
        """Start the worker. This method does not block.

        :param int pollers: Count of poller threads to use. Must be equal or
            less than the `workers` attribute.
        :param int workers: Count of worker threads to use.
        """
        if pollers < 1:
            raise ValueError("poller_threads count must be greater than 0")
        if workers < 1:
            raise ValueError("worker_threads count must be greater than 0")
        if pollers > workers:
            raise ValueError("poller_thread must be less or equal to "
                             "worker_threads")

        super(ThreadedActivityExecutor, self).start()

        # we use this semaphore to ensure we have at most poller_tasks running
        poller_semaphore = threading.Semaphore(pollers)

        def run_poller_worker(self):
            self._thread_queue.get()
            thread = threading.current_thread()
            log.debug("Poller/worker %s started", thread.name)
            try:
                while not self._worker_shutdown:
                    work_callable = None
                    with poller_semaphore:

                        while work_callable is None:
                            # make sure that after we wake up we're still
                            # relevant
                            if self._worker_shutdown:
                                return
                            work_callable = self._worker.poll_for_activities()
                    work_callable()

            except Exception as err:
                _, _, tb = sys.exc_info()
                tb_list = traceback.extract_tb(tb)
                handler = self._worker.unhandled_exception_handler
                handler(err, tb_list)
            finally:
                log.debug("Poller/worker %s terminating", thread.name)
                self._thread_queue.task_done()

        for i in range(workers):
            self._thread_queue.put(i)
            thread = threading.Thread(target=run_poller_worker, args=(self,))
            thread.daemon = True
            thread.name = "%r Thread-%d" % (self, i)
            thread.start()
