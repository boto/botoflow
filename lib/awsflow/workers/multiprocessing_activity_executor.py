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
import traceback
import multiprocessing
import signal
import logging

import dill

from .multiprocessing_executor import MultiprocessingExecutor

log = logging.getLogger(__name__)


class MultiprocessingActivityExecutor(MultiprocessingExecutor):
    """This is an executor for :py:class:`~.ActivityWorker` that uses multiple processes to
    parallelize the activity work.

    """

    def start(self, pollers=1, workers=1):
        """Start the worker. This method does not block.

        :param int pollers: Count of poller processes to use. Must be equal or
            less than the `workers` attribute.
        :param int workers: Count of worker processes to use.
        """
        if pollers < 1:
            raise ValueError("pollers count must be greater than 0")
        if workers < 1:
            raise ValueError("workers count must be greater than 0")
        if pollers > workers:
            raise ValueError("pollers must be less or equal to "
                             "workers")

        super(MultiprocessingActivityExecutor, self).start()

        # we use this semaphore to ensure we have at most poller_tasks running
        poller_semaphore = self._process_manager().Semaphore(pollers)

        def run_poller_worker_with_exc(executor_pickle):
            executor = dill.loads(executor_pickle)
            try:
                executor._process_queue.get()
                # ignore any SIGINT, so it looks closer to threading
                signal.signal(signal.SIGINT, signal.SIG_IGN)
                run_poller_worker(executor)
            except Exception as err:
                _, _, tb = sys.exc_info()
                tb_list = traceback.extract_tb(tb)
                handler = executor._worker.unhandled_exception_handler
                handler(err, tb_list)
            finally:
                process = multiprocessing.current_process()
                log.debug("Poller/executor %s terminating", process.name)
                executor._process_queue.task_done()

        def run_poller_worker(executor):
            process = multiprocessing.current_process()
            log.debug("Poller/executor %s started", process.name)
            initializer = self.initializer
            initializer(executor)
            while executor._worker_shutdown.empty():
                work_callable = None
                with poller_semaphore:

                    while work_callable is None:
                        # make sure that after we wake up we're still relevant
                        if not executor._worker_shutdown.empty():
                            return
                        try:
                            work_callable = executor._worker.poll_for_activities()
                        except Exception as err:
                            _, _, tb = sys.exc_info()
                            tb_list = traceback.extract_tb(tb)
                            handler = executor._worker.unhandled_exception_handler
                            handler(err, tb_list)

                try:
                    work_callable()
                except Exception as err:
                    _, _, tb = sys.exc_info()
                    tb_list = traceback.extract_tb(tb)
                    handler = executor._worker.unhandled_exception_handler
                    handler(err, tb_list)

        for i in range(workers):
            process = multiprocessing.Process(
                target=run_poller_worker_with_exc, args=(dill.dumps(self),))
            self._process_queue.put(i)
            process.daemon = True
            process.name = "%r Process-%d" % (self, i)
            process.start()
