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

import multiprocessing
import multiprocessing.queues
import signal
import logging

import dill

from ..core import async_traceback

from .multiprocessing_executor import MultiprocessingExecutor

log = logging.getLogger(__name__)


class MultiprocessingWorkflowExecutor(MultiprocessingExecutor):
    """This is a multiprocessing workflow executor, suitable for handling lots of
    workflow decisions in parallel on CPython. 
    """

    def start(self, pollers=1):
        """Start the worker.

        :param int pollers: Poller/worker count to start. Because the expected
            lifetime of the decider is short (should be seconds at most), we
            don't need a separate worker queue.

        Example of starting and terminating the worker:

        .. code-block:: python

            worker.start(pollers=10)
            time.sleep(360)
            worker.stop()
            worker.join()  # will block

        """
        if pollers < 1:
            raise ValueError("pollers count must be greater than 0")

        super(MultiprocessingWorkflowExecutor, self).start()

        start_condition = self._process_manager().Condition()

        def run_decider(executor):
            executor._process_queue.get()
            # ignore any SIGINT, so it looks closer to threading
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            process = multiprocessing.current_process()
            log.debug("Poller/decider %s started", process.name)

            while executor._worker_shutdown.empty():
                with start_condition:
                    start_condition.notify_all()
                try:
                    executor._worker.run_once()

                except Exception as err:
                    tb_list = async_traceback.extract_tb()
                    handler = executor._worker.unhandled_exception_handler
                    handler(err, tb_list)

        def run_decider_with_exc(executor_pickle):
            executor = dill.loads(executor_pickle)
            initializer = self.initializer
            initializer(executor)
            try:
                run_decider(executor)
            except Exception as err:
                tb_list = async_traceback.extract_tb()
                handler = executor._worker.unhandled_exception_handler
                handler(err, tb_list)
            finally:
                process = multiprocessing.current_process()
                log.debug("Poller/decider %s terminating", process.name)
                executor._process_queue.task_done()

        for i in range(pollers):
            with start_condition:
                self._process_queue.put(i)
                process = multiprocessing.Process(target=run_decider_with_exc,
                                                  args=(dill.dumps(self),))
                process.daemon = True
                process.name = "%r Process-%d" % (self, i)
                process.start()
                # wait for the process to "ready" before starting next one
                # or returning
                start_condition.wait()
