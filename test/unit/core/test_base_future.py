import re
import unittest
import logging

import awsflow.core.base_future as futuremod
from awsflow.core.async_event_loop import AsyncEventLoop
from awsflow.core.async_task import AsyncTask
from awsflow.logging_filters import AWSFlowFilter

logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger('awsflow').addFilter(AWSFlowFilter())


class TestFuture(unittest.TestCase):

    def task_callback(self, *args, **kwargs):
        self.callback_info.append((args, kwargs))

    def setUp(self):
        self.callback_info = list()
        self.ev = AsyncEventLoop()

    def test_constructor(self):
        with self.ev:
            future = futuremod.BaseFuture()
            self.assertEqual(future._state, futuremod.PENDING)
            self.assertEqual(future._result, None)
            self.assertEqual(future._tasks, [])

    def test_states(self):
        with self.ev:
            future = futuremod.BaseFuture()
            self.assertEqual(future._state, futuremod.PENDING)
            self.assertFalse(future.done())
            self.assertFalse(future.running())
            self.assertFalse(future.cancelled())

            future.set_running_or_notify_cancel()
            self.assertEqual(future._state, futuremod.RUNNING)
            self.assertFalse(future.done())
            self.assertTrue(future.running())
            self.assertFalse(future.cancelled())

            future.set_result(None)
            self.assertEqual(future._state, futuremod.FINISHED)
            self.assertTrue(future.done())
            self.assertFalse(future.running())
            self.assertFalse(future.cancelled())

            future = futuremod.BaseFuture()
            future.cancel()
            self.assertEqual(future._state, futuremod.CANCELLED)
            self.assertTrue(future.done())
            self.assertFalse(future.running())
            self.assertTrue(future.cancelled())

    def test_set_result(self):
        with self.ev:
            future = futuremod.BaseFuture()
            future.set_result('TestResult')
        self.assertEqual(future._result, 'TestResult')
        self.assertEqual(future.result(), 'TestResult')

    def test_set_exception(self):
        future = futuremod.BaseFuture()
        err = RuntimeError('TestError')
        future.set_exception(err)

        self.assertEqual(future._exception, err)
        self.assertEqual(future._traceback, None)
        self.assertTrue(future.done())
        self.assertEqual(future.exception(), err)
        self.assertRaises(RuntimeError, future.result)


    def test_set_result_then_task(self):
        with self.ev:
            future = futuremod.BaseFuture()
            task = AsyncTask(self.task_callback, (future,))
            future.set_result(12)
            future.add_task(task)

        self.ev.execute_all_tasks()

        self.assertEqual(len(self.callback_info), 1)
        self.assertEqual(self.callback_info[0][0], (future,))
        self.assertTrue(future.done())

    def test_task_then_set_result(self):
        with self.ev:
            future = futuremod.BaseFuture()
            task = AsyncTask(self.task_callback, (future,))
            future.add_task(task)
            self.assertFalse(self.callback_info)
            future.set_result(12)

        self.ev.execute_all_tasks()
        self.assertEqual(len(self.callback_info), 1)
        self.assertEqual(self.callback_info[0][0], (future,))
        self.assertTrue(future.done())

    def test_set_exception_then_task(self):
        with self.ev:
            future = futuremod.BaseFuture()
            task = AsyncTask(self.task_callback, (future,))
            future.set_exception(RuntimeError('TestError'))
            future.add_task(task)

        self.ev.execute_all_tasks()
        self.assertEqual(len(self.callback_info), 1)
        self.assertEqual(self.callback_info[0][0], (future,))
        self.assertTrue(future.done())

    def test_task_then_set_exception(self):
        with self.ev:
            future = futuremod.BaseFuture()
            task = AsyncTask(self.task_callback, (future,))
            future.add_task(task)
            self.assertFalse(self.callback_info)
            future.set_exception(RuntimeError('TestError'))

        self.ev.execute_all_tasks()
        self.assertEqual(len(self.callback_info), 1)
        self.assertEqual(self.callback_info[0][0], (future,))
        self.assertTrue(future.done())

    def test_cancel(self):
        future = futuremod.BaseFuture()
        self.assertTrue(future.cancel())
        # the second time should just return as already cancelled
        self.assertTrue(future.cancel())

        future = futuremod.BaseFuture()
        future.set_result(True)
        self.assertFalse(future.cancel())

    def test_repr(self):
        future = futuremod.BaseFuture()

        self.assertTrue(re.match(r'<BaseFuture at .* state=pending>',
                                 repr(future)))

        future.set_result('TestResult')
        self.assertTrue(re.match(
            r'<BaseFuture at .* state=finished returned TestResult', repr(future)))


    def test_return_is_base_exception(self):
        self.assertTrue(isinstance(futuremod.Return(), BaseException))

if __name__ == '__main__':
    unittest.main()
