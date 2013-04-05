import unittest
import logging

from awsflow.core.async_event_loop import AsyncEventLoop
from awsflow.core.async_task import AsyncTask
from awsflow.core.decorators import task
from awsflow.core.base_future import BaseFuture
from awsflow.core.exceptions import CancellationError
from awsflow.logging_filters import AWSFlowFilter

logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger('awsflow').addFilter(AWSFlowFilter())


class TestTask(unittest.TestCase):

    def setUp(self):
        self.counter = 0
        self.except_called = False
        self.finally_called = False

    @task
    def count(self):
        self.counter += 1

    def test_task(self):
        ev = AsyncEventLoop()
        with ev:
            self.count()
        ev.execute_all_tasks()
        self.assertEqual(1, self.counter)

    def test_two_tasks(self):
        ev = AsyncEventLoop()
        with ev:
            self.count()
            self.count()
        ev.execute_all_tasks()
        self.assertEqual(2, self.counter)

    def test_recursive(self):
        ev = AsyncEventLoop()

        @task
        def recursive(ct=10):
            self.counter += 1
            if ct == 1:
                return
            ct -=1
            recursive(ct)

        with ev:
            recursive()
        ev.execute_all_tasks()
        self.assertEqual(10, self.counter)

    def test_exceptions(self):
        @task
        def task_func():
            raise RuntimeError("Test")

        @task_func.do_except
        def except_func(err):
            self.except_called = True

        @task_func.do_finally
        def finally_func():
            self.finally_called = True

        ev = AsyncEventLoop()
        with ev:
            task_func()
        ev.execute_all_tasks()

        self.assertTrue(self.except_called)
        self.assertTrue(self.finally_called)

    def test_task_finally(self):
        @task
        def recursive(ct=1):
            self.counter += 1
            if ct == 1:
                return
            ct -=1
            recursive(ct)

        @task
        def recursive_caller():
            recursive()

        @recursive_caller.do_finally
        def finally_func():
            self.finally_called = True

        ev = AsyncEventLoop()
        with ev:
            recursive_caller()
        ev.execute_all_tasks()

        self.assertTrue(self.finally_called)

    def test_tasks_finally(self):
        @task
        def recursive(ct=10):
            if ct == 1:
                return
            ct -=1
            recursive(ct)

        @recursive.do_finally
        def recursive():
            self.counter += 1


        ev = AsyncEventLoop()
        with ev:
            recursive()
        ev.execute_all_tasks()

        self.assertEqual(10, self.counter)

    def test_finally_with_subtask(self):
        @task
        def count():
            self.counter += 1

        @count.do_finally
        def count():
            self.counter += 1

            @task
            def count():
                self.counter +=1
            @count.do_finally
            def count():
                self.counter += 1

            count()

        ev = AsyncEventLoop()
        with ev:
            count()
        ev.execute_all_tasks()

        self.assertEqual(4, self.counter)

    def test_finally_with_err_subtask(self):
        @task
        def count():
            self.counter += 1

        @count.do_finally
        def count():
            self.counter += 1

            @task
            def err():
                raise RuntimeError("Test")
            @err.do_finally
            def err():
                self.counter += 1

            err()

        ev = AsyncEventLoop()
        with ev:
            count()
        ev.execute_all_tasks()

        self.assertEqual(3, self.counter)

    def test_except_with_err_subtask(self):
        @task
        def count():
            self.counter += 1

        @count.do_finally
        def count():
            self.counter += 1

            @task
            def err():
                raise RuntimeError("Test")
            @err.do_except
            def err(err):
                if isinstance(err, RuntimeError):
                    self.counter += 1

            err()

        ev = AsyncEventLoop()
        with ev:
            count()
        ev.execute_all_tasks()

        self.assertEqual(3, self.counter)

    def test_finally_reraise_subtask(self):
        @task
        def count():
            self.counter += 1

        @count.do_finally
        def count():
            self.counter += 1

            @task
            def err():
                raise RuntimeError("Test")

            @err.do_except
            def err(err):
                if isinstance(err, RuntimeError):
                    self.counter += 1
                raise err

            err()

        ev = AsyncEventLoop()
        with ev:
            count()
        ev.execute_all_tasks()

        self.assertEqual(3, self.counter)

    def test_finally_reraise_catch_subtask(self):
        @task
        def count():
            self.counter += 1

        @count.do_finally
        def count():
            self.counter += 1

            @task
            def err():
                raise RuntimeError("Test")

            @err.do_except
            def err(err):
                if isinstance(err, RuntimeError):
                    self.counter += 1
                raise err

            err()

        @task
        def main():
            self.counter +=1
            count()
        @main.do_except
        def main(err):
            self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertEqual(5, self.counter)

    def test_finally_reraise_catch_finally_subtask(self):
        @task
        def count():
            self.counter += 1

        @count.do_finally
        def count():
            self.counter += 1

            @task
            def err():
                raise RuntimeError("Test")

            @err.do_except
            def err(err):
                if isinstance(err, RuntimeError):
                    self.counter += 1
                raise err

            err()

        @task
        def main():
            self.counter +=1
            count()
        @main.do_except
        def main(err):
            self.counter += 1
        @main.do_finally
        def main():
            self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertEqual(6, self.counter)

    def test_except_and_finally_raise(self):
        @task
        def raises():
            raise RuntimeError("Error")
        @raises.do_except
        def raises(err):
            raise err
        @raises.do_finally
        def raises():
            raise ValueError("Finally wins")

        @task
        def main():
            raises()
        @main.do_except
        def main(err):
            if isinstance(err, ValueError):
                self.except_called = True
            elif isinstance(err, RuntimeError):
                self.except_called = False

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertTrue(self.except_called)

    def test_cancel_before_except(self):
        @task
        def raises():
            raise RuntimeError("Error")

        @task
        def main():
            raises()
            self.count()
            self.count()

        @main.do_except
        def main(err):
            self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertEqual(1, self.counter)

    def test_cancel_except_finally(self):
        @task
        def raises():
            raise RuntimeError("Error")

        @task
        def other():
            self.counter -= 1
        @other.do_except
        def other(err):
            if isinstance(err, CancellationError):
                self.counter += 1
        @other.do_finally
        def other():
            self.counter += 1

        @task
        def main():
            raises()
            other()

        @main.do_except
        def main(err):
            self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertEqual(3, self.counter)

    def test_future(self):
        future = BaseFuture()
        @task
        def other():
            future.set_result(1)

        @task
        def main():
            other()

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertEqual(1, future.result())

    def test_future_with_task(self):
        future = BaseFuture()

        def count():
            self.counter +=1

        @task
        def other():
            future.set_result(1)

        @task
        def main():
            other()

        ev = AsyncEventLoop()
        with ev:
            future.add_task(AsyncTask(count))
            main()
        ev.execute_all_tasks()

        self.assertEqual(1, future.result())
        self.assertEqual(1, self.counter)

if __name__ == '__main__':
    unittest.main()
