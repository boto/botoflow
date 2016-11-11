import unittest
import logging
import pytest

from botoflow.core.async_event_loop import AsyncEventLoop
from botoflow.core.decorators import coroutine
from botoflow.core.base_future import BaseFuture, return_
from botoflow.core.future import AllFuture, AnyFuture, Future
from botoflow.core.exceptions import CancellationError
from botoflow.logging_filters import BotoflowFilter

logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger('botoflow').addFilter(BotoflowFilter())

pytestmark = pytest.mark.usefixtures('core_debug')


class TestAsync(unittest.TestCase):
    def setUp(self):
        self.counter = 0
        self.except_called = False
        self.finally_called = False

    def test_simple(self):
        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def count_generator():
            if False: yield
            self.counter += 1

        @coroutine
        def main():
            future = count_generator()
            for i in range(3):
                yield count()
            yield future

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()
        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(4, self.counter)

    def test_simple_async_with_parens(self):
        @coroutine()
        def count():
            self.counter += 1

        @coroutine()
        def count_generator():
            if False: yield
            self.counter += 1

        @coroutine()
        def main():
            future = count_generator()
            for i in range(3):
                yield count()
            yield future

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()
        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(4, self.counter)

    def test_simple_class(self):

        class Main(object):

            def __init__(self):
                self.counter = 0

            @coroutine
            def count(self):
                self.counter += 1

            @coroutine
            def count_generator(self):
                if False: yield
                self.counter += 1

            @coroutine
            def main(self):
                future = self.count_generator()
                for i in range(3):
                    yield self.count()
                yield future

        ev = AsyncEventLoop()
        main = Main()
        with ev:
            future = main.main()

        ev.execute_all_tasks()
        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(4, main.counter)

    def test_external_future(self):
        future = BaseFuture()

        @coroutine
        def main():
            result = yield future
            self.counter += result

        ev = AsyncEventLoop()
        with ev:
            mainf = main()
        ev.execute_all_tasks()
        future.set_result(1)
        ev.execute_all_tasks()

        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(1, self.counter)

    def test_two_futures(self):
        @coroutine
        def returns():
            if False: yield
            return_(1)

        @coroutine
        def count_generator():
            result = yield returns()
            self.counter += result

        @coroutine
        def main():
            yield count_generator()

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()
        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(1, self.counter)

    def test_raise_return(self):
        @coroutine
        def returns():
            return_("result")

        @coroutine
        def main():
            result = yield returns()
            if result == 'result':
                self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            main()

        ev.execute_all_tasks()
        self.assertEqual(1, self.counter)

    def test_implicit_cancel(self):
        @coroutine
        def raises():
            raise RuntimeError()

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def main():
            yield raises()
            yield count()

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertEqual(0, self.counter)

    def test_catch(self):

        @coroutine
        def raises():
            raise RuntimeError("TestErr")

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def echo(inp):
            return inp

        @coroutine
        def main():
            try:
                future = count()
                yield raises()
            except RuntimeError:
                yield count()
                future = echo(1)
                result = yield echo(2)
                if result == 2:
                    self.counter += 1
                result = yield future
                if result == 1:
                    self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertEqual(4, self.counter)

    def test_explicit_cancel(self):
        # FIXME does not work if the futures are of the same type
        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def cancel_me():
            self.counter += 1

        @coroutine
        def main():
            future = cancel_me()
            future.cancel()
            for i in range(3):
                yield count()
            yield future

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()
        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(3, self.counter)

    @pytest.mark.xfail
    def test_async(self):
        # FIXME
        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def raises():
            self.counter += 1
            raise RuntimeError("TestError")

        @coroutine
        def main():
            yield count()
            future = raises()
            other()
            try:
                result = yield future
                self.counter -= 1 # should not happen
            except RuntimeError:
                self.counter += 1

        @coroutine
        def other():
            import pdb; pdb.set_trace()
            assert 'Should not run, should be cancelled'

        @other.do_except
        def othererr(err):
            if isinstance(err, CancellationError):
                assert 'Should call cancel'
                self.counter += 1

        @other.do_finally
        def otherfin():
            self.counter += 1

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()
        self.assertTrue(isinstance(future, BaseFuture))
        self.assertEqual(4, self.counter)

    def test_cancel(self):
        @coroutine
        def raises():
            raise RuntimeError()

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def main():
            fut1 = raises()
            fut2 = count()
            yield fut1

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertEqual(0, self.counter)


class TestAllFuture(unittest.TestCase):

    def setUp(self):
        self.counter = 0

    def test_simple(self):
        future1 = BaseFuture()
        future2 = BaseFuture()

        ev = AsyncEventLoop()
        with ev:
            all_future = AllFuture(future1, future2)
            future2.set_result(2)
            future1.set_result(1)

        ev.execute_all_tasks()
        self.assertEqual(all_future.result(), (1, 2))

    def test_exception(self):
        future1 = BaseFuture()
        future2 = BaseFuture()

        ev = AsyncEventLoop()
        with ev:
            all_future = AllFuture(future1, future2)
            future2.set_result(2)
            future1.set_exception(RuntimeError())

        ev.execute_all_tasks()
        self.assertEqual(type(all_future.exception()), RuntimeError)

    def test_cancel(self):
        @coroutine
        def raises():
            raise RuntimeError()

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def main():
            yield (raises(), count())

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertEqual(0, self.counter)

    def test_cancel_only_all(self):
        """
        Test that only futures "in" All get cancelled if one of them fails
        """
        @coroutine
        def raises():
            raise RuntimeError()

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def main():
            count()
            try:
                yield (raises(), count())
            except RuntimeError:
                yield count()

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertEqual(2, self.counter)

    def test_no_futures_yield_empty_tuple(self):
        @coroutine
        def main():
            results = yield []
            return_(results)

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertFalse(future.result())


class TestAnyFuture(unittest.TestCase):

    def setUp(self):
        self.counter = 0

    def test_simple(self):
        future1 = BaseFuture()
        future2 = BaseFuture()

        ev = AsyncEventLoop()
        with ev:
            any_future = AnyFuture(future1, future2)
            future2.set_result(3)

        ev.execute_all_tasks()
        self.assertEqual(any_future.result(), 3)
        self.assertFalse(future1.done())

    def test_exception(self):
        future1 = BaseFuture()
        future2 = BaseFuture()

        ev = AsyncEventLoop()
        with ev:
            any_future = AnyFuture(future1, future2)
            future1.set_exception(RuntimeError())

        ev.execute_all_tasks()
        self.assertEqual(type(any_future.exception()), RuntimeError)

    def test_cancel(self):
        @coroutine
        def raises():
            raise RuntimeError()

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def main():
            yield AnyFuture(raises(), count())

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertEqual(0, self.counter)


    def test_cancel_only_any(self):
        """
        Test that only futures "in" Any get cancelled if one of them fails
        """
        @coroutine
        def raises():
            raise RuntimeError()

        @coroutine
        def count():
            self.counter += 1

        @coroutine
        def main():
            count()
            try:
                fut1, fut2 = raises(), raises()
                yield AnyFuture(fut1, fut2)
            except RuntimeError:
                yield count()
                try:
                    yield fut1
                except RuntimeError:
                    yield count()

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertEqual(3, self.counter)

    def test_no_futures_yield_empty_tuple(self):
        @coroutine
        def main():
            results = yield AnyFuture()
            return_(results)

        ev = AsyncEventLoop()
        with ev:
            future = main()
        ev.execute_all_tasks()

        self.assertFalse(future.result())


if __name__ == '__main__':
    unittest.main()
