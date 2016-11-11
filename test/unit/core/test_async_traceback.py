import pytest
import unittest
import logging
import pytest

import sys
import six

from botoflow.core.async_event_loop import AsyncEventLoop
from botoflow.core.decorators import coroutine, task
from botoflow.core.async_traceback import format_exc, print_exc
from botoflow.logging_filters import BotoflowFilter

logging.basicConfig(level=logging.DEBUG,
                    format='%(filename)s:%(lineno)d (%(funcName)s) - %(message)s')
logging.getLogger('botoflow').addFilter(BotoflowFilter())

pytestmark = pytest.mark.usefixtures('core_debug')


class TestTraceback(unittest.TestCase):

    def setUp(self):
        self.tb_str = None

    @pytest.mark.xfail(sys.version_info >= (3,5,0),
                       reason="Some kind of brokennes on 3.5.0 specifically")
    def test_format(self):
        @task
        def task_func():
            raise RuntimeError("Test")

        @task_func.do_except
        def except_func(err):
            self.tb_str = "".join(format_exc())

        ev = AsyncEventLoop()
        with ev:
            task_func()
        ev.execute_all_tasks()

        self.assertTrue(self.tb_str)
        self.assertEqual(1, self.tb_str.count('---continuation---'))

    @pytest.mark.xfail(sys.version_info >= (3,5,0),
                       reason="Some kind of brokennes on 3.5.0 specifically")
    def test_print(self):
        @task
        def task_func():
            raise RuntimeError("Test")

        @task_func.do_except
        def except_func(err):
            strfile = six.StringIO()
            print_exc(file=strfile)
            self.tb_str = strfile.getvalue()

        ev = AsyncEventLoop()
        with ev:
            task_func()
        ev.execute_all_tasks()

        self.assertTrue(self.tb_str)
        self.assertEqual(1, self.tb_str.count('---continuation---'))

    def test_recursive(self):
        @task
        def task_raises_recursive(count=3):
            if not count:
                raise RuntimeError("Test")
            count -= 1
            task_raises_recursive(count)

        @task
        def task_func():
            task_raises_recursive()

        @task_func.do_except
        def except_func(err):
            self.tb_str = format_exc()

        ev = AsyncEventLoop()
        with ev:
            task_func()
        ev.execute_all_tasks()

        self.assertTrue(self.tb_str)

    @pytest.mark.xfail(sys.version_info >= (3,5,0),
                       reason="Some kind of brokennes on 3.5.0 specifically")
    def test_async(self):
        @coroutine
        def raises():
            raise RuntimeError("TestErr")

        @coroutine
        def main():
            try:
                yield raises()
            except RuntimeError:
                self.tb_str = "".join(format_exc())

        ev = AsyncEventLoop()
        with ev:
            main()
        ev.execute_all_tasks()

        self.assertTrue(self.tb_str)
        self.assertEqual(2, self.tb_str.count('---continuation---'))


if __name__ == '__main__':
    unittest.main()
