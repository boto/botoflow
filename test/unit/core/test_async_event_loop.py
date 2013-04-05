import unittest

from awsflow.core import async_event_loop

class TestEventLoop(unittest.TestCase):

    def test_smoke(self):
        ev = async_event_loop.AsyncEventLoop()
        self.assertFalse(ev.execute_all_tasks())

if __name__ == '__main__':
    unittest.main()
