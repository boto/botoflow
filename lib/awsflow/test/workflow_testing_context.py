from awsflow.core import AsyncEventLoop

from awsflow.context import ContextBase


class WorkflowTestingContext(ContextBase):

    def __init__(self):
        self._event_loop = AsyncEventLoop()

    def __enter__(self):
        self._context = self.get_context()
        self.set_context(self)
        self._event_loop.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._event_loop.execute_all_tasks()
        self._event_loop.__exit__(exc_type, exc_val, exc_tb)
