from abc import abstractmethod


class BaseRetryStrategy(object):

    def __init__(self, retry_on=None, raise_on=None):
        if retry_on is None:
            retry_on = []

        if raise_on is None:
            raise_on = []

        if retry_on and raise_on:
            raise AttributeError("Cannot have retry_on and raise_on in "
                                 "the same retry strategy")
        self._retry_on = tuple(retry_on)
        self._raise_on = tuple(raise_on)

        self._error_count = 0
        self._last_error = None
        self._first_timestamp = None
        self._last_timestamp = None
        self._user_context = None

    @abstractmethod
    def next_delay(self, error, timestamp, user_context=None):
        """Returns the new delay in seconds or raises the error
        :param Exception error: error to retry or raise on
        :param float timestamp: datetime of the error
        :param object user_context: user specific context
        """
        raise NotImplementedError()

    def start_delay(self, timestamp):
        self._first_timestamp = timestamp

    def _set_properties(self, error, timestamp, user_context):
        self._error_count += 1
        self._last_error = error
        self._last_timestamp = timestamp
        self._user_context = user_context

    @property
    def user_context(self):
        """
        :return object: User context if specified or None
        """
        return self._user_context

    @property
    def timestamp(self):
        """
        :return int: returns the last datetime of error
        """
        return self._last_timestamp

    @property
    def first_timestamp(self):
        """
        :return float: datetime of the first error
        """
        return self._first_timestamp

    @property
    def error(self):
        """
        :return Exception: returns the last error captured
        """
        return self._last_error

    @property
    def raise_on(self):
        """List of exceptions on which we should not retry
         :return list: list of exception classes
         """
        return self._raise_on

    @property
    def retry_on(self):
        """List of exceptions to retry on
        :return list: list of exception classes
        """
        return self._retry_on