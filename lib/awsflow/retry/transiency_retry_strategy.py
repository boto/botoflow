from .base_retry_strategy import BaseRetryStrategy


class TransiencyRetryStrategy(BaseRetryStrategy):

    def __init__(self, retry_on=None, raise_on=None, max_consecutive_errors=None):
        """
        :param list retry_on: exceptions to retry on
        :param list raise_on: list of exceptions to immediately raise on
        :param int max_consecutive_errors: max consecutive errors to allow
           before raising. None indicates unlimited
        """
        if raise_on is None:
            raise_on = []
        if retry_on is None:
            retry_on = []

        super(TransiencyRetryStrategy, self).__init__(retry_on, raise_on)

        self.consecutive_errors = 0

        if max_consecutive_errors is not None and max_consecutive_errors < 1:
            raise AttributeError("max_consecutive_errors must be None or greater than 0")
        self.max_consecutive_errors = max_consecutive_errors

    def next_delay(self, error, timestamp, user_context=None):
        last_error = self.error
        self._set_properties(error, timestamp, user_context)

        if (not self.retry_on or isinstance(error, self.retry_on)) \
           and (not self.raise_on or not isinstance(error, self.raise_on)):

            if type(last_error) != type(error):
                self.consecutive_errors = 1
                return 0

            self.consecutive_errors += 1
            if self.max_consecutive_errors is None \
               or self.consecutive_errors <= self.max_consecutive_errors:
                return 0  # retry immediately

        # if we reach this point, means the retry strategy failed and we need
        # to raise the exception
        raise error