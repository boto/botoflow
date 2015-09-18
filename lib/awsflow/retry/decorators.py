from functools import wraps


# noinspection PyUnusedLocal
def transiency_retry(retry_on=None, raise_on=None, max_consecutive_errors=None):
    """
    :param list retry_on: exceptions to retry on
    :param list raise_on: list of exceptions to immediately raise on
    :param int max_consecutive_errors: max consecutive errors to allow
    before raising. None indicates unlimited
    """
    def _transiency_retry(f):
        @wraps(f)
        def __transiency_retry(*args, **kwargs):
            return f(*args, **kwargs)
        return __transiency_retry
    return _transiency_retry
