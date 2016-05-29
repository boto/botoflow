from  .. import swf_exceptions


class SWFOp(object):
    """Callable"""

    def __init__(self, endpoint, op):
        self.endpoint = endpoint
        self.op = op

    def __call__(self, **kwargs):
        response, response_data = self.op.call(self.endpoint, **kwargs)
        if response.ok:
            return response_data

        exception = swf_exceptions.SWFResponseError

        if 'Errors' in response_data:
            _type = response_data['Errors'][0]['Type']
            if _type in swf_exceptions._swf_fault_exception:
                exception = swf_exceptions._swf_fault_exception[_type]

        if exception == swf_exceptions.SWFResponseError:
            error = exception(response_data.get('message'),
                              'No error provided by SWF: {0}'
                              .format(response_data))
        else:
            error = exception(response_data.get('message'),
                              response_data)
        raise error  # exception from SWF Service


