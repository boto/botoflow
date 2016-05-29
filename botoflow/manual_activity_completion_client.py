from botoflow.swf_exceptions import swf_exception_wrapper
from botoflow.data_converter import JSONDataConverter
from botoflow.core.exceptions import CancellationError


class ManualActivityCompletionClient(object):
    def __init__(self, swf_client, data_converter=JSONDataConverter()):
        """Helper class to work with manual activities

        :param swf_client: botocore SWF client
        :type swf_client: botocore.clients.Client
        :param data_converter: DataConverter to use for marshaling data
        :type data_converter: botoflow.data_converter.BaseDataConverter
        """
        self._swf_client = swf_client
        self.data_converter = data_converter

    def complete(self, result, task_token):
        result = self.data_converter.dumps(result)
        with swf_exception_wrapper():
            self._swf_client.respond_activity_task_completed(result=result, taskToken=task_token)

    def fail(self, details, task_token, reason=''):
        details = self.data_converter.dumps(details)
        with swf_exception_wrapper():
            self._swf_client.respond_activity_task_failed(details=details, reason=reason, taskToken=task_token)

    def cancel(self, details, task_token):
        with swf_exception_wrapper():
            self._swf_client.respond_activity_task_cancelled(details=details, taskToken=task_token)

    def record_heartbeat(self, details, task_token):
        with swf_exception_wrapper():
            response_data = self._swf_client.record_activity_task_hearbeat(details=details, taskToken=task_token)
        if response_data['cancelRequested']:
            raise CancellationError()
