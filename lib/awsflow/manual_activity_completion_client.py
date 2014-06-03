from awsflow.workers.swf_op_callable import SWFOp
from awsflow.data_converter import JSONDataConverter
from awsflow.core.exceptions import CancellationError


class ManualActivityCompletionClient(object):
    def __init__(self, swf_endpoint, data_converter=JSONDataConverter()):
        _op = swf_endpoint.service.get_operation('RespondActivityTaskCompleted')
        self._complete_activity_op = SWFOp(swf_endpoint, _op)

        _op = swf_endpoint.service.get_operation('RespondActivityTaskFailed')
        self._failed_activity_op = SWFOp(swf_endpoint, _op)

        _op = swf_endpoint.service.get_operation('RespondActivityTaskCanceled')
        self._cancelled_activity_op = SWFOp(swf_endpoint, _op)

        _op = swf_endpoint.service.get_operation('RecordActivityTaskHeartbeat')
        self._record_activity_heartbeat_op = SWFOp(swf_endpoint, _op)
 
        self.data_converter = data_converter

    def complete(self, result, task_token):
        result = self.data_converter.dumps(result)
        self._complete_activity_op(result=result, task_token=task_token)

    def fail(self, details, task_token, reason=''):
        details = self.data_converter.dumps(details)
        self._failed_activity_op(details=details, reason=reason, task_token=task_token)

    def cancel(self, details, task_token):
        self._cancelled_activity_op(details=details, task_token=task_token)

    def record_heartbeat(self, details, task_token):
        response_data = self._record_activity_hearbeat_op(details=details, task_token=task_token)
        if response_data['cancelRequested']:
            raise CancellationError()
