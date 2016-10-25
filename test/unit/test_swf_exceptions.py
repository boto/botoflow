import pytest

from botocore.exceptions import ClientError

from botoflow import swf_exceptions


def test_swf_exception_wrapper_with_no_exception():
    with swf_exceptions.swf_exception_wrapper():
        pass # nothing went wrong


def test_swf_exception_wrapper_with_unknown_exception():
    with pytest.raises(swf_exceptions.SWFResponseError) as exc:
        with swf_exceptions.swf_exception_wrapper():
            raise ClientError({'Error': {'Code': 'SomethingBrokeError', 'Message': 'Oops'}}, 'foobar')

    assert str(exc.value) == "Oops"


@pytest.mark.parametrize('code_key,expected_exception', [
    ('DomainDeprecatedFault', swf_exceptions.DomainDeprecatedError),
    ('DomainAlreadyExistsFault', swf_exceptions.DomainAlreadyExistsError),
    ('DefaultUndefinedFault', swf_exceptions.DefaultUndefinedError),
    ('LimitExceededFault', swf_exceptions.LimitExceededError),
    ('WorkflowExecutionAlreadyStartedFault', swf_exceptions.WorkflowExecutionAlreadyStartedError),
    ('TypeDeprecatedFault', swf_exceptions.TypeDeprecatedError),
    ('TypeAlreadyExistsFault', swf_exceptions.TypeAlreadyExistsError),
    ('OperationNotPermittedFault', swf_exceptions.OperationNotPermittedError),
    ('UnknownResourceFault', swf_exceptions.UnknownResourceError),
    ('SWFResponseError', swf_exceptions.SWFResponseError),
    ('ThrottlingException', swf_exceptions.ThrottlingException),
    ('ValidationException', swf_exceptions.ValidationException),
    ('UnrecognizedClientException', swf_exceptions.UnrecognizedClientException),
    ('InternalFailure', swf_exceptions.InternalFailureError),
])
def test_swf_exception_wrapper_with_known_exception(code_key, expected_exception):
    with pytest.raises(expected_exception) as exc:
        with swf_exceptions.swf_exception_wrapper():
            raise ClientError({'Error': {'Code': code_key, 'Message': 'Oops'}}, 'foobar')

    assert str(exc.value) == "Oops"


def test_swf_exception_wrapper_with_no_error_response_details():
    with pytest.raises(swf_exceptions.SWFResponseError) as exc:
        with swf_exceptions.swf_exception_wrapper():
            raise ClientError({'Error': {'Code': None, 'Message': None}}, 'foobar')


def test_swf_exception_wrapper_with_malformed_code_key():
    with pytest.raises(swf_exceptions.SWFResponseError) as exc:
        with swf_exceptions.swf_exception_wrapper():
            raise ClientError({'Error': {'Code': (123, "this is not a key"), 'Message': None}}, 'foobar')


def test_swf_exception_wrapper_with_non_client_error():
    exception = RuntimeError("Not handled by swf_exception_wrapper")
    with pytest.raises(RuntimeError) as exc:
        with swf_exceptions.swf_exception_wrapper():
            raise exception

    assert exc.value == exception
