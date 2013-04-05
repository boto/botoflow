# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Exceptions from the SWF service
"""


class SWFResponseError(Exception):
    """Base exception for SWF Errors"""
    pass


class DomainDeprecatedError(SWFResponseError):
    """Returned when the specified domain has been deprecated.
    """
    pass


class DomainAlreadyExistsError(SWFResponseError):
    """Returned if the specified domain already exists. You will get this fault
    even if the existing domain is in deprecated status.
    """
    pass


class DefaultUndefinedError(SWFResponseError):
    """Constructs a new DefaultUndefinedException with the specified error
    message.
    """
    pass


class LimitExceededError(SWFResponseError):
    """Returned by any operation if a system imposed limitation has been
    reached.

    To address this fault you should either clean up unused resources or
    increase the limit by contacting AWS.
    """
    pass


class WorkflowExecutionAlreadyStartedError(SWFResponseError):
    """Returned by StartWorkflowExecution when an open execution with the same
    workflowId is already running in the specified domain.
    """


class TypeDeprecatedError(SWFResponseError):
    """Returned when the specified activity or workflow type was already
    deprecated.
    """
    pass


class TypeAlreadyExistsError(SWFResponseError):
    """Returned if the type already exists in the specified domain.

    You will get this fault even if the existing type is in deprecated
    status. You can specify another version if the intent is to create a new
    distinct version of the type.
    """
    pass


class OperationNotPermittedError(SWFResponseError):
    """Returned when the requester does not have the required permissions to
    perform the requested operation.
    """
    pass


class UnknownResourceError(SWFResponseError):
    """Returned when the named resource cannot be found with in the scope of
    this operation (region or domain).

    This could happen if the named resource was never created or is no longer
    available for this operation.
    """
    pass


# SWF __type/fault string to awsflow exception mapping
_swf_fault_exception = {
    'com.amazonaws.swf.base.model#DomainDeprecatedFault': DomainDeprecatedError,
    'com.amazonaws.swf.base.model#DomainAlreadyExistsFault': DomainAlreadyExistsError,
    'com.amazonaws.swf.base.model#DefaultUndefinedFault': DefaultUndefinedError,
    'com.amazonaws.swf.base.model#LimitExceededFault': LimitExceededError,
    'com.amazonaws.swf.base.model#WorkflowExecutionAlreadyStartedFault': WorkflowExecutionAlreadyStartedError,
    'com.amazonaws.swf.base.model#TypeDeprecatedFault': TypeDeprecatedError,
    'com.amazonaws.swf.base.model#TypeAlreadyExistsFault': TypeAlreadyExistsError,
    'com.amazonaws.swf.base.model#OperationNotPermittedFault': OperationNotPermittedError,
    'com.amazonaws.swf.base.model#UnknownResourceFault': UnknownResourceError
}

