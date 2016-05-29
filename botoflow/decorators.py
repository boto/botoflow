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

import types

from . import decorator_descriptors
from .activity_retrying import Retrying
from .constants import USE_WORKER_TASK_LIST, CHILD_TERMINATE
from .workflow_types import WorkflowType, ActivityType, SignalType

__all__ = ('workflow', 'activities', 'execute', 'activity', 'manual_activity', 'signal', 'retry_activity')


def _str_or_none(value):
    if value is None:
        return value
    return str(value)


def _set_swf_options(obj, opts_key, options):
    if not hasattr(obj, 'swf_options'):
        obj.swf_options = dict()

    if not opts_key in obj.swf_options:
        obj.swf_options[opts_key] = options
    else:  # union
        obj.swf_options[opts_key] = dict(options.items()
                                         + obj.swf_options[opts_key].items())
    return obj


def workflow(name):
    """Using this decorator you can override the workflow name (class name) to
    use.

    This cam be very useful, when you're writing a newer version of the same
    workflow, but you want to write it in a new class.

    .. code-block:: python

        class ExampleWorkflow(WorkflowDefinition):

            @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
            def example_start(self, a, b):
                pass

        @workflow(name='ExampleWorkflow')
        class ExampleWorkflowV2(ExampleWorkflow):

            @execute(version='2.0', execution_start_to_close_timeout=1*MINUTES)
            def example_start_v2(self, a, b, c):
                pass

    Now you have two classes that handle *ExampleWorkflow*, but with version
    1.0 and version 2.0, which you can start by caling corresponding
    example_start() and example_start_v2()

    :param str name: Specifies the name of the workflow type. If not set, the
        name will be defaulted to the workflow definition class name.

    """
    def _workflow(cls):
        for workflow_type in cls._workflow_types:
            workflow_type._reset_name(name, force=True)
        return cls
    return _workflow


def execute(version,
            execution_start_to_close_timeout,
            task_list=USE_WORKER_TASK_LIST,
            task_start_to_close_timeout=30,  # as in java flow
            child_policy=CHILD_TERMINATE,
            data_converter=None,
            description=None,
            skip_registration=False):
    """Use this decorator indicates the entry point of the workflow.

    The entry point of the workflow can be invoked remotely by your application
    using clients that are generated automatically by the botoflow.

    :param str version: Required version of the workflow type. Maximum length
        is 64 characters.
    :param int execution_start_to_close_timeout: Specifies the
        defaultExecutionStartToCloseTimeout registered with Amazon SWF for the
        workflow type. This specifies the total time a workflow execution of
        this type may take to complete. See the Amazon SWF API reference for
        more details.
    :param str task_list: The default task list for the decision tasks for
        executions of this workflow type. The default can be overridden using
        :py:func`~botoflow.options_overrides.workflow_options` when starting a
        workflow execution. Set to
        :py:data:`~botoflow.constants.USE_WORKER_TASK_LIST` by default. This is
        a special value which indicates that the task list used by the worker,
        which is performing the registration, should be used.
    :param int task_start_to_close_timeout: Specifies the
        defaultTaskStartToCloseTimeout registered with Amazon SWF for the
        workflow type. This specifies the time a single decision task for a
        workflow execution of this type may take to complete. See the Amazon
        SWF API reference for more details. The default is 30 seconds.
    :param str child_policy: Specifies the policy to use for the child
        workflows if an execution of this type is terminated. The default value
        is :py:data:`~botoflow.constants.CHILD_TERMINATE`.
    :param data_converter: Specifies the type of the DataConverter to use for
        serializing/deserializing data when sending requests to and receiving
        results from workflow executions of this workflow type.  Set to `None`
        by default, which indicates that the JsonDataConverter should be used.
    :type data_converter: :py:class:`~awsflow.data_converter.abstract_data_converter.AbstractDataConverter`
    :param description: Textual description of the workflow definition. By
        default will use the docstring of the workflow definition class if
        available. The maximum length is 1024 characters, so a long docstring
        will be truncated to that length.
    :type description: str or None
    :param bool skip_registration: Indicates that the workflow type should not
        be registered with Amazon SWF.
    """
    _workflow_type = WorkflowType(
        version,
        task_list=task_list,
        execution_start_to_close_timeout=execution_start_to_close_timeout,
        task_start_to_close_timeout=task_start_to_close_timeout,
        child_policy=child_policy,
        data_converter=data_converter,
        description=description,
        skip_registration=skip_registration)

    def _execute(func):
        # set description
        if _workflow_type.description is None:
            _workflow_type.description = func.__doc__

        _set_swf_options(func, 'workflow_type', _workflow_type)
        return decorator_descriptors.WorkflowExecuteFunc(func)

    return _execute


def activities(task_list=USE_WORKER_TASK_LIST,
               activity_name_prefix="",
               heartbeat_timeout=None,
               schedule_to_start_timeout=None,
               start_to_close_timeout=None,
               schedule_to_close_timeout=None,
               data_converter=None):
    """This decorator can be used to declare a set of activity types.
    Each method on the class annotated with this decorator represents an
    activity type. An interface cannot have both @workflow and @activities
    decorators.

    :param str activity_name_prefix: Specifies the prefix of the name of the
        activity types declared in the interface. If set to empty string (which
        is the default), the name of the class followed by '.' is used as the
        prefix.

    :param data_converter: Specifies the type of the DataConverter to use for
        serializing/deserializing data when creating tasks of this activity
        type and its results. Set to `None` by default, which indicates that
        the JsonDataConverter should be used.
    :type data_converter: :py:class:`~awsflow.data_converter.abstract_data_converter.AbstractDataConverter`
    """

    def _activities(cls):
        for name in dir(cls):
            try:
                _func = getattr(cls, name)
            except AttributeError:
                continue

            if isinstance(_func, types.FunctionType):
                if hasattr(_func, 'swf_options'):  # decorated
                    _set_swf_options(_func, 'activity_name_prefix',
                                     activity_name_prefix)
                    _set_swf_options(_func, 'data_converter',
                                     data_converter)

                    activity_type = _func.swf_options['activity_type']

                    activity_type._set_activities_value(
                        'task_list', task_list)
                    activity_type._set_activities_value(
                        'heartbeat_timeout', heartbeat_timeout)
                    activity_type._set_activities_value(
                        'schedule_to_start_timeout', schedule_to_start_timeout)
                    activity_type._set_activities_value(
                        'start_to_close_timeout', start_to_close_timeout)
                    activity_type._set_activities_value(
                        'schedule_to_close_timeout', schedule_to_close_timeout)

        return cls

    return _activities


def activity(version,
             name=None,
             task_list=USE_WORKER_TASK_LIST,
             heartbeat_timeout=None,
             schedule_to_start_timeout=None,  # indicates not set
             start_to_close_timeout=None,  # indicates not set
             schedule_to_close_timeout=None,
             description=None,
             skip_registration=False,
             manual=False):

    """Indicates an activity type

    :param str version: Specifies the version of the activity type.
    :param str name: Specifies the name of the activity type. The default is
        `None`, which indicates that the default prefix and the activity method
        name should be used to determine the name of the activity type (which
        is of the form {prefix}{name}). Note that when you specify a name in an
        :py:func:`.activity`, the framework will not automatically prepend a
        prefix to it. You are free to use your own naming scheme.
    :param str task_list: Specifies the default task list to be registered with
        Amazon SWF for this activity type.  The default can be overridden using
        :py:func`~botoflow.options_overrides.activity_options` when calling the
        activity. Set to :py:data:`~botoflow.constants.USE_WORKER_TASK_LIST` by
        default. This is a special value which indicates that the task list
        used by the worker, which is performing the registration, should be
        used.
    :param heartbeat_timeout: Specifies the defaultTaskHeartbeatTimeout
        registered with Amazon SWF for this activity type. Activity workers
        must provide heartbeat within this duration; otherwise, the task will
        be timed out. Set to `None` by default, which is a special value that
        indicates this timeout should be disabled. See Amazon SWF API reference
        for more details.
    :type heartbeat_timeout: int or None
    :param schedule_to_start_timeout: Specifies the
        defaultTaskScheduleToStartTimeout registered with Amazon SWF for this
        activity type. This is the maximum time a task of this activity type is
        allowed to wait before it is assigned to a worker. It is required
        either here or in :py:func:`.activities`.
    :type schedule_to_start_timeout: int or None
    :param start_to_close_timeout: Specifies the defaultTaskStartToCloseTimeout
        registered with Amazon SWF for this activity type. This timeout
        determines the maximum time a worker can take to process an activity
        task of this type.
    :type start_to_close_timeout: int or None
    :param schedule_to_close_timeout: Specifies the
        defaultScheduleToCloseTimeout registered with Amazon SWF for this
        activity type. This timeout determines the total duration that the task
        can stay in open state. Set to `None` by default, which indicates this
        timeout should be disabled.
    :type schedule_to_close_timeout: int or None
    :param description: Textual description of the activity type. By default
        will use the docstring of the activity if available. The maximum length
        is 1024 characters, so a long docstring will be truncated to that
        length.
    :type description: str or None
    :param bool skip_registration: Indicates that the activity type should not
        be registered with Amazon SWF.
    :param bool manual: Indicates that this is a manual activity, if set to true.
    """

    _activity_type = ActivityType(
        version,
        name=name,
        task_list=task_list,
        heartbeat_timeout=heartbeat_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        schedule_to_close_timeout=schedule_to_close_timeout,
        description=description,
        skip_registration=skip_registration,
        manual=manual)

    def _activity(func):
        # assume class for now XXX find a safer way
        if not isinstance(func, types.FunctionType):
            raise AttributeError("Can only be applied to functions/methods")

        # set description
        if _activity_type.description is None:
            description = func.__doc__

            if description is None:
                description = ""
            else:
                # truncate to 1024 chars
                description = (description[:1022] + '..') if len(
                    description) > 1024 else description

            _activity_type.description = description

        _set_swf_options(func, 'activity_type', _activity_type)
        return decorator_descriptors.ActivityFunc(func)

    return _activity


def retry_activity(stop_max_attempt_number=None, stop_max_delay=None, wait_fixed=None, wait_random_min=None,
                   wait_random_max=None, wait_incrementing_start=None, wait_incrementing_increment=None,
                   wait_exponential_multiplier=None, wait_exponential_max=None, retry_on_exception=None,
                   retry_on_result=None, wrap_exception=False, stop_func=None, wait_func=None):
    """Retry activity decorator

    (based on the :py:mod:`retrying` library)

    **Examples:**

    .. code-block:: python
       :caption: Retry forever

       @retry
       @activity(version='1.0', start_to_close_timeout=float('inf'))
       def never_give_up_never_surrender_activity():
           print "Retry forever ignoring Exceptions, don't wait between retries"


    .. code-block:: python
       :caption: Retry only a certain number of times

       @retry(stop_max_attempt_number=7)
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def stop_after_7_attempts_activity():
           print "Stopping after 7 attempts"

    .. code-block:: python
       :caption: Stop retrying after 10 seconds

       @retry(stop_max_delay=10*SECONDS)
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def stop_after_10_s_activity():
           print "Stopping after 10 seconds"

    .. code-block:: python
       :caption: Wait a random amount of time

       @retry(wait_random_min=1*SECONDS, wait_random_max=2*SECONDS)
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def wait_random_1_to_2_s_activity():
           print "Randomly wait 1 to 2 seconds between retries"

    .. code-block:: python
       :caption: Wait an exponentially growing amount of time

       @retry(wait_exponential_multiplier=1*SECONDS, wait_exponential_max=10*SECONDS)
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def wait_exponential_1_activity():
           print "Wait 2^x * 1 second between each retry, up to 10 seconds, then 10 seconds afterwards"

    .. code-block:: python
       :caption: Retry on exception

       @retry(retry_on_exception=retry_on_exception(IOError, OSError))
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def might_io_os_error():
           print "Retry forever with no wait if an IOError or OSError occurs, raise any other errors"

    .. code-block:: python
       :caption: Custom exception retryer

       def retry_if_io_error(exception):
       \"\"\"Return True if we should retry (in this case when it's an IOError), False otherwise\"\"\"
       if isinstance(exception, ActivityTaskFailedError):
           return isinstance(exception.cause, IOError)

       @retry(retry_on_exception=retry_if_io_error)
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def might_io_error():
           print "Retry forever with no wait if an IOError occurs, raise any other errors"

    .. code-block:: python
       :caption: Custom retryer based on result

       def retry_if_result_none(result):
       \"\"\"Return True if we should retry (in this case when result is None), False otherwise\"\"\"
           return result is None

       @retry(retry_on_result=retry_if_result_none)
       @activity(version='1.0', start_to_close_timeout=10*MINUTES)
       def might_return_none():
           print "Retry forever ignoring Exceptions with no wait if return value is None"

    :param stop_max_attempt_number: Stop retrying after reaching this attempt number. Default is 3 attempts.
    :type stop_max_attempt_number: int
    :param stop_max_delay: Retry for at most this given period of time in seconds. Default is 1 second.
    :type stop_max_delay: float
    :param wait_fixed: Wait a fixed amount of seconds before retrying. Default is 1 second.
    :type wait_fixed: float
    :param wait_random_min: Random wait time minimum in seconds. Default is 0 seconds.
    :type wait_random_min: float
    :param wait_random_max: Maximum random wait time in seconds. Default is 1 second.
    :type wait_random_max: float
    :param wait_incrementing_start: Starting point for waiting an incremental amount of time. Default is 0.
    :type wait_incrementing_start: float
    :param wait_incrementing_increment: For each attempt, by how much we increment the waiting period.
       Default is 1 second.
    :type wait_incrementing_increment: float
    :param wait_exponential_multiplier: Exponential wait time multiplier. Default is 1.
    :type wait_exponential_multiplier: float
    :param wait_exponential_max: Maximum number in seconds for the exponential multiplier to get to. Default is 1073741.
    :type wait_exponential_max: float
    :param retry_on_exception: A function that returns True if an exception needs to be retried on
    :type retry_on_exception: callable
    :param retry_on_result: A function that returns True if a retry is needed based on a result.
    :type retry_on_result: callable
    :param wrap_exception: Whether to wrap the non-retryable exceptions in RetryError
    :type wrap_exception: bool
    :param stop_func: Function that decides when to stop retrying
    :type stop_func: callable
    :param wait_func: Function that looks like f(previous_attempt_number, delay_since_first_attempt_ms) and returns a
       a new time in ms for the next wait period
    :type wait_func: callable
    """
    def _retry(func):
        retrying = Retrying(stop_max_attempt_number=stop_max_attempt_number,
                            stop_max_delay=stop_max_delay, wait_fixed=wait_fixed, wait_random_min=wait_random_min,
                            wait_random_max=wait_random_max, wait_incrementing_start=wait_incrementing_start,
                            wait_incrementing_increment=wait_incrementing_increment,
                            wait_exponential_multiplier=wait_exponential_multiplier,
                            wait_exponential_max=wait_exponential_max, retry_on_exception=retry_on_exception,
                            retry_on_result=retry_on_result,
                            wrap_exception=wrap_exception,
                            stop_func=stop_func,
                            wait_func=wait_func)

        # is the activity already wrapped in a descriptor?
        if isinstance(func, decorator_descriptors.ActivityFunc):
            _set_swf_options(func.func, 'activity_retrying', retrying)
        else:
            _set_swf_options(func, 'activity_retrying', retrying)

        return func

    return _retry


def manual_activity(version,
                    name=None,
                    task_list=USE_WORKER_TASK_LIST,
                    heartbeat_timeout=None,
                    schedule_to_start_timeout=None,  #indicates not set
                    start_to_close_timeout=None,    #indicates not set
                    schedule_to_close_timeout=None,
                    description=None,
                    skip_registration=False):

    """Indicates a manual activity type

    :param str version: Specifies the version of the activity type.
    :param str name: Specifies the name of the activity type. The default is
        `None`, which indicates that the default prefix and the activity method
        name should be used to determine the name of the activity type (which
        is of the form {prefix}{name}). Note that when you specify a name in an
        :py:func:`.activity`, the framework will not automatically prepend a
        prefix to it. You are free to use your own naming scheme.
    :param str task_list: Specifies the default task list to be registered with
        Amazon SWF for this activity type.  The default can be overridden using
        :py:func`~botoflow.options_overrides.activity_options` when calling the
        activity. Set to :py:data:`~botoflow.constants.USE_WORKER_TASK_LIST` by
        default. This is a special value which indicates that the task list
        used by the worker, which is performing the registration, should be
        used.
    :param heartbeat_timeout: Specifies the defaultTaskHeartbeatTimeout
        registered with Amazon SWF for this activity type. Activity workers
        must provide heartbeat within this duration; otherwise, the task will
        be timed out. Set to `None` by default, which is a special value that
        indicates this timeout should be disabled. See Amazon SWF API reference
        for more details.
    :type heartbeat_timeout: int or None
    :param schedule_to_start_timeout: Specifies the
        defaultTaskScheduleToStartTimeout registered with Amazon SWF for this
        activity type. This is the maximum time a task of this activity type is
        allowed to wait before it is assigned to a worker. It is required
        either here or in :py:func:`.activities`.
    :type schedule_to_start_timeout: int or None
    :param start_to_close_timeout: Specifies the defaultTaskStartToCloseTimeout
        registered with Amazon SWF for this activity type. This timeout
        determines the maximum time a worker can take to process an activity
        task of this type.
    :type start_to_close_timeout: int or None
    :param schedule_to_close_timeout: Specifies the
        defaultScheduleToCloseTimeout registered with Amazon SWF for this
        activity type. This timeout determines the total duration that the task
        can stay in open state. Set to `None` by default, which indicates this
        timeout should be disabled.
    :type schedule_to_close_timeout: int or None
    :param description: Textual description of the activity type. By default
        will use the docstring of the activity if available. The maximum length
        is 1024 characters, so a long docstring will be truncated to that
        length.
    :type description: str or None
    :param bool skip_registration: Indicates that the activity type should not
        be registered with Amazon SWF.
    """

    return activity(version, name, task_list, heartbeat_timeout,
                    schedule_to_start_timeout, start_to_close_timeout,
                    schedule_to_close_timeout,
                    description, skip_registration, manual=True)


def signal(name=None):
    """When used on a method in the WorkflowDefinition subclass, identifies a
    signal that can be received by executions of the workflow.
    Use of this decorator is required to define a signal method.

    :param str name: Specifies the name portion of the signal name.
        If not set, the name of the method is used.
    """

    def _signal(func):
        _signal_type = SignalType(name=name)
        if name is None:
            _signal_type = SignalType(name=func.__name__)

        _set_swf_options(func, 'signal_type', _signal_type)
        return decorator_descriptors.SignalFunc(func)

    return _signal
