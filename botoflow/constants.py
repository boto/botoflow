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

"""This module contains various workflow, activity and time constants.

Tasklist settings
+++++++++++++++++

.. py:data:: USE_WORKER_TASK_LIST

    Use task list of the ActivityWorker or WorkflowWorker that is used to register activity or workflow as the default
    task list for the activity or workflow type.

.. py:data:: NO_DEFAULT_TASK_LIST

    Do not specify task list on registration. Which means that task list is required when scheduling activity.


Child workflow termination policy settings
++++++++++++++++++++++++++++++++++++++++++

You can learn more about *Child Workflows* from the official
`SWF Developer Guide <http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-adv.html#swf-dev-adv-child-workflows>`_.

.. py:data:: CHILD_TERMINATE

    The child executions will be terminated.

.. py:data:: CHILD_REQUEST_CANCEL

    Request to cancel will be attempted for each child execution by recording a
    :py:class:`~botoflow.history_events.events.WorkflowExecutionCancelRequested` event in its history. It is up to the
    decider to take appropriate actions when it receives an execution history with this event.

.. py:data:: CHILD_ABANDON

    Child policy to abandon the parent workflow. If there are any child workflows still running the will be allowed
    to continue without notice.


Time multipliers
++++++++++++++++

The time multiplier constants are just an attempt at making setting various workflow or activity timeouts more readable.

Consider the following examples and their readability:

.. code-block:: python

    @activities(schedule_to_start_timeout=120,
                start_to_close_timeout=23400)
    class ImportantBusinessActivities(object): ...

    # using the time multiplier constants
    from botoflow.constants import MINUTES, HOURS

    @activities(schedule_to_start_timeout=2*MINUTES,
                start_to_close_timeout=30*MINUTES + 6*HOURS)
    class ImportantBusinessActivities(object): ...


.. py:data:: SECONDS

    ``2*SECONDS = 2``

.. py:data:: MINUTES

    ``2*MINUTES = 120``


.. py:data:: HOURS

    ``2*HOURS = 7200``


.. py:data:: DAYS

    ``2*DAYS = 172800``


.. py:data:: WEEKS

    ``2*WEEKS = 1209600``

"""

# TASK LIST SETTINGS
USE_WORKER_TASK_LIST = "USE_WORKER_TASK_LIST"
NO_DEFAULT_TASK_LIST = "NO_DEFAULT_TASK_LIST"

# CHILD POLICIES
CHILD_TERMINATE = "TERMINATE"
CHILD_REQUEST_CANCEL = "REQUEST_CANCEL"
CHILD_ABANDON = "ABANDON"

# TIME MULTIPLIERS
SECONDS = 1
MINUTES = 60
HOURS = 3600
DAYS = 86400
WEEKS = 604800
