===============
Feature Details
===============

.. warning::

   This section a very early draft.


Workflow and Activity Contracts
-------------------------------

Python classes and decorators are used to declare the signatures of workflows
and activities. For example, a workflow type MyWorkflow is defined as a
subclass of :py:class:`~awsflow.workflow_definition.WorkflowDefinition`:

.. code-block:: python

    class MyWorkflow(WorkflowDefinition):

        @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
        def start_my_workflow(self, input1, input2):
            self.workflow_state = "Workflow started"

        @signal()
        def signal1(self, input1, input2):
            pass

In the example above, the workflow interface MyWorkflow contains a method,
`start_my_workflow`, for starting a new execution. This method is decorated
with @ :py:func:`~awsflow.decorators.execute`. In a given workflow, at least
one method can be decorated with @ :py:func:`~awsflow.decorators.execute`. This
method is the entry point of the workflow logic, and the framework calls this
method to execute the workflow logic when a decision task is received.

The workflow also defines the signals that may be sent to the workflow. The
signal method gets invoked when a signal with a matching name is received by
the workflow execution. For example, the `MyWorkflow` declares a signal method,
`signal1`, decorated with @ :py:func:`~awsflow.decorators.signal`.

The @ :py:func:`~awsflow.decorators.signal` decoratr is required on signal
methods. Signals cannot `return` (if you return a value from a signal, it will
be ignored). A workflow may have zero or more signal methods defined in it.

Methods annotated with @ :py:func:`~awsflow.decorators.execute` and @
:py:func:`~awsflow.decorators.signal` annotations may have any number of
parameters including positional and keyword arguments (`*args, **kwds`).


Additionally, you are also able to report the latest state of a workflow
execution, using a
:py:attr:`~awsflow.workflow_definition.WorkflowDefinition.workflow_state`. This
state is not the entire application state of the workflow. The intended use of
this feature is to allow you to store up to 32KB of data to indicate the
latest status of the execution. For example, in an order processing workflow,
you may store a string which indicates that the order has been received,
processed, or canceled. This method is called by the framework every time a
decision task is completed to get the latest state. The state is stored in
Amazon Simple Workflow Service (SWF) and can be retrieved using SWF APIs. This
allows you to check the latest state of a workflow execution. You can assign
any serializable object, which fits your needs, to this property.


Activities on the other hand can be any `object` with some or all of it's
methods decorated with @ :py:func:`~awsflow.decorators.activity`. In addition,
there's another decorator that can be applied to the class to set some activity
defaults.

.. code-block:: python

    @activities(default_task_schedule_to_start_timeout = 4*MINUTES,
                default_task_start_to_close_timeout = 2*HOURS)
    class MyActivities(object):

        # Overrides values from the @activities decorator
        @activity(default_task_schedule_to_start_timeout = 2*MINUTES)
        def activity1(self):
            """This is sample activity 1"""
            pass

        # the docstring is usually used as the description field, but i'ts also
        # overridable
        @activity(description="My special description")
        def activity2(self, a):
            print a

        # not an activity
        def _some_method(self):
            pass


Workflow and Activity Type Registration
---------------------------------------

Amazon SWF requires activity and workflow types to be registered before they
can be used. The framework automatically registers the workflows and activities
in the implementations you add to the workers. The framework looks for types
that implement workflows and activities and registers them with Amazon SWF. By
default, the framework uses the interface definitions to infer registration
options for workflow and activity types. The workflow worker registers all workflow types
it is configured with that have the @ :py:func:`~awsflow.decorators.execute`
annotation. Similarly, each activity method is required to be annotated with
the @ :py:func:`~awsflow.decorators.activity` decorator and optionally the
class can be decorated with @ :py:func:`~awsflow.decorators.activities`.  The
activity worker registers all activity types that it is configured with the
:py:func:`~awsflow.decorators.activity` decorator. The registration is
performed automatically when you start one of the workers. Workflow and
activity types that have have the `skip_registration` attribute set to `True`
will not be registered with Amazon SWF.

Note that Amazon SWF does not allow you to re-register or modify the type after
it has been registered once. The framework will try to register all types, but
if the type is already registered it will not be re-registered and no error
will be reported. If you need to modify registered settings, you should
register a new version of the type. You can also override registered settings
when starting a new execution or calling an activity using
:py:mod:`awsflow.options` context managers.

The registration requires a type name and some other registration options. The
default implementation determines these as follows:


Workflow Type Name and Version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The framework determines the name of the workflow type from the workflow
definition and the @execute decorated method. The form of the default workflow
type name is `class.__name__`. The default name of the workflow type in the
above example is: *MyWorkflow*. You can override the default name using the
name parameter of the @ :py:func:`~awsflow.decorators.workflow` decorator. The
name must not be an empty string.

The workflow version is specified using the `version` parameter of the @
:py:func:`~awsflow.decorators.execute`. There is no default for the `version`
and it must be explicitly specified. Version is a free form string and you are
free to use your own versioning scheme.


Signal Name
^^^^^^^^^^^

The name of the signal can be specified using the name parameter of the @
:py:func:`~awsflow.decorators.signal`. If not specified, it is defaulted to
the name of the signal method.


Activity Type Name and Version
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The framework determines the name of the activity type from the activity class name. The form of the default activity type name is {prefix}{name}. The
{prefix} is set to the name of the activity class followed by a '.' and
the {name} is set to the method name. The default {prefix} can be overridden in
the @ :py:func:`~awsflow.decorators.activities` decorator with `activity_name_prefix` parameter. You can also specify
the activity type name using the @ :py:func:`~awsflow.decorators.activity` decorator on the activity
method. Note that when you override the name using @activity, the framework
**will** automatically prepend the prefix to it.

The activity version is specified using the version parameter of the @Activities annotation. This version is used as the default for all activities defined in the interface and can be overridden on a per-activity basis using the @Activity annotation.


Workflow and Activity Cancellation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A cancel request will, by default, send a cancel request to all activities and
cancel the workflow execution itself (simultaneously). This behavior can be
changed by implementing a cancellation_handler in your WorkflowDefinition class.

From within an execution, activities can be cancelled with:
"<activity_future>.cancel()".

This will raise a CancelledError exception signifying successful cancellation.
Cancellation will not be successful in any of the following cases:
(1) the activity is not heartbeating
(2) the activity is heartbeating, but chooses to ignore cancel requests
(3) the activity completes before receiving the request

To heartbeat, within an activity simply do: "awsflow.get_context().heartbeat()".
This will raise a CancellationError if a cancel is requested. If this error, a
subclass of, or a CancelledError is raised by the activity, Python-awsflow will
report to SWF that the activity task was cancelled. This exception then gets
assigned to the activity future (see above).

Likewise, workflows can be cancelled from within a WorkflowDefinition's execution
by doing: "<WorkflowDefinition>self.cancel()". This sends out a cancel request to
all open activities and cancels the workflow itself, all in a single decision
(does not wait for activities to complete/cancel).

To cancel other workflow executions, use <WorkflowDefinition>self.cancel_external()
and supply the workflow and run IDs of the target execution as arguments.