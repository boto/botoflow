========
Overview
========

A growing number of applications rely on asynchronous and distributed
processing. The scalability of the application is the primary motivation for
using this approach. By designing autonomous distributed components, developers
get the flexibility to deploy and scale out parts of the application
independently as load increases. Another motivation is the consumption of cloud
services. As application developers take advantage of cloud computing, they
need to bridge their existing on-premise assets with new assets in the
cloud. Another motivation for such a design approach is the inherent
distributed nature of the process being modeled by the application. For
example, automating an order fulfillment business process may span several
systems and human tasks.

The asynchronous and distributed model has the benefits of loose coupling and
selective scalability, but it also creates new challenges. Application
developers must coordinate multiple distributed components to get the desired
results. They must deal with the increased latency and unreliability inherent
in remote communication. Components may take extended periods of time to
complete tasks, requests may fail, and errors originating from remote systems
must be handled. To accomplish this today, developers are forced to build
complicated infrastructure that typically involves message queues and databases
along with complex logic to synchronize them. All this "plumbing" is extraneous
to business logic and makes the application code unnecessarily complicated and
hard to maintain.

The AWS Flow Framework in Python provides a solution that makes application development
simple while enabling developers to get all the benefits of asynchronous and
distributed processing in their applications. It accomplishes this through a
powerful programming model layered on Amazon SWF.


Hello World!
------------

The following example shows a distributed program written using the AWS Flow
Framework. This program asynchronously calls a remote method, which returns a
greeting message that is printed to the console.

.. code-block:: python

    @activities(schedule_to_start_timeout=1*MINUTES,
                start_to_close_timeout=1*MINUTES)
    class HelloWorldActivities(object):

        @activity('1.0')
        def get_name(self):
            return input()

        @activity('1.0')
        def print_greeting(self, name):
            print "Hello {}!".format(name)


    class HelloWorldWorkflow(WorkflowDefinition):

        @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
        def hello_world(self):
            name = yield HelloWorldActivities.get_name()
            yield HelloWorldActivities.print_hello(name)

The example above defines a workflow and two activities. Workflows and
activities are the basic building blocks in AWS Flow Framework in Python. An activity is
a unit of functionality that is invoked asynchronously through Amazon SWF; in
other words, it is the implementation of a task. A workflow, on the other hand,
is the control flow that coordinates the execution of activities. Using a
workflow you can arbitrarily compose tasks, simply by calling activity methods.

For clean separation, the framework uses different base classes/decorators to
define the signature or contract of activities and workflows. The activities
class is decorated with @ :py:func:`~awsflow.decorators.activities` and @
:py:func:`~awsflow.decorators.activity`, and workflow is a subclass of
:py:class:`~awsflow.workflow_definition.WorkflowDefinition`. The workflow
class in our example is HelloWorldWorkflow and has only one method:
`hello_world` that is decorated with @
:py:func:`~awsflow.decorators.execute`. Similarly, the activities, `get_name`
and `print_greeting`, are represented by methods in the activities class. The
workflow implementation invokes the `get_name` activity to get the user's
name/message and calls the `print_greeting` activity to print a greeting to the
console.

It is important to understand that this trivial program is in fact a
distributed application. The workflow and activity implementations can be
hosted on separate machines and scaled independently. In order to call an
activity, you don't need to instantiate the activities object and instead call
it as you would call a classmethod.

You can start an instance of this workflow using the following code snippet:

.. code-block:: python

    swf_endpoint = botocore.session.get_session().get_service('swf').get_endpoint('us-east-1')
    worker = WorkflowWorker(swf_endpoint, 'domain1', "tasklist1", HelloWorldWorkflow)
    with worker: HelloWorldWorkflow.hello_world()

Here we use botocore SWF endpoint *(we use it for authentication as well as
low-level communication with SWF service)* and pass it to our WorkflowWorker,
then we call `HelloWorldWorkflow.hello_world()` in the worker context to start
our workflow execution.


.. seqdiag::

    seqdiag flow {
        Application   -->> WorkflowLogic [label = "HelloWorldWorkflow.hello_world()"];
        WorkflowLogic -->> Activity      [label = "HelloWorldActivities.get_name()"];
        WorkflowLogic <<-- Activity      [label = "return name"];
        WorkflowLogic -->> Activity      [label = "HelloWorldActivities.print_greeting()"]
    }


Non-Blocking Code Using Tasks
-----------------------------

Note that in the previous example, the `get_ame` activity was returning a `str`
but when called from within the workflow, it returns
:py:class:`~awsflow.core.future.Future`. When you call this method from within
the workflow, it returns immediately. This is because it only schedules a task
for execution and does not block, waiting for it to complete. This means that
the actual result of the activity is not available to the caller when the call
returns. Therefore, the method returns an object of type
:py:class:`~awsflow.core.future.Future` as a placeholder for the "future"
result of executing the remote activity. The actual result is returned only
after the activity has completed, and the time needed to dispatch the task
through Amazon SWF.

One more thing to note in this example is the `yield` keyword for both
`get_name()` and `print_greeting()` calls. The `yield` keyword in this case is
used to indicate that we want to wait for the
:py:class:`~awsflow.core.future.Future` object to complete the call and return
the actual value. You use it to indicate that you need the result of the
activity at that point in the code (in this case to pass the name to
`print_greeting()`, we need the result of `get_name()` immediately).


AWS Flow Framework in Python and Amazon Simple Workflow Service
-----------------------------------------------------

AWS Flow Framework in Python uses the Amazon Simple Workflow Service (SWF) to schedule
tasks for execution by remote components, to get their results back, and to
store the overall execution state of the application. Amazon SWF makes it
possible for your application components to be deployed on separate machines
and scaled independently. This also makes the application highly fault tolerant
as it can be executed by multiple processes (workers) and is guaranteed to make
progress if any of them is running.

When using Amazon SWF directly, you implement the processing steps of your
application as activity workers and the orchestration logic in a decider (also
called a workflow worker). The workers and the deciders also implement code to
poll Amazon SWF and call APIs to provide results and decisions. You then start
workflow executions by calling Amazon SWF APIs from your application.

A program written using the framework consists of three types of components:

* Implementation of individual tasks (that is, activities).
* The coordination logic that orchestrates these tasks (the
  :py:class:`~awsflow.workflow_definition.WorkflowDefinition`).
* A component that initiates the coordination logic.

Each of these components can be hosted on separate workers or worker pools and
interact with each other through Amazon SWF. Hence, using the framework, you
can easily create activity workers that host and execute activities, and
workflow workers that host and execute the orchestration logic.


Durable Execution State
-----------------------

In the Hello World example, the activity takes only a few seconds to execute,
but the AWS Flow Framework in Python allows activities to take arbitrarily long to
complete. For example, an activity may be used to perform complex computation
that takes several hours. In order to reliably execute such long running
processes, the execution state of workflow must be stored durably. The
framework relies on Amazon SWF for this purpose. Amazon SWF maintains the
history (or the state of execution) of each workflow instance. At any point in
time, the history of a workflow instance is a complete and authoritative record
of all the activities scheduled so far, their progress, and results. The
framework uses this history to seamlessly keep track of the progress of the
workflow. This frees you from having to manage the execution state explicitly
in a durable store yourself.

Amazon SWF also provides a reliable communication mechanism between the
workflow and activities. The framework uses it to dispatch tasks to remote
activities and to receive their results. Tasks scheduled in Amazon SWF are
stored durably and are guaranteed to be delivered at most once. When a task
completes, successfully or with error, its results are also stored durably by
Amazon SWF. The framework retrieves these results from Amazon SWF and based on
them proceeds with the execution of your workflow. Amazon SWF guarantees that
the remote activity will either complete successfully or the calling code will
be notified of the failure to complete.

You can also configure the framework to retry a failed task
automatically. These semantics eliminate the need for you to use complex
message passing and queues in your code. You can simply rely on the framework
and Amazon SWF to schedule remote tasks and let them handle the details of
dispatching tasks, retrying failed tasks, and durably storing the results of
their execution. Once a task is complete, the framework receives the results on
your behalf. Since Amazon SWF maintains a durable record of all tasks and their
results, the remote task and its results are not lost if the application
crashes or gets disconnected. Even if all activity workers and deciders go
down, because the execution state is stored by Amazon SWF, the workflow
execution can continue as soon as the activity workers and deciders come back
up.

.. blockdiag::

    blockdiag {
        orientation = portrait;
        Amazon_SWF [shape = cloud, label = "Amazon SWF"];

        group {
            label = "Host pool";
            color = "magenta";
            Activity_Workers [stacked, label = "Activity Workers"];
        }

        group {
            label = "Workflow starter";
            color = "#77FF77";

            HelloWorldWorkflow;
        }

        group {
            HelloWorldActivities [stacked];
        }

        HelloWorldWorkflow -> Amazon_SWF;
        HelloWorldActivities -> Amazon_SWF;
        Activity_Workers <-> Amazon_SWF;

    }

Distributed Execution
---------------------

In essence, each workflow instance is a virtual thread of execution. This
virtual thread of execution may span activities and orchestration logic running
on several remote machines. Amazon SWF and the framework act as the operating
system that manages these threads on a virtual CPU. It keeps the state of
execution of the thread, switches between threads, and knows how to resurrect a
thread back to the point at which it was switched out. As remote activities
complete, the framework looks at the history and replays the workflow logic,
plugging in the results of completed tasks. As tasks complete, the workflow
logic makes more progress each time it is replayed. Since the workflow logic
invokes activities, which may be remote and long running, the framework does
not replay them. Instead, it plugs in the results that activities returned
using the history.

This ability to resurrect the program from state stored in Amazon SWF means
that the program is stateless and you can run it on many machines for the
purpose of scalability. The program can be initiated independently of workers'
availability because the initiation is managed by Amazon SWF. The program is
highly scalable as any number of instances can be created in parallel. Requests
to execute activities are delivered to workers through dynamically allocated
consistent logical queues called task lists; therefore, the work is
automatically load balanced among worker processes. Amazon SWF uses the HTTP
long poll mechanism to deliver tasks to workers allowing them to pull tasks at
their own pace. This ensures that workers are not overloaded even if there is
an unexpected spike in requests. The HTTP poll mechanism also allows your
workers to run behind firewalls since you are not required to open externally
visible ports. This allows your applications to use resources in the cloud as
well as on on-premise data centers.

Together, the AWS Flow Framework in Python and Amazon SWF make it easy to create scalable
and fault tolerant applications that perform asynchronous tasks that may be
long running, remote, or both.

