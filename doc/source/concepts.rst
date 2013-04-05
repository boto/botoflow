===========================
AWS Flow Framework in Python concepts
===========================

This section describes concepts that are important to understand and use the
AWS Flow Framework correctly.

Activity
--------

An activity is a method that may be executed remotely. The implementation of an
activity may be long running, and may perform complex computations and
I/O. Activities are invoked using a classmethod that is decorated with activity
information and is recognized by the framework. Using this classmethod, you can
invoke activity methods asynchronously. Activity implementations are hosted
using container programs called activity workers. When you invoke an activity,
a task is created in Amazon SWF (SWF). The worker that hosts the activity
implementation receives this task, executes the activity, and returns the
result to Amazon SWF.

Activities are declared using a @activity decorator. This decorator defines the
signature or the contract of the activity. Note that activities need not be
hosted by the same worker process. How and where activities are hosted is a
deployment decision, and you can host activities on different workers.


Workflow
--------

Workflow is the control flow that coordinates activities. A workflow is defined
by subclassing from a
:py:class:`~awsflow.workflow_definition.WorkflowDefinition` class. This
class may contain a method decorated with an @execute() decorator, which is
the entry point of the workflow. The entry point of the workflow can be invoked
remotely by your application by calling the method in an appropriate context.

Future/yield
------------

TODO

Replay
------

.. TODO::

   This needs to be better adapted to AWS Flow futures


When you run the HelloWorld example, the AWS Flow Framework in Python will call its entry
point method, `hello_world`. This method will result in the creation of two
tasks: one for `get_name` and another for `print_greeting`. Since the
`print_greeting` task depends on the result of get_name, it cannot be executed
until get_name has produced a result. Given that get_name is a remote task,
which may take a long time to complete, it is not a good idea to keep waiting
for it on the same thread or even the same worker process - doing so would make
the application unreliable and limit its scalability.

Instead, the framework relies on the history maintained by Amazon SWF to
execute the workflow in episodes. When a workflow is executed, the framework
does not wait for remote activity calls to complete. Instead, it only completes
tasks that have no dependency on pending remote activity calls and reports to
Amazon SWF that the current episode of execution has completed. It also gives
Amazon SWF a list of outstanding remote activity calls, termed as schedule
activity task decisions. Amazon SWF stores a durable record of these decisions
in history and schedules tasks for them. These tasks are dispatched to activity
workers for execution. When an activity worker completes a task, Amazon SWF
records the result in the workflow execution history and schedules a new
decision task for the workflow worker. Upon receiving this task, the workflow
worker starts executing the workflow from the beginning again - that is, from
the workflow entry point. The same tasks will be created again; however, the
framework inspects the workflow execution history from Amazon SWF to figure out
which remote tasks have completed. In this case, it will find a result for the
`get_name` activity, so it will mark the task completed and execute
`print_greeting`, which is now unblocked.

This mechanism of running episodes of the workflow implementation as tasks
complete is called replay. It is important to understand that after a workflow
execution is started, the implementation of the workflow will be invoked many
times over by the framework. The first episode happens in order to process the
task that was created for starting the workflow execution. As the workflow
execution continues, it creates activity tasks. These activity tasks are
dispatched to workers. When a worker completes an activity task, the framework
replays the workflow implementation, plugging in the result of the completed
activity. This time, the code that depends on the result of the activity will
also execute, possibly scheduling more tasks. As more activities complete, the
framework keeps replaying the workflow with their results until no tasks for
the workflow execution are left pending. At this point, the workflow execution
is considered complete.

In each episode of workflow replay, the following happens:

1. The framework executes the entry point method of the workflow. Any
   asynchronous code invoked from it will result in the creation of
   tasks. However, calls to an activity method will result in a task only if it
   wasn’t already scheduled in a previous episode.
2. The tasks are executed as they become unblocked (`yield` returns a value).

   * All tasks that do not block on the results of remote activity calls will be
      executed by the framework.
   * If a task depends on the result of a remote activity, the framework will
      check the history to see if the task has already completed. If so, the
      dependent task will be executed. This may in turn create more tasks.

3. When all tasks that can be executed have completed, the framework will
   report back to Amazon SWF and provide it with a list of additional remote
   tasks to schedule.

If there are no additional tasks to schedule and all pending tasks have already
completed, the workflow worker will report to Amazon SWF that the workflow
execution has completed.  Because of this replay mechanism, developers must
ensure that the workflow logic should not do any time-consuming computation or
I/O processing. Such computation and processing should be delegated to
activities since they don't get replayed. In addition, the workflow logic
should be completely deterministic and must not take different paths through
the control flow between episodes.


Exchanging Data with Activities and Workflows
---------------------------------------------

Workflow executions can take input at the start and produce output on
completion. The input data can be provided by passing arguments when calling
the workflow entry point method. Similarly, data can be passed to activities
when calling the activity method. The return value of an activity method is
returned to the caller through the
:py:class:`~awsflow.core.future.Future`. AWS Flow Framework in Python takes care of
marshaling the data across the wire using a component called DataConverter. The
default
:py:class:`~awsflow.data_converter.json_data_converter.JSONDataConverter` used
by the framework is based on simplejson and pickle concepts.


Signals
-------

Besides initial inputs, there are cases where you might need to give additional
input to the workflow execution while it is running. For example, you may need
to process an external event that happens after the workflow execution has been
started. To accomplish this, Amazon SWF provides the ability to send signals to
a running workflow instance. In the AWS Flow Framework in Python, you can define the
signals that your workflow can accept as methods in the workflow definition and
decorate them with the @ :py:func:`~awsflow.decorators.signal`. Methods
decorated with @ :py:func:`~awsflow.decorators.signal` get invoked when a
signal with a matching name is received by Amazon SWF. You can use the
workflow instance objects to send signals (as you would call an
instancemethod). When a signal is received, the framework unmarshals the data
passed with the signal and invokes the appropriate signal method.


Task Lists and Routing
----------------------

In Amazon SWF, tasks are organized into named lists that are automatically
managed by Amazon SWF. Each task is scheduled in a list and workers poll task
lists to get tasks. When you create a worker, you provide the name of the task
list that you want the worker to poll. Similarly, a task list can be specified
when you schedule a task using the
:py:class:`awsflow.options.activity_options` context manager. If you don't
specify a task list, the AWS Flow Framework in Python will use a default one to schedule
the task. The default task list is specified when a type is registered with
Amazon SWF.

There are situations where you want some tasks to be assigned to a specific
worker or a group of workers. For example, in an image processing scenario, you
may have an activity to download the image and another activity to process
it. In this case, the file-processing activity should be assigned to the same
worker that downloaded the file or another worker running on the same host. To
address such use cases, the framework enables you to explicitly specify a task
list when calling an activity. This allows you to make the task available to a
specific worker. For example, in the image processing workflow, you want the
same worker to download and process the image. In the following workflow
implementation, the `download_image` activity returns the name of the task list
that is used to schedule the task for the `create_thumbnail` activity.

.. code-block:: python

    class ImageProcessingWorkflow(WorkflowDefinition):

        @execute(version='1.0', execution_start_to_close_timeout=10*MINUTES)
        def process_images(self, image_urls):
            processing_futures = []

            for image_url in image_urls:
                future = self.process_image(image_url)
                processing_futures.append(future)

            # wait for all the images to be processed
            yield processing_futures

        @async
        def process_image(self, image_url):
            worker_task_list, image_name = ImageActivities.download_image(image_url)

            # all activities started inside this context manager will have
            # their task list set to worker_task_list
            with activity_options(task_list=worker_task_list):
                ImageActivities.create_thumbnail(image_name)


You can configure the task list that the worker should poll when you create the
worker. For example, you can use the host name as the name for the
host-specific task list:

.. code-block:: python

    from socket import gethostname
    swf_endpoint = botocore.session.get_session().get_service('swf').get_endpoint('us-east-1')
    worker = ActivityWorker(swf_endpoint, 'domain1', gethostname(), ImageActivities())
    worker.run()

