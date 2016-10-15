Change Log
==========

0.8 (WIP)
---------

**API Changes**

* Remove ``botoflow.retry`` module in favor of ``@retry_activity``
  decorator which is much more extensible and is based on ``retrying``
  library.
* ``WorkflowStarter`` is now ``workflow_starter`` as context managers
  should be snake case and it's better to do this now, before 1.0
  release.
* Renamed ``botoflow.workflow_types`` module to ``botoflow.flow_types`` package.
* Removed ``botoflow.types`` alias. Use ``botoflow.flow_types``.

**Bugfixes**

* Fix a **critical** bug in decider history event ordering, which could affect
  any workflows that execute activities/child workflows concurrently.

**Miscellaneous**

* It's open sourced!
* ``doc`` directory renamed to ``docs``.
* Various package naming fixes.
* Add ``CONTRIBUTING.rst``.
* Add ``README.rst``.
* Lots of work and refactoring in documentation.
* Up to date `requirements.txt`.
* CircleCI integration.
* Thanks to `Gintare Laniauskaite <http://github.com/myselfher>`_ for
  work on the LOGO and documentation design!
* Add tox support and test CPython 2.7, 3.4 and 3.5.
* Mark some tests in test_async_traceback as xfail for Python 3.5.0
  specifically (works fine on 3.5.1).
* Add ``botoflow.__version__``.
* ``examples`` directory and ``helloworld`` example. Thanks to @l2ol33rt for contribution.


0.7 (2016-04-15)
----------------

**Improvements**

* Add ThrottlingException.
* Do not skip registration for all activities.
* Only try registering unregistered activities.
* Supporting more BaseFuture methods properly.
* *json_data_converter* - add support for Decimal objects + tests.
* *json_data_converter* - Make ActivityType serializable in cases
  where ``ActivityFailedException`` is passsed throug SWF.
* *json_data_converter* - Support ``datetime`` and ``timedelta``
  object serialization.
* Add support for activity retrying using the ``retrying`` library as
  the base with the **major difference** being that it takes seconds
  as the smallest resolution as opposed to millis.
* Support handling basic inputs that for example come from on botoflow
  based workflow starters. See commit `70423b5`_ for details.
  
**Bugfixes**

* Fix a regression in ChildWorkflowExecutionHandler where child
  workflow exceptions aren't passed to the parent process.
* Make sure that (Any|All)Future s behave wen passed in an empty
  iterable (they will immediately set an empty result).
* Handle starting a child workflow with a duplicate Workflow ID.
  
**Miscellaneous**

* Unit and integration test improvements.
* *json_data_converter* - Unit test improvements.


0.6 (2015-08-27)
----------------

**API Changes**

* Expose workflow_id and run_id to decision context.
* Initial workflow cancellation support.

  * Add .cancel() method in ``WorkflowDefinition``.

* Add async ``cancellation_handler`` method that can be overwritten to
  do cleanup for workflow cancellation tasks.

**Improvements**

* Implement workflow cancellation events.
* Big refactor of decider.py into a bunch of specific handlers
  (ie. ``activity_task_handler.py``, etc.).
* Add separate future for cancel activity.
* Support ``RequestCancelActivityTaskFailedError``.
* ``ActivityWorker`` understands ``Cancellation`` / ``CancelledError``.
* Implement ``ActivityFuture`` that replaces the regular Future for
  all activities and has SWF support for cancellations.
* Implement ``request_heartbeat()`` in Activity context.
* ``ActivityTaskHandler`` uses ``ActivityFuture``.

**Bugfixes**

* Add fallback handling for decoding of exception objects.
* Fix bug in workflow options overrides.
* Added kwarg formatting translation for Child workflow execution
  trigger.
* Respect pagination token returned in SWF workflow execution history.
* Fixed bug with activities decorator kwargs not falling down to
  enclosed activity decorators.

**Miscellaneous**

* Cleanup bogus prints.
* Doctype the async decorator a bit.
* Implement some integration tests.
* ``delete_decision()`` now returns True if a decision was deleted
  successfully and False otherwise.
* Adjust handling of cancel events; parse exception out of event details.
* Cleanup of new handlers and decider.
* More activity cancellation work and more integration tests.
* Logic improvements + integration tests.
* Docstrings all around cancellation work.
* Refactor a bunch of cancellation related tests into a separate test file.

  
0.5 (2014-10-03)
----------------

**API Changes**

* The worker API now uses ``botocore`` session, followed by region,
  followed by domain, followed by default tasklist.

**Miscellaneous**

* Use BotoCore 0.66 which has lots of incompatible changes, therefore
  the worker API changes as well.

  
0.4 (2014-05-21)
----------------

**API Changes**

* Threading and multiprocessing based workers are renamed "Executor" and
  accept a worker as an argument so that they can be used with either
  the normal WorkflowWorker or GenericWorkflowWorker.

**New Features**

* Add GenericWorkflowWorker which allows specifying a function that
  can use any method it wants to lookup workflow definitions from a
  name and version, including by dynamically creating it.

  
0.3 (2014-04-07)
----------------

**Improvements**

* Include additional attributes when serializing / deserializing
  subclasses of list and dict.
* Handle (de)serialization of subclasses of list.

**Bugfixes**

* *json_data_converter* - base64 encode any string that can't bedecoded as unicode.
* *json_data_converter* - Make sure we "flowify" dictionary values.
* Fix workflow and activity registration issue where default task list
  would not be set.

**Miscellaneous**

* Test that default activity task list is honoured.

  
0.2 (2013-05-06)
----------------

**API Changes**

* You can use now @async and @async() with the same result.
* As a shortcut: ``from botoflow import Future``.

**New Features**

* Implement the ``@workflow`` decorator. Using this decorator, you can
  change the workflow name.
* ``@execute`` now also accepts data_converter and description
  parameters. If not specified, the description parameter defaults to
  the @execute method's docstring, as with ``@activity``.

**Improvements**

* The metaclass will now look at all bases of a WorkflowDefinition
  subclass and copy all the ``@execute`` and ``@signal`` methods to
  the class we're creating. Subclassing another workflow should now
  work much better.
* Refactor the data_converter attribute into a property, and add a
  type check.
  
**Bugfixes**

* Create a new event loop on every reset to help with an edge case
  where gc happens at the wrong moment.

**Miscellaneous**

* Add a test that tests ability to pass in multiple signals as data
  into workflow.
* Add a simple workflow subclassing test.
* Add a test that shows *multiver* works.
* Add a test that tests that ``@execute`` method is copied from the
  superclass to the subclass.

  
0.1 (2013-04-05)
----------------

* Initial release

.. _70423b5: https://github.com/boto/botoflow/commit/70423b50532d36082d9d9e6af1b74fc7679bb2f1
