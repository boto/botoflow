.. botoflow documentation master file, created by
   sphinx-quickstart on Mon Feb 25 16:12:26 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

============================
botoflow
============================

.. warning::

    This documentation as well as the underlying APIs are 'work in progress'
    (!) and may/will change before the first release.

--------------
About Botoflow
--------------

The botoflow is a programming framework that works together with
Amazon Simple Workflow Service (Amazon SWF) to help developers build
asynchronous and distributed applications that process work asynchronously and
distribute the processing across components that execute remotely.  Using the
framework, such work can be organized into discrete units called tasks. You can
create tasks that are independent of each other and may execute
concurrently. You can also create tasks that depend on the outcome of other
tasks and need to be sequenced. In fact, the framework allows you to implement
complex graphs of tasks by interconnecting them through a control flow. Best of
all, the flow of tasks can be expressed naturally through the control flow of
the application itself, using features of Python that you are already familiar
with. This makes development easy by allowing you to focus on your
application's business logic while the framework takes care of the mechanics of
creating and coordinating tasks.  Under the hood, the framework is powered by
the scheduling, routing, and state management features of Amazon SWF. The use
of Amazon SWF also makes your applications scalable, reliable, and
auditable. Applications written using the framework are highly concurrent and
can be readily distributed across processes and machines. The framework is
ideal for a broad set of use cases such as business process workflows, media
encoding, long running tasks, and background processing.




Overview
--------

.. toctree::
   :maxdepth: 2

   overview

Concepts
--------

.. toctree::
   :maxdepth: 2
	      
   concepts

Setting up Development Environment
----------------------------------

.. toctree::
   :maxdepth: 2
	      
   setting_up_dev_env


Feature Details
---------------

.. toctree::
   :maxdepth: 2
	      
   feature_details


API Reference
-------------

.. toctree::
   :maxdepth: 3
	      
   reference/index
   

==================
Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

