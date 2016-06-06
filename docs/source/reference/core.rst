Asynchronous Framework Core
===========================

At the core of botoflow is asynchronous framework utilizing :py:class:`~botoflow.core.future.Future`, similar to the Future described in :pep:`3156`. Since this code was developed before arrival of :pep:`3156` and :py:mod:`asyncio`.

.. todo::

   Eventually we will deprecate this approach in favor of using just :py:mod:`asyncio` as the core. This will take some time as we want to have  *Python 2.7* support, for as long as the language itself is supported.


Core event loop
---------------

.. automodule:: botoflow.core.async_event_loop
   :members:

Async Traceback support and manipulation
----------------------------------------

.. automodule:: botoflow.core.async_traceback
   :members:

Decorators
----------

.. automodule:: botoflow.core.decorators
   :members:

Exceptions
----------

.. automodule:: botoflow.core.exceptions
   :members:

Future and the like
-------------------

.. automodule:: botoflow.core.future
   :members:
