================================================
Botoflow - Asynchronous Framework for Amazon SWF
================================================

Botoflow is an asynchronous framework for `Amazon SWF`_ that helps you
build SWF applications using Python. You can find the latest, most
up to date, documentation at `Read the Docs`_ including the "Getting Started Guide".

Under the hood it uses `botocore`_ low level interface to interact with `Amazon SWF`_.

.. _`botocore`: https://github.com/boto/botocore
.. _`Read the Docs`: https://botoflow.readthedocs.io/en/latest/
.. _`Amazon SWF`: https://aws.amazon.com/swf/


Issue tracker
-------------

Please report any bugs or enhancement ideas using our issue tracker:
https://github.com/boto/botoflow/issues . Also, feel free to ask any
other project related questions there.


Development
-----------

Getting Started
~~~~~~~~~~~~~~~
Assuming that you have Python and ``virtualenv`` installed, set up your
environment and install the required dependencies:

.. code-block:: sh

    $ git clone https://github.com/boto/botoflow.git
    $ cd botoflow
    $ virtualenv venv
    ...
    $ . venv/bin/activate
    $ pip install -r requirements.txt
    $ pip install -e .

Running Tests
~~~~~~~~~~~~~
You can run tests in all supported Python versions using ``tox``. By default,
it will run all of the unit tests, but you can also specify your own
``pytest`` options. Note that this requires that you have all supported
versions of Python installed, otherwise you must pass ``-e`` or run the
``pytest`` command directly:

.. code-block:: sh

    $ tox
    $ tox test/unit/test_workflow_time.py
    $ tox -e py27,py34 test/integration

You can also run individual tests with your default Python version:

.. code-block:: sh

    $ py.test -v test/unit

Generating Documentation
~~~~~~~~~~~~~~~~~~~~~~~~
Sphinx is used for documentation. You can generate HTML locally with the
following:

.. code-block:: sh

    $ pip install -r requirements-docs.txt
    $ cd docs
    $ make html
