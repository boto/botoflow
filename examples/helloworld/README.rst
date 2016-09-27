==================
Helloworld Example
==================

**Requires: python3**

This example creates two activities and one decider for those activities. The first activity will
ask for a name and then the second activity will print that name.

How To
------

Make sure you have an environment configured: `Setup development environment <https://github.com/boto/botoflow/blob/master/docs/source/setting_up_dev_env.rst>`_


Configuration
~~~~~~~~~~~~~

Configure the ``config.ini``:

.. code-block:: sh

    $ cat << EOF > config.ini
      [default]
      domain = helloworld
      tasklist = tasklist1
      EOF

**NOTE:** Take a look at the ``sample_config.ini`` for all options


Execute
~~~~~~~

Activate the ``virtualenv`` that has ``botoflow`` installed:

.. code-block:: sh

    $ . venv/bin/activate

Now run the example:

.. code-block:: sh

    $ python helloworld.py
