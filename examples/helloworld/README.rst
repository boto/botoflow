==================
Helloworld Example
==================

**Requires: python3**

This example creates a two activities and one decider for those activities. The first activity will
ask for a name and then the second activity will print that name.

How To
------

Preconfiguration
~~~~~~~~~~~~~~~~

First an IAM user with privileges in SWF is needed. Can inline policy for an existing user can be:

.. code-block::

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": [
            "swf:*"
          ],
          "Effect": "Allow",
          "Resource": "*"
        }
      ]
    }


Setup a SWF ``domain`` and register the ``workflow-type`` in your AWS account before being able to run this example.

This is possible with ``awscli``:

.. code-block:: sh

    $ aws swf register-domain \
              --name helloworld \
              --description "Helloworld domain" \
              --workflow-execution-retention-period-in-days 7 \
              --region us-east-1


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
