======================================
Setting up the Development Environment
======================================

The following sections provide instructions for setting up your development
environment for the AWS Flow Framework in Python.


Prerequisites
-------------

To develop applications that use the AWS Flow Framework in Python, you will need:

* A working Python 2.7+ environment.
* `BotoCore <https://pypi.python.org/pypi/botocore>`_ (and it's dependencies).
* AWS Flow.
* An active AWS account signed up for `Simple Workflow Service <http://aws.amazon.com/swf>`_.


Developing a Workflow
---------------------

After you have set up the development environment, you can start developing
workflows with the AWS Flow Framework in Python. The typical steps involved in developing
a workflow are as follows:

#. Define activity and workflow contracts. First, analyze your application
   requirements and identify the workflow and activities that are needed to
   fulfill them. For example, in a media processing use case, you may need to
   download a file, process it, and upload the processed file to an Amazon
   Simple Storage Service (S3) bucket. For this application, you may define a
   file processing workflow and activities to download the file, perform
   processing on it, upload the processed file, and delete files from the local
   disk.
#. Implement activities and workflows. The workflow implementation provides the
   business logic, while each activity implements a single logical processing
   step in the application. The workflow implementation calls the activities.
#. Implement host programs for activity and workflow implementations. After you
   have implemented your workflow and activities, you need to create host
   programs. A host program is responsible for getting tasks from Amazon SWF
   and dispatching them to the appropriate implementation method. AWS Flow
   Framework provides worker classes that make implementing these host programs
   trivial.
#. Test your workflow. TODO: AWS Flow Framework in Python does not yet provide nice
   testing facilities
#. Deploy the workers. You can now deploy your workers as desired - for
   example, you can deploy them to instances in the cloud or in your own data
   centers. Once deployed, the workers start polling Amazon SWF for tasks.
#. Start executions. You can start an execution of your workflow from any
   program using the workflow definition. You can also use the Amazon SWF
   console to start and view workflow executions in your Amazon SWF account.
