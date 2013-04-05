# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import sys
import traceback
import time
import functools
import copy
import logging

import six

from ..swf_exceptions import TypeAlreadyExistsError

from ..context import ActivityContext, get_context, set_context

from .base_worker import BaseWorker
from .activity_task import ActivityTask

log = logging.getLogger(__name__)


class ActivityWorker(BaseWorker):
    """For implementing activity workers, you can use the ActivityWorker class
    to conveniently poll a task list for activity tasks.

    You configure the activity worker with activity implementation
    objects. This worker class runs a loop to poll for activity tasks in the
    specified task list. When an activity task is received, it looks up the
    appropriate implementation that you provided and calls the activity method
    to process the task. **Unlike the** :py:class:`~.WorkflowWorker`, which
    calls the @execute decorated method (factory method) to create a new
    instance for every decision task, the :py:class:`~.ActivityWorker` simply
    uses the `object` you provided.

    The ActivityWorker class uses the AWS Flow Framework decorators to
    determine the registration and execution options.

    :param endpoint: botocore Endpoint object.
    :type endpoint: botocore.endpoint.endpoint
    :param str domain: SWF domain to operate on.
    :param str task_list: default task list on which to put all the workflow
        requests.
    :param object activities: Can either be a single, or a set of activities
        objects.

    Here's an example for setting up an ActivityWorker::

        # instantiate a list of activites we want to serve on this worker
        activities = [SomeActivities(), OtherActivities()]

        # create the worker object
        activity_worker = ActivityWorker(session, "SOMEDOMAIN",
                                         "MYTASKLIST", *activities_obj)
        activity_worker.run()
    """

    def __init__(self, endpoint, domain, task_list,
                 *activity_definitions):

        super(ActivityWorker, self).__init__(endpoint, domain, task_list)

        self._register_activity_type_op = self._build_swf_op(
            "RegisterActivityType")
        self._poll_for_activity_task_op = self._build_swf_op(
            "PollForActivityTask")
        self._respond_activity_task_completed_op = self._build_swf_op(
            "RespondActivityTaskCompleted")
        self._respond_activity_task_failed_op = self._build_swf_op(
            "RespondActivityTaskFailed")

        self._activity_definitions = activity_definitions
        self._setup_activities()
        self._register_activities()

    def __getstate__(self):
        newdict = copy.copy(self.__dict__)
        del newdict['_activity_names_to_methods']
        return newdict

    def __setstate__(self, newdict):
        self.__dict__ = newdict
        self._setup_activities()

    def _setup_activities(self):
        # this dict represents a table of activity names that point to the
        # methods
        self._activity_names_to_methods = dict()

        for activity in self._activity_definitions:
            # extract activities info from the class
            for name in dir(activity):
                try:
                    func = getattr(activity, name)
                except AttributeError:
                    continue

                # this is a bit tricky, but what it essentially does, it fishes for
                # methods that are activities and maps them
                if isinstance(func, functools.partial):
                    if hasattr(func, 'swf_options'):
                        if 'activity_type' in func.swf_options:
                            activity_type = func.swf_options['activity_type']
                            self._activity_names_to_methods[activity_type.name] \
                                = (func, activity_type)

    def _register_activities(self):
        """
        Registers the activities with SWF
        """
        for act_name, func_info in \
            six.iteritems(self._activity_names_to_methods):
            activity_type = func_info[1]

            if activity_type.skip_registration:
                log.debug("Skipping workflow '%s %s' registration",
                          activity_type.name, activity_type.version)

            kwargs = activity_type.to_registration_options_dict(
                domain=self.domain, worker_task_list=self.task_list)

            try:
                log.debug("Registering activity with the following "
                          "options: %s", kwargs)
                self._register_activity_type_op(**kwargs)
            except TypeAlreadyExistsError:
                log.debug("Activity '%s %s' already registered",
                          activity_type.name, activity_type.version)

    def _poll_for_activities(self):
        """
        Returns a closure function ready for execution
        """
        poll_time = time.time()
        try:
            task_dict = self._poll_for_activity_task_op(
                domain=self.domain, task_list={'name':self.task_list},
                identity=self.identity)
            if task_dict['startedEventId'] == 0:
                return

            task = ActivityTask(task_dict)

        except KeyboardInterrupt:
            # seep before actually exiting as the connection is not yet closed
            # on the other end
            sleep_time = 60 - (time.time() - poll_time)
            six.print_("Exiting in {0}...".format(sleep_time), file=sys.stderr)
            time.sleep(sleep_time)
            raise

        func, activity_type = self._activity_names_to_methods[task.name]

        def process_activity():
            saved_context = None
            try:
                saved_context = get_context()
            except AttributeError:
                pass

            context = ActivityContext(self, task)
            set_context(context)
            try:
                fargs, kwargs = activity_type.data_converter.loads(task.input)

                # make sure kwargs are non-unicode in 2.6
                if sys.version_info[0:2] == (2, 6):
                    kwargs = dict([(str(k), v) \
                                   for k, v in six.iteritems(kwargs)])

                try:
                    log.debug("Running activity with args: %r, "
                              "kwargs: %r", fargs, kwargs)
                    result = func(*fargs, **kwargs)
                    log.debug("Activity returned: %r", result)
                    self._respond_activity_task_completed_op(
                        task_token=task.token,
                        result=activity_type.data_converter.dumps(result))
                except Exception as err:
                    _, _, tb = sys.exc_info()
                    tb_list = traceback.extract_tb(tb)

                    log.debug("Activity raised an exception: %s, %s",
                              err, tb_list)
                    # the [1:] slices out the framework part so that it looks
                    # like the code ran alone
                    details = activity_type.data_converter.dumps(
                        [err, tb_list[1:]])
                    self._respond_activity_task_failed_op(
                        task_token=task.token, reason='', details=details)

            finally:
                set_context(saved_context)

        return process_activity

    def run(self):
        """Run this worker forever (or till SIGINT).
        """
        while True:
            self.run_once()

    def run_once(self):
        """Run this worker once (perform one decision loop).
        """
        process_activity = self._poll_for_activities()
        if process_activity is not None:
            process_activity()
