# Copyright 2016 Darjus Loktevic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import copy

from ..data_converter import JSONDataConverter
from ..constants import USE_WORKER_TASK_LIST
from ..utils import str_or_NONE
from ..context import get_context, DecisionContext

from .base_flow_type import BaseFlowType


class ActivityType(BaseFlowType):

    DEFAULT_DATA_CONVERTER = JSONDataConverter()

    def __init__(self,
                 version,
                 name=None,
                 task_list=USE_WORKER_TASK_LIST,
                 heartbeat_timeout=None,
                 schedule_to_start_timeout=None,
                 start_to_close_timeout=None,
                 schedule_to_close_timeout=None,
                 description=None,
                 data_converter=None,
                 skip_registration=False,
                 manual=False):

        self.version = version
        self.name = name
        self.task_list = task_list
        self.heartbeat_timeout = heartbeat_timeout
        self.schedule_to_start_timeout = schedule_to_start_timeout
        self.start_to_close_timeout = start_to_close_timeout
        self.schedule_to_close_timeout = schedule_to_close_timeout
        self.description = description
        self.skip_registration = skip_registration
        self.manual = manual

        # retrying will be set by ActivityFunc, as it's a separate decorator
        # and we want to not care about the decorator order
        self.retrying = None

        if data_converter is None:
            self.data_converter = self.DEFAULT_DATA_CONVERTER
        else:
            self.data_converter = data_converter

    def __getstate__(self):
        """Prepare the activity type for serialization as it's included in ActivityTaskFailed exception, and it
        may be passed around by our clients for exception handling.

        But only serialize basic information about the activity, not to bloat serialized object

        :return: A serializable dict
        :rtype: dict
        """
        dct = copy(self.__dict__)
        if 'retrying' in dct:
            del dct['retrying']

        if 'data_converter' in dct:
            del dct['data_converter']

        if 'description' in dct:
            del dct['description']

        if 'skip_registration' in dct:
            del dct['skip_registration']

        return dct

    def __setstate__(self, dct):
        """Recreate our activity type based on the dct
        :param dct: a deserialized dictionary to recreate our state
        :type dct: dict
        """
        self.__dict__ = dct

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__getstate__() == other.__getstate__()
        return False

    def _set_activities_value(self, key, value):
        if getattr(self, key) is None:
            setattr(self, key, value)

    def _reset_name(self, cls, func, activity_name_prefix):
        # generate activity name
        _name = "%s%s" % (activity_name_prefix, func.__name__)
        if self.name is None:
            _name = "%s.%s" % (cls.__name__, func.__name__)

        else:
            _name = "%s%s" % (activity_name_prefix, self.name)
        self.name = _name

    def to_decision_dict(self):
        decision_dict = {
            'activity_type_version': self.version,
            'activity_type_name': self.name,
            'task_list': {'name': str_or_NONE(self.task_list)},
            'heartbeat_timeout': str_or_NONE(self.heartbeat_timeout),
            'schedule_to_start_timeout': str_or_NONE(
                self.schedule_to_start_timeout),
            'start_to_close_timeout': str_or_NONE(self.start_to_close_timeout),
            'schedule_to_close_timeout': str_or_NONE(
                self.schedule_to_close_timeout),
        }
        return decision_dict

    def to_registration_options_dict(self, domain, worker_task_list):
        if self.skip_registration:
            return None

        task_list = self.task_list
        if task_list == USE_WORKER_TASK_LIST:
            task_list = worker_task_list

        registration_options = {
            'domain': domain,
            'version': self.version,
            'name': self.name,
            'defaultTaskList': {'name': str_or_NONE(task_list)},
            'defaultTaskHeartbeatTimeout': str_or_NONE(
                self.heartbeat_timeout),
            'defaultTaskScheduleToStartTimeout': str_or_NONE(
                self.schedule_to_start_timeout),
            'defaultTaskStartToCloseTimeout': str_or_NONE(
                self.start_to_close_timeout),
            'defaultTaskScheduleToCloseTimeout': str_or_NONE(
                self.schedule_to_close_timeout),
            'description': str_or_NONE(self.description)
        }
        return registration_options

    def __call__(self, *args, **kwargs):
        """
        You can call this directly to support dynamic activities.
        """
        context = None
        try:
            context = get_context()
        except AttributeError:  # not in context
            pass

        if not isinstance(context, DecisionContext):
            raise TypeError("ActivityType can only be called in the decision "
                            "context")

        decision_dict = self.to_decision_dict()

        # apply any options overrides
        _decision_dict = {}
        _decision_dict.update(decision_dict)
        _decision_dict.update(context._activity_options_overrides.items())

        if self.retrying is not None:
            return self.retrying.call(context.decider._handle_execute_activity,
                                      self, _decision_dict, args, kwargs)

        return context.decider._handle_execute_activity(
            self, _decision_dict, args, kwargs)
