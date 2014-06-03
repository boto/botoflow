from awsflow import *


@activities(schedule_to_start_timeout=60,
            start_to_close_timeout=60)
class BunchOfActivities(object):
    @activity(version='1.1')
    def sum(self, x, y):
        return x + y

    @activity(version='1.4',
              schedule_to_close_timeout=60*2)
    def mul(self, x, y):
        return x * y

    @activity('1.1')
    def throw(self):
        raise ValueError("Hello-Error")


@activities(schedule_to_start_timeout=60,
            start_to_close_timeout=60)
class ManualActivities(object):
    @manual_activity(version='1.0')
    def perform_task(self, **kwargs):
        activity_context = context.get_context()
        task_token = activity_context.task.token

        with open('task_token.txt', 'w') as shared_file:
            shared_file.write(task_token)
