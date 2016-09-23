import botocore
import configparser
import os.path
import sys
from botoflow import activity, activities, execute, workflow_starter, WorkflowDefinition
from botoflow.workers import workflow_worker, activity_worker
from botoflow.constants import MINUTES


@activities(schedule_to_start_timeout=1*MINUTES,
            start_to_close_timeout=1*MINUTES)
class HelloWorldActivities(object):

    @activity('1.0')
    def get_name(self):
        return input('Whats your name: ')

    @activity('1.0')
    def print_greeting(self, name):
        print("Hello {}!".format(name))


class HelloWorldWorkflow(WorkflowDefinition):

    @execute(version='1.0', execution_start_to_close_timeout=1*MINUTES)
    def hello_world(self):
        name = yield HelloWorldActivities.get_name()
        yield HelloWorldActivities.print_greeting(name)


def main(config_filename='config.ini'):
    config = configparser.ConfigParser()
    if os.path.isfile(config_filename):
        config.read(config_filename)
    else:
        print("Cannot file config file: {}".format(config_filename))
        sys.exit(1)

    PROFILE = config.get('default', 'profile', fallback=None)
    REGION = config.get('default', 'region', fallback='us-east-1')
    DOMAIN = config.get('default', 'domain', fallback=None)
    TASKLIST = config.get('default', 'tasklist', fallback=None)

    if not DOMAIN or not TASKLIST:
        print("You must define a domain and tasklist in config.ini")
        sys.exit(1)

    if PROFILE:
        session = botocore.session.Session(profile=PROFILE)
    else:
        session = botocore.session.get_session()

    # Create decider
    # Registers workflowtype
    decider = workflow_worker.WorkflowWorker(session,
                                             REGION,
                                             DOMAIN,
                                             TASKLIST,
                                             HelloWorldWorkflow)

    # Create worker
    # Registers activities
    worker = activity_worker.ActivityWorker(session,
                                            REGION,
                                            DOMAIN,
                                            TASKLIST,
                                            HelloWorldActivities())

    # Now that all workflow and activies are registered initialize the
    # workflow
    with workflow_starter(session,
                          REGION,
                          DOMAIN,
                          TASKLIST):
        print("Workflow started")
        HelloWorldWorkflow.hello_world()  # starts the workflow

    print("Fire decider")
    decider.run_once()

    print("Fire worker")
    worker.run_once()

    print("Fire decider again")
    decider.run_once()

    print("Fire worker again")
    worker.run_once()

    print("Fire decider to complete workflow")
    decider.run_once()


if __name__ == '__main__':
    main()
