# -*- mode:python ; fill-column:120 -*-
import logging
import time
import unittest

from calendar import timegm

from awsflow import (WorkflowDefinition, execute, return_, async, activity, ThreadedWorkflowExecutor,
                      ThreadedActivityExecutor, WorkflowWorker, ActivityWorker, activity_options, workflow_time,
                      workflow_types, logging_filters, WorkflowStarter, workflow)

from awsflow.exceptions import ActivityTaskFailedError, WorkflowFailedError
from utils import SWFMixIn
from various_activities import BunchOfActivities


class TestSimpleWorkflows(SWFMixIn, unittest.TestCase):

    def test_no_activities(self):
        class NoActivitiesWorkflow(WorkflowDefinition):
            @execute(version='1.2', execution_start_to_close_timeout=60)
            def execute(self, arg1):
                return_(arg1)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list) as starter:
            instance = NoActivitiesWorkflow.execute(arg1="TestExecution")
            self.workflow_execution = instance.workflow_execution

        # start + stop should run the worker's Decider once
        worker = ThreadedWorkflowExecutor(WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            NoActivitiesWorkflow))
        worker.start()
        worker.stop()
        worker.join()
        time.sleep(2)

        self.assertEqual("TestExecution", starter.wait_for_completion(instance, 1))

        hist = self.get_workflow_execution_history()

        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 'TestExecution')

    def test_no_activities_failure(self):
        class NoActivitiesFailureWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1):
                raise RuntimeError("ExecutionFailed")

        worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, NoActivitiesFailureWorkflow)
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list) as starter:
            instance = NoActivitiesFailureWorkflow.execute(arg1="TestExecution")
            self.workflow_execution = instance.workflow_execution

        worker.run_once()
        time.sleep(1)

        try:
            starter.wait_for_completion(instance, 1)
        except WorkflowFailedError as err:
            self.assertEqual(RuntimeError, type(err.cause))
        else:
            self.fail("Should never succeed")

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionFailed')
        self.assertEqual(str(self.serializer.loads(
            hist[-1]['workflowExecutionFailedEventAttributes']['details'])[0]),
                         "ExecutionFailed")

    def test_no_activities_with_state(self):
        class NoActivitiesWorkflow(WorkflowDefinition):
            @execute(version='1.2', execution_start_to_close_timeout=60)
            def execute(self, arg1):
                self.workflow_state = "Workflow Started"
                return_(arg1)

        worker = ThreadedWorkflowExecutor(WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, NoActivitiesWorkflow))
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = NoActivitiesWorkflow.execute(arg1="TestExecution")
            self.workflow_execution = instance.workflow_execution

        # start + stop should run the worker's Decider once
        worker.start()
        worker.stop()
        worker.join()
        time.sleep(2)

        hist = self.get_workflow_execution_history()

        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(
            hist[-2]['decisionTaskCompletedEventAttributes']['executionContext'],
            'Workflow Started')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 'TestExecution')

    def test_one_activity(self):
        class OneActivityWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                arg_sum = yield self.activities_client.sum(arg1, arg2)
                return_(arg_sum)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, OneActivityWorkflow)

        act_worker = ThreadedActivityExecutor(ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities()))

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.start(1, 4)
        act_worker.stop()
        wf_worker.run_once()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 3)

    def test_one_activity_timed(self):

        class OneActivityTimedWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                mytime = workflow_time.time()
                yield BunchOfActivities.sum(arg1, arg2)
                return_([timegm(mytime.timetuple()),
                         timegm(workflow_time.time().timetuple())])

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, OneActivityTimedWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityTimedWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']),
                         [timegm(hist[2]['eventTimestamp'].timetuple()),
                          timegm(hist[8]['eventTimestamp'].timetuple())])

    def test_one_activity_dynamic(self):
        class OneActivityTimedWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                # create an activity call dynamically
                sum = workflow_types.ActivityType('1.1', name='BunchOfActivities.sum')
                arg_sum = yield sum(arg1, arg2)
                return_(arg_sum)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, OneActivityTimedWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityTimedWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 3)

    def test_one_activity_options_overrides(self):
        class OneActivityWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                with activity_options(start_to_close_timeout=66):
                    arg_sum = yield BunchOfActivities.sum(arg1, arg2)
                return_(arg_sum)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, OneActivityWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[4]['activityTaskScheduledEventAttributes']['startToCloseTimeout'], '66')

    def test_one_activity_with_timer(self):
        class OneActivityWithTimerWorkflow(WorkflowDefinition):
            def __init__(self, workflow_execution):
                super(OneActivityWithTimerWorkflow, self).__init__(workflow_execution)
                self.activities_client = BunchOfActivities()

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                yield workflow_time.sleep(2)
                arg_sum = yield self.activities_client.sum(arg1, arg2)
                return_(arg_sum)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, OneActivityWithTimerWorkflow)

        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityWithTimerWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 16)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')

        # timer specific checks
        self.assertEqual(hist[4]['eventType'], 'TimerStarted')
        self.assertEqual(hist[4]['timerStartedEventAttributes']['startToFireTimeout'], '2')
        self.assertEqual(hist[5]['eventType'], 'TimerFired')

    def test_one_activity_default_task_list(self):
        class OneActivityCustomTaskList(object):

            @activity(version='1.1', task_list='abracadabra')
            def sum(self, x, y):
                return x + y

        class OneActivityDefaultTaskListWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                arg_sum = yield OneActivityCustomTaskList.sum(arg1, arg2)
                return_(arg_sum)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list,
            OneActivityDefaultTaskListWorkflow)

        act_worker = ThreadedActivityExecutor(ActivityWorker(
            self.session, self.region, self.domain, 'abracadabra',
            OneActivityCustomTaskList()))

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = OneActivityDefaultTaskListWorkflow.execute(
                arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.start(1, 4)
        act_worker.stop()
        wf_worker.run_once()
        act_worker.join()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 11)
        self.assertEqual(hist[4]['activityTaskScheduledEventAttributes']
                         ['taskList']['name'], 'abracadabra')
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 3)

    def test_try_except_finally_activity(self):
        class TryExceptFinallyWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                @async
                def do_try_except():
                    arg_sum = 0
                    try:
                        arg_sum += yield BunchOfActivities.sum(arg1, arg2)
                        yield BunchOfActivities.throw()
                    except ActivityTaskFailedError as err:
                        if isinstance(err.cause, ValueError) \
                           and str(err.cause) == 'Hello-Error':

                            if err.event_id != 13 or err.activity_id != '2':
                                raise RuntimeError("Test Failed")
                            arg_sum += yield BunchOfActivities.sum(arg1, arg2)
                    finally:
                        arg_sum += yield BunchOfActivities.sum(arg1, arg2)
                        return_(arg_sum)

                result = yield do_try_except()
                return_(result)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, TryExceptFinallyWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = TryExceptFinallyWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        for i in range(4):
            wf_worker.run_once()
            act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 29)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 9)

    def test_try_except_with_timer(self):
        class TryExceptFinallyWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                @async
                def do_try_except():
                    arg_sum = 0
                    try:
                        arg_sum += yield BunchOfActivities.sum(arg1, arg2)
                        yield BunchOfActivities.throw()
                    except ActivityTaskFailedError as err:
                        if isinstance(err.cause, ValueError) \
                           and str(err.cause) == 'Hello-Error':

                            if err.event_id != 13 or err.activity_id != '2':
                                raise RuntimeError("Test Failed")
                            arg_sum += yield BunchOfActivities.sum(arg1, arg2)
                    yield workflow_time.sleep(1)
                    return_(arg_sum)

                result = yield do_try_except()
                return_(result)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, TryExceptFinallyWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = TryExceptFinallyWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        for i in range(3):
            wf_worker.run_once()
            act_worker.run_once()

        # Once for the timer
        wf_worker.run_once()
        # Once for the completion
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 28)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 6)


    def test_two_activities(self):
        class BunchOfActivitiesWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                arg_sum = yield BunchOfActivities.sum(arg1, arg2)
                arg_mul = yield BunchOfActivities.mul(arg1, arg2)
                return_((arg_sum, arg_mul))

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivitiesWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = BunchOfActivitiesWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 17)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), (3, 2))

    def test_next_page_token_activities(self):
        # process over a hundred events, so that we're clear we can work with nextPageToken
        class NextPageTokenWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, repeat, arg1):
                for i in range(repeat):
                    yield BunchOfActivities.sum(i, arg1)
                return_(repeat)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, NextPageTokenWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = NextPageTokenWorkflow.execute(repeat=21, arg1=1)
            self.workflow_execution = instance.workflow_execution

        for i in range(21):
            wf_worker.run_once()
            act_worker.run_once()

        wf_worker.run_once() # finish off
        time.sleep(1)

        hist, token = self.get_workflow_execution_history_with_token()
        events = hist
        hist = self.get_workflow_execution_history(next_page_token=token)

        events.extend(hist)
        self.assertEqual(len(events), 131)
        self.assertEqual(events[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            events[-1]['workflowExecutionCompletedEventAttributes']['result']), 21)

    def test_all_future_activities(self):
        class AllFutureWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                sum_future = BunchOfActivities.sum(arg1, arg2)
                mul_future = BunchOfActivities.mul(arg1, arg2)
                result = yield sum_future, mul_future
                return_(result)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, AllFutureWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, BunchOfActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = AllFutureWorkflow.execute(arg1=1, arg2=2)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 17)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), (3, 2))


    def test_any_future_activities(self):

        class SleepingActivities(object):
            @activity(version='1.2',
                      schedule_to_start_timeout=60,
                      start_to_close_timeout=60)
            def sleep(self, time_to_sleep):
                time.sleep(time_to_sleep)
                return time_to_sleep

        class AnyFutureWorkflow(WorkflowDefinition):

            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1, arg2):
                sleep1_future = SleepingActivities.sleep(arg1)
                sleep2_future = SleepingActivities.sleep(arg2)
                result = yield sleep1_future | sleep2_future
                return_(result)

        wf_worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, AnyFutureWorkflow)
        act_worker = ActivityWorker(
            self.session, self.region, self.domain, self.task_list, SleepingActivities())

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = AnyFutureWorkflow.execute(arg1=5, arg2=1)
            self.workflow_execution = instance.workflow_execution

        wf_worker.run_once()
        act_worker.run_once()
        act_worker.run_once()
        wf_worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 14)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertTrue(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']))

    def test_workflow_continue_as_new(self):
        class NoActivitiesWorkflow(WorkflowDefinition):
            @execute(version='1.1', execution_start_to_close_timeout=60)
            def execute(self, arg1):
                if arg1 > 0:
                    arg1 -= 1
                    self.execute(arg1)
                else:
                    return "TestExecution"

        worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, NoActivitiesWorkflow)
        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = NoActivitiesWorkflow.execute(arg1=1)
            self.workflow_execution = instance.workflow_execution

        for i in range(2):
            worker.run_once()
        time.sleep(1)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionContinuedAsNew')
        new_run_id = hist[-1]['workflowExecutionContinuedAsNewEventAttributes']['newExecutionRunId']

        hist = self.get_workflow_execution_history(run_id=new_run_id)

        self.assertEqual(len(hist), 5)
        self.assertEqual(hist[-1]['eventType'], 'WorkflowExecutionCompleted')
        self.assertEqual(self.serializer.loads(
            hist[-1]['workflowExecutionCompletedEventAttributes']['result']), 'TestExecution')

    def test_subclassed_workflow(self):
        class SuperClassWorkflow(WorkflowDefinition):
            @execute(version='1.0', execution_start_to_close_timeout=60)
            def execute(self):
                pass

        class SubClassWorkflow(SuperClassWorkflow):
            @execute(version='1.0', execution_start_to_close_timeout=60)
            def execute(self):
                pass

        worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, SubClassWorkflow)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SubClassWorkflow.execute()
            self.workflow_execution = instance.workflow_execution

        worker.run_once()
        time.sleep(2)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)

    def test_subclassed_workflow_no_exec(self):
        class SuperClassWorkflow(WorkflowDefinition):
            @execute(version='1.0', execution_start_to_close_timeout=60)
            def execute(self):
                pass

        class SubClassWorkflow(SuperClassWorkflow):
            pass

        worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, SubClassWorkflow)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SubClassWorkflow.execute()
            self.workflow_execution = instance.workflow_execution

        worker.run_once()
        time.sleep(2)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)

    def test_subclassed_workflow_multiver(self):
        class MultiverWorkflow(WorkflowDefinition):
            @execute(version='1.0', execution_start_to_close_timeout=60)
            def start_wf(self):
                pass

        @workflow(name='MultiverWorkflow')
        class SubMultiverWorkflow(MultiverWorkflow):
            @execute(version='1.1', execution_start_to_close_timeout=60)
            def start_wf(self):
                pass

            @execute(version='1.2', execution_start_to_close_timeout=60)
            def start_wf_v2(self):
                pass


        worker = WorkflowWorker(
            self.session, self.region, self.domain, self.task_list, SubMultiverWorkflow)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SubMultiverWorkflow.start_wf()
            self.workflow_execution = instance.workflow_execution

        worker.run_once()
        time.sleep(2)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)

        with WorkflowStarter(self.session, self.region, self.domain, self.task_list):
            instance = SubMultiverWorkflow.start_wf_v2()
            self.workflow_execution = instance.workflow_execution

        worker.run_once()
        time.sleep(2)

        hist = self.get_workflow_execution_history()
        self.assertEqual(len(hist), 5)
        self.assertEqual({'name': 'MultiverWorkflow', 'version': '1.2'},
                         hist[0]
                         ['workflowExecutionStartedEventAttributes']
                         ['workflowType'])


if __name__ == '__main__':
    unittest.main()
