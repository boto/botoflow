from datetime import datetime

from mock import patch, MagicMock, call

from botoflow.decider import decider
from botoflow.workers import GenericWorkflowWorker
from botoflow.decider.decision_task_poller import DecisionTaskPoller, DecisionTask
from botoflow.history_events.events import (
    WorkflowExecutionStarted, WorkflowExecutionCompleted, DecisionTaskScheduled, DecisionTaskStarted,
    DecisionTaskCompleted, StartChildWorkflowExecutionInitiated, ChildWorkflowExecutionStarted,
    ChildWorkflowExecutionCompleted, ChildWorkflowExecutionFailed)


@patch.object(decider, 'get_context')
@patch.object(decider, 'set_context')
def test_decide_reordered(m_old_context, m_context):
    date = datetime(1975, 5, 25)
    id_range = iter(range(1, 100))

    events = [WorkflowExecutionStarted(next(id_range), date, {}),
              DecisionTaskScheduled(next(id_range), date, {}),
              DecisionTaskStarted(next(id_range), date, {}),
              DecisionTaskCompleted(next(id_range), date, {}),
              StartChildWorkflowExecutionInitiated(next(id_range), date, {}),
              ChildWorkflowExecutionStarted(next(id_range), date, {}),
              DecisionTaskScheduled(next(id_range), date, {}),
              DecisionTaskCompleted(next(id_range), date, {}),
              StartChildWorkflowExecutionInitiated(next(id_range), date, {}),
              DecisionTaskStarted(next(id_range), date, {}),
              ChildWorkflowExecutionStarted(next(id_range), date, {}),
              ChildWorkflowExecutionCompleted(next(id_range), date, {}),
              DecisionTaskScheduled(next(id_range), date, {}),
              DecisionTaskCompleted(next(id_range), date, {}),
              ChildWorkflowExecutionFailed(next(id_range), date, {}),
              DecisionTaskStarted(next(id_range), date, {}),
              ChildWorkflowExecutionCompleted(next(id_range), date, {},),  # must go after StartChildWorkflow...
              ChildWorkflowExecutionCompleted(next(id_range), date, {}),  # this one too
              DecisionTaskCompleted(next(id_range), date, {}),
              StartChildWorkflowExecutionInitiated(next(id_range), date, {}),
              DecisionTaskStarted(next(id_range), date, {}),
              DecisionTaskCompleted(next(id_range), date, {}),
              WorkflowExecutionCompleted(next(id_range), date, {})]

    m_worker = MagicMock(spec=GenericWorkflowWorker)
    m_poller = MagicMock(spec=DecisionTaskPoller)
    m_decision_task = MagicMock(spec=DecisionTask, workflow_id='unit-wfid', run_id='unit-runid', task_token='unit-tt',
                                previous_started_event_id=1, events=iter(events))
    m_poller().poll.return_value = m_decision_task

    m_handle_history_event = MagicMock(spec=decider.Decider._handle_history_event)

    decider_inst = decider.Decider(m_worker, 'unit-domain', 'unit-tlist', MagicMock(),
                                   'unit-id', _Poller=m_poller)
    decider_inst._process_decisions = MagicMock(spec=decider.Decider._process_decisions)
    decider_inst._handle_history_event = m_handle_history_event

    decider_inst.decide()

    assert m_handle_history_event.mock_calls == [call(events[0]), call(events[4]), call(events[5]), call(events[8]),
                                                 call(events[10]), call(events[11]), call(events[14]),
                                                 call(events[19]), call(events[16]), call(events[17])]
