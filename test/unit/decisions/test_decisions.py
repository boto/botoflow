from botoflow.decisions import decisions


def test_cancel_workflow_execution():
    decision = decisions.CancelWorkflowExecution(details="Blah").decision
    assert decision['decisionType'] == 'CancelWorkflowExecution'
    assert decision['cancelWorkflowExecutionDecisionAttributes']['details'] == "Blah"

    decision = decisions.CancelWorkflowExecution().decision
    assert 'details' not in decision['cancelWorkflowExecutionDecisionAttributes']


def test_cancel_timer():
    decision = decisions.CancelTimer("timer_id").decision
    assert decision['decisionType'] == 'CancelTimer'
    assert decision['cancelTimerDecisionAttributes']['timerId'] == "timer_id"


def test_complete_workflow_execution():
    decision = decisions.CompleteWorkflowExecution("result").decision
    assert decision['decisionType'] == 'CompleteWorkflowExecution'
    assert decision['completeWorkflowExecutionDecisionAttributes']['result'] == "result"

    decision = decisions.CompleteWorkflowExecution().decision
    assert 'result' not in decision['completeWorkflowExecutionDecisionAttributes']


def test_continue_as_new_workflow_execution():
    decision = decisions.ContinueAsNewWorkflowExecution(
        child_policy="child_policy", execution_start_to_close_timeout="5", input="input", tag_list=['tag', 'list'],
        task_list="task_list", task_start_to_close_timeout="3", version="1.0", task_priority='100').decision

    assert decision['decisionType'] == 'ContinueAsNewWorkflowExecution'
    assert decision['continueAsNewWorkflowExecutionDecisionAttributes'] == {
        'childPolicy': 'child_policy', 'executionStartToCloseTimeout': '5', 'input': 'input',
        'tagList': ['tag', 'list'], 'taskList': 'task_list', 'taskStartToCloseTimeout': '3',
        'workflowTypeVersion': '1.0', 'taskPriority': '100'}

    decision = decisions.ContinueAsNewWorkflowExecution().decision
    assert 'childPolicy' not in decision['continueAsNewWorkflowExecutionDecisionAttributes']


def test_fail_workflow_execution():
    decision = decisions.FailWorkflowExecution(reason="reason", details="details").decision
    assert decision['decisionType'] == 'FailWorkflowExecution'
    assert decision['failWorkflowExecutionDecisionAttributes'] == {'reason': 'reason', 'details': 'details'}

    decision = decisions.FailWorkflowExecution().decision
    assert 'reason' not in decision['failWorkflowExecutionDecisionAttributes']


def test_record_marker():
    decision = decisions.RecordMarker(marker_name="marker_name", details="details").decision
    assert decision['decisionType'] == 'RecordMarker'
    assert decision['recordMarkerDecisionAttributes'] == {'markerName': 'marker_name', 'details': 'details'}

    decision = decisions.RecordMarker(marker_name="marker_name").decision
    assert 'details' not in decision['recordMarkerDecisionAttributes']


def test_request_cancel_activity():
    decision = decisions.RequestCancelActivityTask("activity_id").decision
    assert decision['decisionType'] == 'RequestCancelActivityTask'
    assert decision['requestCancelActivityTaskDecisionAttributes'] == {'activityId': 'activity_id'}


def test_request_cancel_external_workflow_execution():
    decision = decisions.RequestCancelExternalWorkflowExecution(workflow_id="workflow_id", run_id="run_id",
                                                                control="control").decision
    assert decision['decisionType'] == 'RequestCancelExternalWorkflowExecution'
    assert decision['requestCancelExternalWorkflowExecutionDecisionAttributes'] == {
        'workflowId': 'workflow_id', 'runId': 'run_id', 'control': 'control'}

    decision = decisions.RequestCancelExternalWorkflowExecution(workflow_id="workflow_id", run_id="run_id").decision
    assert 'control' not in decision['requestCancelExternalWorkflowExecutionDecisionAttributes']


def test_schedule_activity_tasl():
    decision = decisions.ScheduleActivityTask(
        activity_id="activity_id", activity_type_name="name", activity_type_version="1.0",
        task_list="task_list", control="control", heartbeat_timeout="3",
        schedule_to_close_timeout="4",
        schedule_to_start_timeout="5", start_to_close_timeout="6",
        task_priority="100",
        input="input").decision

    assert decision['decisionType'] == 'ScheduleActivityTask'
    assert decision['scheduleActivityTaskDecisionAttributes'] == {
        'activityId': 'activity_id', 'activityType': {'name': 'name', 'version': '1.0'}, 'taskList': 'task_list',
        'control': 'control', 'heartbeatTimeout': '3', 'scheduleToCloseTimeout': '4', 'scheduleToStartTimeout': '5',
        'startToCloseTimeout': '6', 'taskPriority': '100', 'input': 'input'}

    decision = decisions.ScheduleActivityTask(
        activity_id="activity_id", activity_type_name="name", activity_type_version="1.0").decision
    assert 'taskList' not in decision['scheduleActivityTaskDecisionAttributes']


def test_signal_external_workflow_execution():
    decision = decisions.SignalExternalWorkflowExecution(workflow_id="workflow_id", run_id="run_id",
                                                         signal_name='signal_name', control="control",
                                                         input='input').decision
    assert decision['decisionType'] == 'SignalExternalWorkflowExecution'
    assert decision['signalExternalEorkflowExecutionDecisionAttributes'] == {
        'workflowId': 'workflow_id', 'runId': 'run_id', 'signalName': 'signal_name', 'control': 'control',
        'input': 'input'}

    decision = decisions.SignalExternalWorkflowExecution(workflow_id="workflow_id", run_id="run_id",
                                                         signal_name='signal_name').decision
    assert 'control' not in decision['signalExternalEorkflowExecutionDecisionAttributes']

    # special for run_id as it can be None but is required
    decision = decisions.SignalExternalWorkflowExecution(workflow_id="workflow_id", run_id=None,
                                                         signal_name='signal_name').decision
    assert 'runId' not in decision['signalExternalEorkflowExecutionDecisionAttributes']


def test_start_child_workflow_execution():
    decision = decisions.StartChildWorkflowExecution(
        workflow_type={'name': 'name', 'version': '1.0'}, workflow_id='workflow_id',
        child_policy="child_policy", control='control', execution_start_to_close_timeout="5", input="input",
        tag_list=['tag', 'list'], task_list="task_list", task_start_to_close_timeout="3", task_priority='100').decision

    assert decision['decisionType'] == 'StartChildWorkflowExecution'
    assert decision['startChildWorkflowExecutionDecisionAttributes'] == {
        'workflowType': {'name': 'name', 'version': '1.0'}, 'workflowId': 'workflow_id', 'childPolicy': 'child_policy',
        'control': 'control', 'executionStartToCloseTimeout': '5', 'input': 'input',
        'tagList': ['tag', 'list'], 'taskList': 'task_list', 'taskStartToCloseTimeout': '3', 'taskPriority': '100'}

    decision = decisions.StartChildWorkflowExecution(
        workflow_type={'name': 'name', 'version': '1.0'}, workflow_id='workflow_id').decision
    assert 'childPolicy' not in decision['startChildWorkflowExecutionDecisionAttributes']


def test_start_timer():
    decision = decisions.StartTimer(timer_id="timer_id", start_to_fire_timeout='4', control="control").decision
    assert decision['decisionType'] == 'StartTimer'
    assert decision['startTimerDecisionAttributes'] == {'timerId': 'timer_id', 'startToFireTimeout': '4',
                                                        'control': 'control'}

    decision = decisions.StartTimer(timer_id="timer_id", start_to_fire_timeout='4').decision
    assert 'control' not in decision['startTimerDecisionAttributes']
