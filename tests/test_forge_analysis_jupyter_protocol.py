"""Protocol-only tests; no real kernel process or isolation claim."""
import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.jupyter_protocol import ExecutionOutput
from demiflow.forge.schema import ForgeError


def message(kind, content, *, parent="msg-one"):
    return dict(header={"msg_type": kind}, parent_header={"msg_id": parent}, content=content)


def send(state, kind, content, *, channel="iopub", generation="generation-one", parent="msg-one"):
    return state.consume(channel, message(kind, content, parent=parent), generation=generation)


@pytest.mark.parametrize("reply_first", [True, False])
def test_completion_needs_matching_reply_and_idle_in_either_order(reply_first):
    state = ExecutionOutput(generation="generation-one", msg_id="msg-one")
    send(state, "stream", {"name": "stdout", "text": "hello\n"})
    assert not send(state, "status", {"execution_state": "idle"}, parent="another-cell")
    assert not send(state, "status", {"execution_state": "idle"}, generation="old-generation")
    if reply_first:
        send(state, "execute_reply", {"status": "ok"}, channel="shell")
    else:
        send(state, "status", {"execution_state": "idle"})
    assert not state.complete and state.status == "running"
    if reply_first:
        send(state, "status", {"execution_state": "idle"})
    else:
        send(state, "execute_reply", {"status": "ok"}, channel="shell")
    assert state.complete and state.status == "succeeded"
    assert not send(state, "stream", {"name": "stdout", "text": "late"})
    assert [item["text"] for item in state.outputs] == ["hello\n"]


def test_display_update_preserves_execute_count_and_clear_wait_is_deferred():
    state = ExecutionOutput(generation="generation-one", msg_id="msg-one")
    send(state, "execute_result", {"data": {"text/plain": "before"}, "metadata": {}, "execution_count": 7,
         "transient": {"display_id": "a display"}})
    send(state, "update_display_data", {"data": {"text/plain": "after"}, "metadata": {}, "transient": {"display_id": "a display"}})
    assert state.outputs[0]["execution_count"] == 7 and state.outputs[0]["output_type"] == "execute_result"
    assert state.outputs[0]["data"]["text/plain"] == "after"
    assert c.encoded_size(state.outputs, maximum=state.maximum) == state.bytes
    send(state, "clear_output", {"wait": True})
    assert state.outputs
    send(state, "stream", {"name": "stderr", "text": "next"})
    assert state.outputs == [{"output_type": "stream", "name": "stderr", "text": "next"}]
    send(state, "update_display_data", {"data": {"text/plain": "orphan"}, "metadata": {}, "transient": {"display_id": "a display"}})
    assert len(state.outputs) == 1 and state.ignored == 1


def test_error_stdin_widgets_and_flood_do_not_become_successful_output():
    state = ExecutionOutput(generation="generation-one", msg_id="msg-one")
    send(state, "comm_open", {"data": {"script": "not executed"}})
    assert state.outputs == [] and state.ignored == 1
    with pytest.raises(ForgeError) as error:
        send(state, "input_request", {"prompt": "secret"}, channel="stdin")
    assert error.value.code == "operation_rejected"
    send(state, "error", {"ename": "ValueError", "evalue": "bad", "traceback": ["ValueError: bad"]})
    send(state, "execute_reply", {"status": "error"}, channel="shell")
    send(state, "status", {"execution_state": "idle"})
    assert state.status == "error"
    flood = ExecutionOutput(generation="generation-one", msg_id="msg-one", output_bytes=1024)
    with pytest.raises(ForgeError) as error:
        for _ in range(100):
            send(flood, "stream", {"name": "stdout", "text": "x"*100})
    assert error.value.code == "resource_limit" and flood.status == "output_limit"
    assert flood.bytes <= 1024 and not send(flood, "stream", {"name": "stdout", "text": "more"})


def test_one_large_display_cannot_bypass_output_budget():
    state = ExecutionOutput(generation="generation-one", msg_id="msg-one", output_bytes=1024)
    with pytest.raises(ForgeError):
        send(state, "display_data", {"data": {"text/plain": "x"*2048}, "metadata": {}})
    assert state.limit_exceeded and state.outputs == []


def test_stream_lifetime_budget_survives_clear_and_output_count_is_32():
    state=ExecutionOutput(generation='generation-one',msg_id='msg-one')
    for _ in range(16):
        send(state,'stream',dict(name='stdout',text='x'*65536))
        send(state,'clear_output',dict(wait=False))
    with pytest.raises(ForgeError) as error:send(state,'stream',dict(name='stdout',text='x'))
    assert error.value.code=='resource_limit' and state.limit_exceeded and state.outputs==[]
    displays=ExecutionOutput(generation='generation-one',msg_id='msg-one')
    for _ in range(32):send(displays,'display_data',dict(data={'text/plain':'one'},metadata={}))
    with pytest.raises(ForgeError) as error:send(displays,'display_data',dict(data={'text/plain':'next'},metadata={}))
    assert error.value.code=='resource_limit' and len(displays.outputs)==32
