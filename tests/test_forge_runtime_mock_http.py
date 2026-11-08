"""Current execution-owned stream recovery; loopback fixture, no real model."""

import json
import threading
import time
from pathlib import Path

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from test_map_prompt_async import PACK, server
from test_forge_runs import base, root, custom, execute, rows, wait


def current_prompt(root, server):
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root, expected=workspace["platform"]["digest"], extras=["llm"])
    import os

    (root / "flows/prompt.yaml").write_text(
        PACK.replace(
            "base_url_env: TEST_PROMPT_URL",
            "base_url: " + os.environ["TEST_PROMPT_URL"],
        )
    )
    flow = """from operators.custom import payload
def run(*,source,target):
    source_rows=data.read_lance(source['uri'],version=source['version'],columns=['case_id']).limit(2).map(payload).materialize()
    result=source_rows.map_prompt_async('enrich',config='flows/prompt.yaml',inputs={'payload':'payload'},output='answer',
        concurrency=1,queue_depth=1,max_requests=2,options={'trust_env':False,'timeout_s':5,
        'max_request_bytes':1048576,'max_response_bytes':1048576}).materialize()
    return result.write_lance(target['uri'],mode='create',return_receipt=True)
"""
    operator = """from typing import TypedDict
class Input(TypedDict):
    case_id: str | None
class Output(TypedDict):
    payload: str
def payload(row: Input) -> Output:
    return {'payload':str(row['case_id'])}
"""
    return custom(
        root,
        flow,
        operators=operator,
        limits={"max_row_bytes": 1024**2, "max_batch_bytes": 1024**2},
    )


def test_rm06_current_mock_http_result_and_execution_checkpoint(root, server):
    prepared = current_prompt(root, server)
    result = execute(root, prepared)
    assert result["status"] == "succeeded", result
    assert len(server["requests"]) == 2
    assert all(row["answer"] == "ok" for row in rows(root, result))
    target = runs.directory(root, result["run_id"])
    import sqlite3
    from contextlib import closing

    with closing(sqlite3.connect(target / "run.sqlite")) as db:
        actions = [
            identity
            for identity, raw in db.execute(
                "SELECT action_id,declaration FROM runtime_actions LIMIT 16"
            )
            if json.loads(raw)["kind"] == "materialize"
        ]
    assert actions
    facts = [
        fact
        for identity in actions
        for fact in runs.journal(root, result["run_id"]).execution_commits(identity)
    ]
    assert facts and all(fact["status"] == "acked" for fact in facts)
    assert not (target / "checkpoints/streams").exists()
    assert not (target / "evidence/calls.sqlite").exists()
    runs.show(root, result["run_id"])
    assert len(server["requests"]) == 2


def test_rm07_current_mock_http_cancel_stops_dispatch(root, server):
    entered, release = threading.Event(), threading.Event()

    def response(body, index):
        entered.set()
        release.wait(3)
        return {"result": "ok"}

    server["respond"] = response
    prepared = current_prompt(root, server)
    runs.start(root, prepared["run_id"], expected=prepared["digest"])
    try:
        assert entered.wait(30)
        started = time.monotonic()
        runs.cancel(root, prepared["run_id"], compact=True)
        result = wait(root, prepared["run_id"], timeout=20)
        assert result["status"] == "cancelled", result
        assert (
            result["state"]["execution_stopped"] and result["state"]["usage_complete"]
        )
        assert len(server["requests"]) == 1 and time.monotonic() - started < 12
        assert all(output["result"] is None for output in result["outputs"])
    finally:
        release.set()
        if runs.show(root, prepared["run_id"])["status"] in runs.ACTIVE:
            runs.cancel(root, prepared["run_id"])
            wait(root, prepared["run_id"], timeout=20)
