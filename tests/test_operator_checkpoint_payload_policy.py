from demiflow.execution.operator_checkpoint import OperatorCheckpoint


def test_key_authoritative_checkpoint_accepts_resumed_row_payload(tmp_path):
    path = tmp_path / "operator.sqlite"
    strict = OperatorCheckpoint(path, operator="search_web")
    strict.register("request-1", {"row": {"stage": "old"}, "request": {"q": "x"}})
    strict.complete("request-1", {"status": "ok"})
    try:
        strict.register("request-1", {"row": {"stage": "new"}, "request": {"q": "x"}})
    except ValueError:
        pass
    else:
        raise AssertionError("immutable payloads must still reject changes")

    resumed = OperatorCheckpoint(path, operator="search_web", payload_policy="key_authoritative")
    value = resumed.register("request-1", {"row": {"stage": "new"}, "request": {"q": "x"}})
    assert value["state"] == "completed"
    assert value["result"] == {"status": "ok"}
