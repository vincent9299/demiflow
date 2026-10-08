from demiflow.execution.stream_checkpoint import StreamCheckpoint


def test_explicit_identity_migration_rebinds_checkpoint(tmp_path):
    path = tmp_path / "checkpoint.json"
    StreamCheckpoint(path, identity={"contract": "v1"})
    try:
        StreamCheckpoint(path, identity={"contract": "v2"})
    except ValueError:
        pass
    else:
        raise AssertionError("identity changes must be rejected by default")
    migrated = StreamCheckpoint(path, identity={"contract": "v2"}, allow_identity_migration=True)
    assert migrated.state["identity"] == migrated.identity
