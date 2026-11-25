"""A004 · Dataset.count keeps its single action boundary; streaming counts via borrowed delivery."""
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


def _action_spy(monkeypatch):
    calls = []
    orig = RunContext.action

    def counting(self, declaration, **kw):
        calls.append(declaration.get("kind"))
        return orig(self, declaration, **kw)

    monkeypatch.setattr(RunContext, "action", counting)
    return calls


def _plain():
    return data.from_items([{"id": i} for i in range(6)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(6)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_count_plain_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _plain().count() == 6
    assert calls == ["count"]


def test_count_streaming_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _stream().count() == 6
    assert calls == ["count"]


def test_count_materialized_reads_no_rows_and_loads_no_program(monkeypatch):
    """A materialized count reuses the proof made at publication.

    Thread workers keep every store read in this process, so the spy sees reads
    from the driver and the workers alike; take_all on the same handle shows
    what reading the rows looks like.
    """
    from demiflow.execution.contracts.dataflow_codec import RECORDS_MEDIA
    from demiflow.execution.seal.plan import PROGRAM_MEDIA
    from demiflow.execution.storage.objects import RunArtifactStore
    reads = []
    for name in ("read", "read_into"):
        real = getattr(RunArtifactStore, name)

        def spy(self, ref, *args, _real=real, **kwargs):
            reads.append(ref)
            return _real(self, ref, *args, **kwargs)
        monkeypatch.setattr(RunArtifactStore, name, spy)
    with data.local_execution(workers=2, worker_mode="thread"):
        mat = _plain().materialize()
        calls = _action_spy(monkeypatch)
        reads.clear()
        assert mat.count() == 6
        counted = list(reads)
        reads.clear()
        assert len(mat.take_all()) == 6
        taken = list(reads)
    blocks = {ref.digest for ref in taken if ref.media_type == RECORDS_MEDIA}
    assert blocks
    assert not blocks & {ref.digest for ref in counted}
    assert not [ref for ref in counted if ref.media_type in (RECORDS_MEDIA, PROGRAM_MEDIA)]
    assert calls == ["count", "take_all"]
