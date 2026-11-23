"""Real native recovery harness and explicitly separate actor component calls."""
import asyncio

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def collect_before_commit(dataset, directory, monkeypatch):
    """Run actual workers, observe their actual rows, interrupt before native commit."""
    from demiflow.execution.outputs.terminals import TerminalSubscription
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow import thaw_record_value

    class BeforeCommit(Exception):
        pass

    rows = []
    # ResultHandle uses read_admitted so its simultaneous output/workspace
    # admission is preserved. Observe the actual read path; patching only the
    # compatibility read() entry misses every row in the bounded collector.
    read = TerminalSubscription.read_admitted

    def observed(subscription, **kwargs):
        records = read(subscription, **kwargs)
        for record in records:
            assert record.inline is not None
            rows.append(thaw_record_value(record.inline))
        return records

    def interrupted(*args, **kwargs):
        raise BeforeCommit('test interruption before native commit')

    with pytest.raises(BeforeCommit):
        with RunContext.start(directory) as run, data.local_execution():
            reference = run.ref
            with monkeypatch.context() as fault:
                fault.setattr(TerminalSubscription, 'read_admitted', observed)
                fault.setattr(DataflowCheckpointCoordinator, 'commit', interrupted)
                dataset.take_all()
    assert rows, 'the real worker must have produced a row before the injected interruption'
    assert run.journal.execution_commits('action-1') == []
    return rows, reference


def resume_collect(dataset, reference):
    with RunContext.resume(reference), data.local_execution():
        return dataset.take_all()


def call_metrics(row):
    """Aggregate returned worker trace, never the unexecuted driver declaration."""
    from demiflow.execution.journal.run_telemetry import CallMetrics
    call = row.get('call') or row['error']['call']
    metrics = CallMetrics()
    turns = call.get('environment', {}).get('turns')
    if turns is None:
        metrics.observe(call)
    else:
        for turn in turns:
            for attempt in turn.get('attempts') or [turn]:
                metrics.observe(attempt)
    return metrics.summary()


def collect_actor_component(declaration):
    """Invoke the real actor in this process so injected operator faults are real.

    This intentionally does not execute a Dataset or prove worker serialization;
    separate native Dataset tests cover those boundaries.
    """
    from demiflow.data.sources import ItemsSource, MaterializedSource
    from demiflow.execution.sources.materialized_input import InputMaterializedHandle
    from demiflow.operator_llm.runtime import PromptActor
    source = declaration._source
    if isinstance(source, ItemsSource):
        rows = source.items
    else:
        assert isinstance(source, MaterializedSource)
        assert isinstance(source.handle, InputMaterializedHandle)
        rows = tuple(source.handle.rows())
    assert len(declaration._stages) == 1
    actor = declaration._stages[0]
    assert isinstance(actor, PromptActor)

    async def invoke():
        await actor.astart()
        try:
            return [await actor(row) for row in rows]
        finally:
            await actor.aclose()

    return asyncio.run(invoke())
