"""Source fanout shares the existing bounded delivery window and ACK frontier."""
from types import SimpleNamespace as NS
from collections import Counter

import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner


def test_source_width_tracks_internal_paths_broadcast_and_unrelated_splits():
    runner = object.__new__(WorkerGraphRunner)
    runner._credit_limit = 64
    node = lambda kind='transform', requirement='incremental': NS(kind=kind,
        kernel=NS(inputs=[NS(name='main', requirement=requirement)]))
    owners = {key: node() for key in ('a','b','x','y','z','finite','unrelated')}
    owners['a'] = owners['b'] = node('source')
    owners['finite'] = node(requirement='finite_complete')
    runner.e = NS(_node_specs=owners, assignment=NS(source_splits=[
        NS(source_id='a'), NS(source_id='a'), NS(source_id='b')]))
    pairs = [('a','x'), ('a','y'), ('x','z'), ('y','z'), ('a','finite'),
             ('z','external'), ('b','other'), ('b','disk'), ('finite','later'), ('unrelated','unused')]
    runner._outgoing = {}
    runner._exchange = {}
    for i,(producer,consumer) in enumerate(pairs):
        edge = NS(edge_id=str(i), producer=producer, consumer=consumer, input_port='main')
        runner._outgoing.setdefault(producer,[]).append(edge)
        if consumer not in owners:
            runner._exchange[str(i)] = NS(mode='finite_sealed' if consumer=='disk' else 'durable_incremental',
                routing='broadcast' if consumer in {'external','later','unused'} else 'forward',
                consumer_partitions=tuple(range(3 if consumer=='external' else 100)))
    # a: fork→two paths through z→3 recipients; b is a different source
    # record and its finite spool consumes no delivery slot. Unrelated owners
    # and a finite consumer's eventual finish do not amplify this source step.
    assert runner._source_delivery_width() == 6


def _flow(rows=5, branches=16):
    source = data.from_items([{'i': i} for i in range(rows)])
    parts = [source.map(lambda row,b=b: {**row,'branch':b}) for b in range(branches)]
    return parts[0].union(*parts[1:])


@pytest.mark.parametrize('resume',[False, True])
def test_actual_shared_source_fanout_keeps_one_read_and_original_ack(tmp_path, monkeypatch, resume):
    from demiflow.execution.source_readers import ItemsReader
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.sealed_plans import load_sealed_plan
    from demiflow.execution.run_journal import RunJournal
    reads, cuts = [], []
    next_record, commit = ItemsReader.next_record, DataflowCheckpointCoordinator.commit
    def observe(reader):
        poll = next_record(reader)
        if poll.status == 'record':
            reads.append(poll.record.inline['i'])
        return poll
    monkeypatch.setattr(ItemsReader,'next_record',observe)
    class Interrupted(Exception): pass
    def interrupt(coordinator, *args, **kwargs):
        image = commit(coordinator,*args,**kwargs)
        cuts.append(image.commit_id)
        if resume and not image.cut.final and len(cuts)==1:
            raise Interrupted()
        return image
    limits={'memory_bytes':256*1024**2,'max_row_bytes':65536,'max_batch_bytes':262144,
        'max_state_bytes':2*1024**2,'max_scratch_bytes':2*1024**3,'timeout_s':120}
    def execute():
        with RunContext.start(tmp_path/'run',limits=limits) as run, data.local_execution(workers=4,worker_mode='thread'):
            nonlocal reference
            reference=run.ref
            return _flow().take_all()
    reference=None
    with monkeypatch.context() as patch:
        patch.setattr(DataflowCheckpointCoordinator,'commit',interrupt)
        if resume:
            with pytest.raises(Interrupted):execute()
        else:
            rows=execute()
    if resume:
        journal=RunJournal(reference.directory)
        before=journal.execution_commits('action-1')
        assert before and before[-1]['status']=='acked'
        with RunContext.resume(reference), data.local_execution(workers=4,worker_mode='thread'):
            rows=_flow().take_all()
        assert journal.execution_commits('action-1')[:len(before)]==before
    assert Counter((r['i'],r['branch']) for r in rows)==Counter((i,b) for i in range(5) for b in range(16))
    assert reads==list(range(5))
    bundle=load_sealed_plan(reference.directory,'action-1')
    assert len(bundle['graph'].sources)==1 and len(bundle['graph'].nodes)==19
    assert any(e.mode=='durable_incremental' for e in bundle['physical'].exchanges)


def test_single_source_invocation_wider_than_all_slots_still_rejects(tmp_path):
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.run_journal import RunJournal
    with pytest.raises(DataflowError, match='one operator invocation exceeds the parked durable credit window'):
        with RunContext.start(tmp_path/'run', limits={'memory_bytes':256*1024**2,
                'max_row_bytes':65536,'max_batch_bytes':262144,'max_state_bytes':2*1024**2,
                'max_scratch_bytes':2*1024**3,'timeout_s':30}) as run, data.local_execution(
                    workers=4,worker_mode='thread'):
            reference=run.ref
            _flow(rows=1,branches=65).take_all()
    assert not RunJournal(reference.directory).execution_commits('action-1')
