"""Read original native graph ACKs for the historical U3 business oracles."""
import hashlib
import json


def committed_rows(run, action_id, reader_id, facts, *, require_final=True):
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from demiflow.execution.storage.objects import RunArtifactStore
    from demiflow.execution.outputs.replay import PublishedRecordReader
    from demiflow.execution.contracts.record_codec import document_limit
    from demiflow.execution.contracts.record_codec import _decode_record_document
    sealed = load_sealed_plan(run.directory, action_id, version='v3')
    bounds = sealed['physical'].bounds
    store = RunArtifactStore(root=run.directory / '.dataflow-store', store_id='local',
                             journal=None, bounds=bounds, readonly=True)
    prior_ids, prior_rows, inherited = [], [], {}
    assert (facts or not require_final) and len(facts) <= 256
    assert [fact['sequence'] for fact in facts] == list(range(1, len(facts) + 1))
    assert len({fact['commit_id'] for fact in facts}) == len(facts)
    final_count = 0
    for fact in facts:
        assert fact['action_id'] == action_id and fact['status'] == 'acked'
        ref = fact['prepared']['state']['dataflow-cut-v3']
        raw = (run.directory / ref['path']).read_bytes()
        assert len(raw) == ref['bytes'] and hashlib.sha256(raw).hexdigest() == ref['sha256']
        state = json.loads(raw)
        cut = state['cut']
        assert cut['sequence'] == fact['sequence']
        contributions = {c['participant_id']: c for c in state['contributions']}
        assert len(contributions) == len(state['contributions'])
        assert set(cut['participants']) <= contributions.keys()
        for owner, contribution in contributions.items():
            if owner in cut['participants']:
                assert contribution['cut_id'] == cut['cut_id']
            else:
                assert contribution == inherited[owner], 'retained contribution needs its original ACK'
        inherited = contributions
        publication = fact['prepared']['sinks']['materialize']
        reader = PublishedRecordReader(publication, store=store, bounds=bounds)
        rows, ids = [], []
        try:
            while True:
                try:
                    document, record_id, sequence, _ = reader.pop()
                except StopIteration:
                    break
                record = _decode_record_document(document, record_id=record_id, sequence=sequence,
                    max_bytes=document_limit(bounds), payload_max_bytes=bounds.max_row_bytes,
                    metadata_max_bytes=bounds.max_control_bytes)
                assert record.inline is not None, 'small historical graph rows must retain actual inline values'
                rows.append(dict(record.inline)); ids.append(record_id)
                assert len(rows) <= 512, 'historical fixture unexpectedly exceeded its original small workload'
        finally:
            reader.close()
        assert len(rows) == publication['rows'] == publication['manifest']['total_rows']
        assert ids[:len(prior_ids)] == prior_ids and rows[:len(prior_rows)] == prior_rows
        prior_ids, prior_rows = ids, rows
        if cut['final']:
            final_count += 1
            assert fact is facts[-1] and publication['manifest']['final']
            assert all(c['end_of_input'] for c in state['dataflow.sources.v3'])
    assert final_count == int(require_final)
    return [row['row'] for row in prior_rows if row['reader_id'] == reader_id]


def capture_acks(run, action_id):
    return {fact['commit_id']: fact for fact in run.journal.execution_commits(action_id)
            if fact['status'] == 'acked'}


def acknowledged_ancestor_rows(run, action_id, facts):
    """Read the actual business boundary, not a pre-read source cursor/cut size."""
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from demiflow.execution.storage.objects import RunArtifactStore
    from demiflow.execution.contracts.checkpoint import artifact_from_json as _artifact_from_json
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    from demiflow.execution.contracts.dataflow_codec import compute_participant_id
    from demiflow.execution.contracts.recovery_contract import identity
    sealed = load_sealed_plan(run.directory, action_id, version='v3')
    physical = sealed['physical']
    result_nodes = [node for node in sealed['graph'].nodes
                    if 'graph_result' in node.kernel.configuration]
    assert len(result_nodes) == 1 and facts
    occurrence = result_nodes[0].kernel.configuration['graph_result']['ancestor']
    owners = [(stage, partition) for stage in physical.stages if occurrence in stage.occurrences
              for partition in stage.partitions]
    assert len(owners) == 1, 'this historical ancestor is a single original occurrence'
    stage, partition = owners[0]
    owner = compute_participant_id(stage.stage_id, partition)
    latest = max(facts.values(), key=lambda fact: fact['sequence'])
    image = StateArtifacts(run).read(latest['prepared']['state']['dataflow-cut-v3'])
    contributions = [c for c in image['contributions'] if c['participant_id'] == owner]
    assert len(contributions) == 1
    ref = _artifact_from_json(contributions[0]['operator_states'][identity(['worker-frontier', partition])])
    store = RunArtifactStore(root=run.directory / '.dataflow-store', store_id='local',
        journal=None, bounds=physical.bounds, readonly=True)
    frontier = json.loads(b''.join(store.read(ref, max_bytes=physical.bounds.max_state_bytes)))
    assert frontier['schema'] == 'demiflow.worker-frontier.v3'
    metrics = frontier['metrics'][occurrence]
    assert set(metrics) == {'rows_input', 'rows_output'}
    assert type(metrics['rows_output']) is int and metrics['rows_output'] >= 0
    # The original ancestor is exactly one output per input. No buffer may be
    # mistaken for a completed business call in this fixture.
    assert metrics['rows_input'] == metrics['rows_output']
    return metrics['rows_output']


def assert_acks_retained(run, action_id, before):
    after = capture_acks(run, action_id)
    assert before and before.keys() <= after.keys()
    assert all(after[key]['prepared'] == fact['prepared']
               and after[key]['receipts'] == fact['receipts'] for key, fact in before.items())


def assert_rejected_before_workers(run, action_id, business_marker):
    """A declared graph storage shortage must not activate any business owner."""
    assert not business_marker.exists()
    assert not run.journal.execution_commits(action_id)
    assert run.journal.execution_action(action_id)['result'] is None
    with run.journal._connection() as db:
        if db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resource_allocations'").fetchone():
            assert db.execute('SELECT count(*) FROM resource_allocations').fetchone()[0] == 0


def assert_closed(run_directory, *, allow_unprovisioned=False):
    from runtime_v3_owned_crash import assert_released, process_records
    if process_records(run_directory):
        return assert_released(run_directory)
    assert allow_unprovisioned, 'expected an actual process worker'
    from demiflow.execution.run_journal import RunJournal
    with RunJournal(run_directory)._connection() as db:
        if db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resource_allocations'").fetchone():
            assert db.execute('SELECT count(*) FROM resource_allocations').fetchone()[0] == 0
