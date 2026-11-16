"""Read original native graph ACKs for the historical U3 business oracles."""
import hashlib
import json


def committed_rows(run, action_id, reader_id, facts, *, require_final=True):
    from demiflow.execution.sealed_plans import load_sealed_plan
    from demiflow.execution.dataflow_store import RunArtifactStore
    from demiflow.execution.dataflow_replay import PublishedRecordReader
    from demiflow.execution.dataflow_record_frames import document_limit
    from demiflow.execution.exchange.dataflow_bridge import _decode_record_document
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


def assert_acks_retained(run, action_id, before):
    after = capture_acks(run, action_id)
    assert before and before.keys() <= after.keys()
    assert all(after[key]['prepared'] == fact['prepared']
               and after[key]['receipts'] == fact['receipts'] for key, fact in before.items())


def assert_closed(run_directory, *, allow_unprovisioned=False):
    from runtime_v3_owned_crash import assert_released, process_records
    if process_records(run_directory):
        return assert_released(run_directory)
    assert allow_unprovisioned, 'expected an actual process worker'
    from demiflow.execution.run_journal import RunJournal
    with RunJournal(run_directory)._connection() as db:
        if db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resource_allocations'").fetchone():
            assert db.execute('SELECT count(*) FROM resource_allocations').fetchone()[0] == 0
