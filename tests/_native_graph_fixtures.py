"""Read-only assertions over the graph fixture's original v3 ACK authority."""
import json
import os
from pathlib import Path

from _native_receipt_fixtures import committed_rows
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.contracts.dataflow_codec import compute_participant_id
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.seal.sealed_plans import load_sealed_plan
from demiflow.execution.storage.state_artifacts import StateArtifacts


# bbd93ff graph.py:46-47/65-67/720/730-732: the old graph driver ignored
# executor.workers and provisioned one 1-CPU/1-GiB host per ancestor/reader,
# inside its fixed 16-CPU/16-GiB pool. These are that same global ceilings.
HISTORICAL_WORKERS = 16
HISTORICAL_MEMORY = 16 * 1024 ** 3


def assert_native_graph(run, action_id, result, *, readers, count):
    bundle = load_sealed_plan(run.directory, action_id, version='v3')
    graph, physical = bundle['graph'], bundle['physical']
    assert result['dag_id'] == graph.dag_id and result['plan_id'] == physical.plan_id
    assert physical.policy.max_active_workers <= HISTORICAL_WORKERS
    assert physical.bounds.max_resident_bytes <= HISTORICAL_MEMORY
    owner = {compute_participant_id(s.stage_id, p): (s, p)
             for s in physical.stages for p in s.partitions}
    routers = [s for s in physical.stages if any(
        'graph_route' in node.kernel.configuration
        for node in graph.nodes if node.occurrence_id in s.occurrences)]
    assert len(routers) == 1 and set(routers[0].partitions) == set(readers)
    facts = run.journal.execution_commits(action_id)
    assert 1 <= len(facts) <= 256
    assert [f['sequence'] for f in facts] == list(range(1, len(facts) + 1))
    assert len({f['commit_id'] for f in facts}) == len(facts)
    assert all(f['status'] == 'acked' for f in facts)
    assert all(set(f['intent']['sinks']) == {'materialize'} for f in facts)
    with run.journal._connection() as db:
        assert {r[0] for r in db.execute('SELECT DISTINCT action_id FROM runtime_commits')} == {action_id}
        assert db.execute('SELECT attempt,fence FROM runtime_owner').fetchone() == (run.attempt, run.fence)
    carriers = [StateArtifacts(run).read(f['prepared']['state']['dataflow-cut-v3']) for f in facts]
    assert [c['cut']['final'] for c in carriers] == [False] * (len(carriers) - 1) + [True]
    origins = {}
    frontiers = []
    for fact, carrier in zip(facts, carriers):
        cut = carrier['cut']
        assert cut['sequence'] == fact['sequence']
        assert set(cut['participants']) <= owner.keys()
        current = {c['participant_id']: c for c in carrier['contributions']}
        assert len(current) == len(carrier['contributions'])
        assert set(cut['participants']) <= current.keys() <= owner.keys()
        for participant, contribution in current.items():
            key = (contribution['cut_id'], participant)
            if participant in cut['participants']:
                assert contribution['cut_id'] == cut['cut_id']
                assert contribution['boot_id']
                origins[key] = contribution
            else:
                assert origins.get(key) == contribution  # original ACK, never invented.
        vector = carrier['dataflow.sources.v3']
        assert len(vector) == 1
        assert vector[0]['split_id'] == physical.splits[0].split_id
        assert vector[0]['offset']['index'] == vector[0]['next_sequence']
        frontiers.append(vector[0]['next_sequence'])
    assert frontiers == sorted(frontiers) and frontiers[-1] == count
    assert carriers[-1]['dataflow.sources.v3'][0]['end_of_input'] is True
    assert {c['participant_id'] for c in carriers[-1]['contributions']} == owner.keys()
    channels = {e.channel_id for e in physical.exchanges
                if set(e.producer_partitions).isdisjoint(e.consumer_partitions)}
    assert set().union(*(set(c['channels']) for c in carriers[-1]['contributions'])) == channels
    # Every observed ancestor/target counter comes from its actual final frontier.
    root_ref = ArtifactRef(**facts[-1]['receipts']['materialize']['data']['ref'])
    store = RunArtifactStore(root=Path(run.directory) / '.dataflow-store',
        store_id=root_ref.store_id, journal=None, bounds=physical.bounds, readonly=True)
    try:
        metrics = {}
        for c in carriers[-1]['contributions']:
            stage, partition = owner[c['participant_id']]
            ref = ArtifactRef(**c['operator_states'][identity(['worker-frontier', partition])])
            state = json.loads(b''.join(store.read(ref, max_bytes=physical.bounds.max_state_bytes)))
            assert state['schema'] == 'demiflow.worker-frontier.v3'
            for occurrence, values in state['metrics'].items():
                metrics[(occurrence, partition)] = values
        config = next(node.kernel.configuration['graph_result'] for node in graph.nodes
                      if 'graph_result' in node.kernel.configuration)
        assert sum(v['rows_output'] for (occ, _), v in metrics.items() if occ == config['ancestor']) == count
        for reader, binding in config['targets'].items():
            assert sum(v['rows_output'] for (occ, part), v in metrics.items()
                       if occ == binding['occurrence_id'] and
                       (binding['partition_id'] is None or part == binding['partition_id'])) == count
    finally:
        store.close()
    expected = [{'x': i * 2} for i in range(count)]
    rows = committed_rows(run, action_id, facts[-1])
    assert len(rows) == count * len(readers)
    for reader in readers:
        assert [r['row'] for r in rows if r['reader_id'] == reader] == expected
    assert result['egress_rows'] == count
    assert result['target_rows'] == {reader: count for reader in readers}
    assert set(result['checkpoints']) == {f['commit_id'] for f in facts}
    return facts, carriers, physical


def assert_managed_graph(executor, run_directory, result):
    diagnostics = executor._last_metadata.diagnostics
    assert diagnostics['engine'] == 'dataflow_v3'
    assert 1 < diagnostics['peak_active_workers'] <= HISTORICAL_WORKERS
    assert diagnostics['admitted_bounds']['max_resident_bytes'] <= HISTORICAL_MEMORY
    workers = diagnostics['workers']
    assert result['workers'] == diagnostics['observed_worker_count'] == len(workers)
    assert not diagnostics['worker_details_truncated']
    records = list(Path(run_directory).with_name(Path(run_directory).name + '-driver-resources')
                   .glob('units/*/*/process.json'))
    pids = {json.loads(path.read_text())['identity']['pid'] for path in records}
    assert os.getpid() not in pids
    assert {w['worker_process_id'] for w in workers} <= pids
    assert len(pids) > 1
    from demiflow.execution.hosts.capacity import local_capacity
    from demiflow.resources.capacity import LocalCapacityPool
    domain, capacity = local_capacity()
    pool = LocalCapacityPool(domain / 'capacity', capacity=capacity)
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}
