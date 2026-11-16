"""Real Dataset values, original seals, and measured relational reuse."""
from pathlib import Path

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan


def build_chain():
    left = data.from_items([{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}, {'k': 1, 'v': 'c'}])
    right = data.from_items([{'k': 1, 'r': 10}, {'k': 1, 'r': 11}, {'k': 2, 'r': 20}])
    last = data.from_items([{'k': 1, 'z': 7}, {'k': 2, 'z': 8}])
    return left.join(right, on='k').join(last, on='k').reduce_by_key(
        'k', lambda s, r: {'k': r['k'], 'values': (s or {}).get('values', []) + [(r['v'], r['r'], r['z'])]})


def test_real_chain_preserves_values_order_and_explicit_region(tmp_path):
    with RunContext.start(tmp_path/'run') as run:
        with data.local_execution(workers=2, worker_mode='thread', partitions=3, batch_rows=2):
            rows = build_chain().take_all()
        assert rows == [{'k': 1, 'values': [('a', 10, 7), ('a', 11, 7), ('c', 10, 7), ('c', 11, 7)]},
                        {'k': 2, 'values': [('b', 20, 8)]}]
        bundle = load_sealed_plan(run.directory, 'action-1', version='v3')
        graph, physical = bundle['graph'], bundle['physical']
        regions = [s for s in physical.stages if s.stage_id.startswith('relational-v1-')]
        assert len(regions) == 1
        kinds = {n.occurrence_id: n.kernel.kind for n in graph.nodes}
        assert [kinds[o] for o in regions[0].occurrences] == ['join', 'join', 'reduce']
        assert regions[0].profile.requests['memory_bytes'] < 256*1024**2
        facts = run.journal.execution_commits('action-1')
        assert facts and all(f['status'] == 'acked' for f in facts)
