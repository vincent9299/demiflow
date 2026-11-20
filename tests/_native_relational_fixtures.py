"""Bounded read-only evidence from the original sealed graph and final ACK."""
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.dataflow_kernel_pack import read_kernel_image
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.kernels import state_codec
from demiflow.execution.sealed_plans import load_sealed_plan
from demiflow.execution.state_artifacts import StateArtifacts


def native_region_evidence(local):
    directory = Path(local.run_ref.directory)
    action_id = local.stats['action_id']
    bundle = load_sealed_plan(directory, action_id, version='v3')
    graph, physical = bundle['graph'], bundle['physical']
    with sqlite3.connect((directory/'run.sqlite').as_uri()+'?mode=ro', uri=True) as db:
        rows = db.execute('SELECT status,prepared FROM runtime_commits WHERE action_id=? ORDER BY sequence LIMIT 257',
                          (action_id,)).fetchall()
    assert 1 <= len(rows) <= 256 and all(row[0] == 'acked' for row in rows)
    carrier = StateArtifacts(SimpleNamespace(directory=directory,
        limits={'max_state_bytes':physical.bounds.max_state_bytes})).read(
            json.loads(rows[-1][1])['state']['dataflow-cut-v3'])
    counters = []
    for stage in physical.stages:
        if not stage.stage_id.startswith('relational-v1-'):
            continue
        occurrence = stage.occurrences[-1]
        images = [entry['operator_states'][occurrence] for entry in carrier['contributions']
                  if occurrence in entry['operator_states']]
        assert len(images) == 1
        ref = ArtifactRef(**images[0])
        store = RunArtifactStore(root=directory/'.dataflow-store', store_id=ref.store_id,
                                  journal=None, bounds=physical.bounds, readonly=True)
        try:
            raw = read_kernel_image(store, ref, physical.bounds, occurrence=occurrence,
                                    partition=stage.partitions[0])
            image = state_codec.loads(raw, max_bytes=physical.bounds.max_state_bytes)
        finally:
            store.close()
        measured = image['relational_region']
        assert measured['region_id'] == stage.stage_id
        counters.append(measured['counters'])
    return graph, physical, counters


def oracle_join(left, right, *, on='k', right_on=None, how='inner'):
    right_on = right_on or on
    result = []
    for lhs in left:
        matches = [rhs for rhs in right if lhs[on] is not None and rhs[right_on] is not None
                   and type(lhs[on]) is type(rhs[right_on]) and lhs[on] == rhs[right_on]]
        if how == 'semi':
            if matches:
                result.append(dict(lhs))
        elif how == 'anti':
            if not matches:
                result.append(dict(lhs))
        elif matches:
            for rhs in matches:
                row = dict(lhs)
                for name, value in rhs.items():
                    if name == on == right_on and row[name] == value:
                        continue
                    destination = name if name not in row else name+'_right'
                    assert destination not in row
                    row[destination] = value
                result.append(row)
        elif how == 'left':
            result.append(dict(lhs))
    return result
