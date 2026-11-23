"""The published chain can be read after its writer and worker have been retired."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow.execution.contracts.checkpoint import (
    legacy_checkpoint_fragment, replayable_effects, validate_checkpoint_manifest,
)
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.recovery_contract import identity
from demiflow.execution.run_journal import RunJournal


def write_original_chain(checkout, root):
    """Use only the fixed released SDK to write its F1/F2, K6 and block receipts."""
    script = """
import json, sys
from pathlib import Path
import demiflow
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.chain import (
    DriverStreamTerminal, build_commit_cut, compile_chain_topology, seal_write_plan_ref,
    stage_plan_artifact,
)
from demiflow.execution.recovery_contract import identity
from demiflow.execution.run_context import RunAction, RunContext
from chain_business import transform

assert Path(demiflow.__file__).resolve().parents[1] == Path(sys.argv[1])
root = Path(sys.argv[2])
rows = [{'v': 1}, {'v': 2}]
plan = LogicalPlan((MapOp(CallableSpec.create(transform)),))
source = ItemsSource(items=tuple(rows))
source_id = identity(['old-fixture-source'])
source_ref = {'source_id': source_id, 'data': {}}
with RunContext.start(root) as run:
    declaration = {'kind': 'chain-materialize', 'source': {'source_id': source_id}}
    run.journal.bind_execution_action(run.fence, 'main-action', declaration)
    action = RunAction(run, 'main-action', declaration)
    program = stage_plan_artifact(action, plan)
    write_program = seal_write_plan_ref(action, plan, source=source, program_id='slice-0')
    topology, dag, physical = compile_chain_topology(action, source, plan, source_ref,
        output_child_id='main-action-chain', host_id='local', plan_ref=program)
    terminal = DriverStreamTerminal(action, source_ref, output_child_id='main-action-chain',
                                    topology=topology)
    sink = BlockSink(terminal.child, 'output', staging_only=True)
    for row in rows:
        sink.append(row)
    cut = build_commit_cut('old-operation', identity(['old-cut']), source_id=source_id,
        next_ordinal=2, sequence=1, channel_id=topology['channel_id'], channel_cut_sequence=0,
        output_descriptor=sink.descriptor(), states={'stream-progress': {'rows': 2, 'inputs': 2, 'final': True}},
        inflight=[{'request_key': identity(['unknown']), 'status': 'unknown'},
                  {'request_key': identity(['known']), 'status': 'known_completed', 'effect_ref': identity(['effect'])}],
        final=True)
    receipt = terminal.commit_cut(cut)
    manifest = terminal.checkpoints[cut['checkpoint_id']]['manifest']
    terminal.finish(2)
    run.journal.complete_execution_action(run.fence, action.action_id, {'kind': 'test-complete'})
    evidence = {'topology': topology, 'dag': dag, 'physical': physical, 'program': program,
                'write_program': write_program, 'checkpoint': manifest, 'source_id': source_id,
                'receipt': receipt, 'facts': run.journal.source_binding('main-action-sealed-plan')}
(root.parent / 'original.json').write_text(json.dumps(evidence))
"""
    (root.parent / 'chain_business.py').write_text("def transform(row):\n    return {'v': row['v'] + 1}\n")
    cp = subprocess.run([sys.executable, '-B', '-c', script, str(checkout), str(root)],
                        cwd=root.parent, env=dict(os.environ, PYTHONPATH=str(checkout)),
                        text=True, capture_output=True, timeout=120)
    assert cp.returncode == 0, cp.stderr
    return json.loads((root.parent / 'original.json').read_text())


@pytest.fixture
def original_chain(tmp_path, legacy_seal_writer):
    root = tmp_path / 'old-run'
    return root, write_original_chain(legacy_seal_writer, root)


def test_original_chain_receipt_and_paid_effects_stay_readable(original_chain):
    root, original = original_chain
    journal = RunJournal(root)
    record, = journal.execution_commits('main-action-chain')
    assert record['status'] == original['receipt']['status'] == 'acked'
    assert record['commit_id'] == original['receipt']['commit_id']
    assert record['prepared']['sinks']['output'] == original['receipt']['receipt_descriptor']
    assert record['intent']['position'] == {'source_id': original['source_id'], 'next_ordinal': 2}
    checkpoint = original['checkpoint']
    validate_checkpoint_manifest(checkpoint, topology={'sources': [{'source_id': original['source_id'], 'partition_id': 'p0'}],
        'channels': [original['topology']['channel_id']]})
    assert replayable_effects(checkpoint) == [identity(['effect'])]
    assert checkpoint['participants']['operators'] == original['topology']['operators']
    assert checkpoint['source_progress'][0]['position'] == {'next_ordinal': 2}
    position = record['intent']['position']
    assert legacy_checkpoint_fragment(position, source_ids=[original['source_id']])['source_progress']
    with pytest.raises(ProtocolError):
        legacy_checkpoint_fragment(position, source_ids=[original['source_id']], channel_ids=['other'])
