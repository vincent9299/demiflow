"""Acceptance for bounded identity of native Python collection values."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import demiflow

from demiflow import data
from demiflow.execution import RunContext, RecoveryContractError
from demiflow.execution.seal.program_identity import describe


def test_unordered_collection_identity_is_stable_across_process_seeds():
    root = Path(demiflow.__file__).resolve().parents[1]
    script = '''import json,os,demiflow
from pathlib import Path
from demiflow.execution.seal.program_identity import describe
assert Path(demiflow.__file__).resolve().is_relative_to(Path(os.environ['DF_INTEGRATION_ROOT']))
print(json.dumps(describe({'alpha','beta',('nested',frozenset({'x','y'}))}),sort_keys=True))
'''
    results = []
    for seed in ['1', '999']:
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(root), DF_INTEGRATION_ROOT=str(root))
        run = subprocess.run([sys.executable, '-B', '-c', script], cwd=root,
                             env=env, capture_output=True, text=True, timeout=20)
        assert run.returncode == 0, run.stderr
        results.append(json.loads(run.stdout))
    assert results[0] == results[1]
    assert describe({1, 2}) != describe({1, 3})
    assert describe({1, 2}) != describe(frozenset({1, 2}))


def test_changed_set_source_is_rejected_on_resume(tmp_path):
    with pytest.raises(RuntimeError, match='stop-after-materialized'):
        with RunContext.start(tmp_path / 'run') as run:
            reference = run.ref
            assert data.from_items([{'value': {1, 2}}]).materialize().take_all() == [{'value': {1, 2}}]
            raise RuntimeError('stop-after-materialized')
    with pytest.raises(RecoveryContractError, match='changed|differ'):
        with RunContext.resume(reference):
            data.from_items([{'value': {1, 3}}]).materialize()


@pytest.mark.parametrize('value', [set(range(16385)), {'a' * 400000, 'b' * 400000, 'c' * 400000}])
def test_set_identity_has_cardinality_and_total_descriptor_byte_limits(value):
    with pytest.raises(RecoveryContractError, match='budget'):
        describe(value)


def test_unadapted_objects_still_fail_closed():
    class Unadapted:
        pass
    with pytest.raises(RecoveryContractError, match='No native semantic identity'):
        describe({Unadapted()})


@pytest.mark.parametrize('factory', [set, frozenset])
def test_set_payload_size_counts_members_and_rejects_unknown_objects(factory):
    from demiflow.data.runtime_contract import check_size, RuntimeContractError
    check_size(factory({1, 2}), maximum=4096)
    with pytest.raises(RuntimeContractError, match='byte budget'):
        check_size(factory({b'x' * 4096}), maximum=1024)
    with pytest.raises(RuntimeContractError, match='Unsupported runtime value'):
        check_size(factory({object()}), maximum=4096)
