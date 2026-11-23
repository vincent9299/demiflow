"""Actual kernel state replaces retired registry-specific test construction.

Component boundary: real kernel and state codec; the bounded in-memory store is
the existing K fixture, not evidence of a process worker or durable commit.
Public actor/partial/shared-owner and actual non-final recovery remain separate.
"""
import functools
from dataclasses import replace
import pytest

from demiflow.execution.contracts import state_codec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels.operator import OperatorKernel
from test_runtime_v3_k import services, spec, program, InputPort, rec, rows_of


class Hooked:
    def __init__(self):
        self.values = [7]
    def __call__(self, row):
        self.values.append(row['n'])
        return {'n': row['n'], 'count': len(self.values)}
    def snapshot_state(self):
        return {'values': self.values}
    def restore_state(self, state):
        self.values = state['values']


class Plain:
    def __init__(self):
        self.count = 0
    def __call__(self, row):
        self.count += 1
        return {'n': row['n'], 'count': self.count}


def kernel_for(target, *, maximum=65536):
    svc = services(programs={'fn': program(target)})
    svc.bounds = replace(svc.bounds, max_state_bytes=maximum)
    policy = spec('MapOp', {'program': 'fn'}, inputs=[InputPort('input', 'incremental')],
                  state='adapter')
    return OperatorKernel(policy, services=svc, partition_id='p0'), svc, policy


def image(svc, ref):
    return state_codec.loads(b''.join(svc.store.read(ref, max_bytes=ref.size_bytes)),
                            max_bytes=ref.size_bytes)


@pytest.mark.parametrize('form', ['hooks', 'plain', 'partial'])
def test_actual_kernel_snapshots_and_restores_original_owner(form):
    target = Hooked() if form == 'hooks' else Plain()
    if form == 'partial':
        target = functools.partial(target)
    first, svc, policy = kernel_for(target)
    second = None
    try:
        first.start()
        initial = image(svc, first.snapshot())['state']
        if form == 'hooks':
            assert initial == {'mode': 'hooks', 'value': {'values': [7]}}
        else:
            assert initial['value']['fields'] == {'count': 0}
        assert rows_of(first.push('input', rec({'n': 10}))) == [
            {'n': 10, 'count': 2 if form == 'hooks' else 1}]
        saved = first.snapshot()
        snapshot = image(svc, saved)
        assert snapshot['state']['mode'] == ('hooks' if form == 'hooks' else 'plain_json')
        before = image(svc, saved)
        list(first.push('input', rec({'n': 11}, sequence=1)))
        assert image(svc, saved) == before
        first.close()
        second = OperatorKernel(policy, services=svc, partition_id='p0')
        second.restore(saved)
        second.start()
        assert rows_of(second.push('input', rec({'n': 12}, sequence=1))) == [
            {'n': 12, 'count': 3 if form == 'hooks' else 2}]
    finally:
        first.close()
        if second is not None:
            second.close()


def test_actual_hook_snapshot_budget_rejects_before_store_write(monkeypatch):
    target = Hooked()
    first, svc, _ = kernel_for(target)
    try:
        first.start()
        first._state_owner().values = ['\x00' * 20000]
        writes = []
        monkeypatch.setattr(svc.store, 'put', lambda *a, **k: writes.append(a))
        with pytest.raises(DataflowError, match='state byte budget'):
            first.snapshot()
        assert writes == []
    finally:
        first.close()


def test_state_codec_preflight_rejects_expansion_before_full_encoding(monkeypatch):
    calls = []
    monkeypatch.setattr(state_codec.json, 'dumps', lambda *a, **k: calls.append(a))
    with pytest.raises(DataflowError, match='state byte budget'):
        state_codec.dumps({'text': '\x00' * 20000}, max_bytes=65536,
                          admit=lambda size: None)
    assert calls == []
