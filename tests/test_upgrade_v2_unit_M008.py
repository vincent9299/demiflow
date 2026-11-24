"""M008: scope_request binds native_occurrence and the admitted response bound."""
from types import SimpleNamespace

import pytest

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identity
from demiflow.execution.worker.row_scope import RowScope, set_row_scope
from demiflow.execution.operator_bindings.dataflow_model_journal import NativeEmbeddingRecovery
from demiflow.operator_llm.journal import RequestRecord


def _bound_rec(run):
    actor = SimpleNamespace(_journal=None, options={'max_response_bytes': 4096})
    action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
    rec = NativeEmbeddingRecovery(actor, action, 'node')
    rec.bind_actor()
    return rec


def _request():
    return RequestRecord({'protocol': 'embeddings-v1', 'model_contract': {'m': 1}, 'body': {'x': 1}})


def test_scope_binds_occurrence_and_bound(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        rec = _bound_rec(run)
        set_row_scope(RowScope('occ-1'))
        try:
            request = _request()
            scoped = rec.scope_request(request)
            assert isinstance(scoped, RequestRecord)
            assert scoped['native_response_bytes'] == rec._response_bound
            assert scoped['native_occurrence'] == identity(['act-1', 'node', 'occ-1'])
            assert scoped.request_key != request.request_key
            # Same batch occurrence across attempts yields a stable key.
            assert rec.scope_request(request).request_key == scoped.request_key
        finally:
            set_row_scope(None)


def test_scope_distinguishes_occurrences(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        rec = _bound_rec(run)
        request = _request()
        set_row_scope(RowScope('occ-1'))
        try:
            first = rec.scope_request(request).request_key
        finally:
            set_row_scope(None)
        set_row_scope(RowScope('occ-2'))
        try:
            second = rec.scope_request(request).request_key
        finally:
            set_row_scope(None)
        assert first != second


def test_scope_rejects_missing_row_scope(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        rec = _bound_rec(run)
        set_row_scope(None)
        with pytest.raises(RecoveryContractError, match='source occurrence'):
            rec.scope_request(_request())