"""Stable request identity replaces the retired independent operator ledger."""
import json
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import EffectCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_authority import DriverJournalService
from demiflow.execution.recovery_contract import identity
from test_runtime_v3_real_seams import prepared


def test_key_authoritative_checkpoint_accepts_resumed_row_payload(tmp_path):
    old = {'row': {'stage': 'old'}, 'request': {'q': 'x'}}
    new = {'row': {'stage': 'new'}, 'request': {'q': 'x'}}
    def command(kind, payload, *, strict, result=None):
        # A request contract decides its semantic identity explicitly. Transient
        # row metadata is excluded only in the key-authoritative variant.
        digest = identity(payload if strict else payload['request'])
        effect = identity(['search_web', 'request-1', strict])
        return EffectCommand(operation_id=identity([effect, kind, digest]), kind=kind,
            effect_id=effect, actor_id=identity('search_web'), request_digest=digest,
            payload_ref=result, budget={'requests': 1} if kind == 'reserve' else {})

    with prepared(tmp_path, data.from_items([])) as (execution, services):
        service = DriverJournalService(execution._action, store=services.store, bounds=services.bounds)
        journal = execution._action.context.journal
        try:
            ref = services.store.put(iter([b'{"status":"ok"}']), max_bytes=64,
                                     media_type='application/json')
            for strict in (True, False):
                assert service.effect(command('reserve', old, strict=strict), lease=None).status == 'reserved'
                assert service.effect(command('mark_sent', old, strict=strict), lease=None).status == 'sent'
                assert service.effect(command('confirm', old, strict=strict, result=ref), lease=None).result_ref == ref
            with pytest.raises(DataflowError, match='different actor/request'):
                service.effect(command('lookup', new, strict=True), lease=None)
            service.close(deadline_ms=3000)
            # Re-open only a transport view of the same action/journal; neither
            # an independent database nor a replacement result is introduced.
            service = DriverJournalService(execution._action, store=services.store, bounds=services.bounds)
            reused = service.effect(command('lookup', new, strict=False), lease=None)
            assert reused.status == 'confirmed' and reused.result_ref == ref
            raw = b''.join(services.store.read(reused.result_ref, max_bytes=64))
            assert json.loads(raw) == {'status': 'ok'}
            changed_request = {**new, 'request': {'q': 'different'}}
            with pytest.raises(DataflowError, match='different actor/request'):
                service.effect(command('lookup', changed_request, strict=False), lease=None)
            assert journal.budget_consumed('requests') == 2
            with journal._connection() as db:
                assert db.execute('SELECT count(*) FROM runtime_effects').fetchone()[0] == 2
            assert not (tmp_path/'operator.sqlite').exists()
        finally:
            service.close(deadline_ms=3000)
