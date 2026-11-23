"""Real run journal, atomic budget/intent and loss-safe native transitions."""
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import EffectCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.journal.dataflow_authority import DriverJournalService
from demiflow.execution.recovery_contract import identity
from test_runtime_v3_real_seams import prepared


def command(kind, *, budget=None):
    return EffectCommand(identity(['rpc', kind]), kind, identity('effect'), identity('actor'),
                         identity('request'), None, budget or {})


def test_effect_budget_and_intent_roll_back_together(tmp_path, monkeypatch):
    with prepared(tmp_path, data.from_items([])) as (ex, services):
        action = ex._action
        service = DriverJournalService(action, store=services.store, bounds=services.bounds)
        journal = action.context.journal
        reserve = journal.reserve_budgets_in_transaction
        def fault(db, reservation, charges):
            reserve(db, reservation, charges)
            raise RuntimeError('crash between budget and intent')
        try:
            with monkeypatch.context() as patch:
                patch.setattr(journal, 'reserve_budgets_in_transaction', fault)
                with pytest.raises(RuntimeError, match='between budget'):
                    service.effect(command('reserve', budget={'requests': 1}), lease=None)
            with journal._connection() as db:
                assert db.execute('SELECT count(*) FROM runtime_effects').fetchone()[0] == 0
                assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone() is None
            for _ in range(2):
                assert service.effect(command('reserve', budget={'requests': 1}), lease=None).status == 'reserved'
            with journal._connection() as db:
                assert db.execute("SELECT consumed FROM runtime_budgets WHERE name='requests'").fetchone()[0] == 1
            for _ in range(2):
                assert service.effect(command('mark_sent'), lease=None).status == 'sent'
            assert service.effect(command('unknown'), lease=None).status == 'unknown'
            with pytest.raises(DataflowError):
                service.effect(command('reserve', budget={'requests': 1}), lease=None)
            assert not (action.context.directory / '.dataflow-effects').exists()
        finally:
            service.close(deadline_ms=3000)


def test_confirmed_effect_is_immutable_and_legacy_unknown_not_replayed(tmp_path):
    with prepared(tmp_path, data.from_items([])) as (ex, services):
        service = DriverJournalService(ex._action, store=services.store, bounds=services.bounds)
        try:
            service.effect(command('reserve'), lease=None)
            service.effect(command('mark_sent'), lease=None)
            ref = services.store.put(iter([b'result']), max_bytes=64, media_type='application/json')
            confirmed = replace(command('confirm'), payload_ref=ref)
            for _ in range(2):
                assert service.effect(confirmed, lease=None).result_ref == ref
            changed = services.store.put(iter([b'changed']), max_bytes=64, media_type='application/json')
            with pytest.raises(DataflowError, match='result changed'):
                service.effect(replace(confirmed, payload_ref=changed), lease=None)
            with pytest.raises(DataflowError, match='different actor'):
                service.effect(replace(command('lookup'), actor_id=identity('other')), lease=None)
            legacy = ex._action.context.directory / '.dataflow-effects'
            legacy.mkdir()
            (legacy / (command('lookup').effect_id + '.json')).write_text('{"state":"sent"}')
            with pytest.raises(DataflowError, match='Legacy file effect'):
                service.effect(command('lookup'), lease=None)
        finally:
            service.close(deadline_ms=3000)
