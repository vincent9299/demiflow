"""Native non-deterministic model recovery and admission boundaries."""
import json
import os

import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.operator_llm import parse_prompt_pack
from image_http_fixture import fixture_http, definition, json_definition


def planned_children(row):
    return [{'instruction': row['plan'] + ':' + str(i)} for i in range(2)]


def fail_after_effect(row):
    if os.getenv('NATIVE_PLANNING_FAIL') == '1':
        raise RuntimeError('planned image was saved')
    return row


def test_nondeterministic_text_expansion_preserves_known_image_identity(tmp_path, fixture_http, monkeypatch):
    from image_http_fixture import fixture_http as image_fixture
    generator = image_fixture.__wrapped__()
    images = next(generator)
    monkeypatch.setenv('SYNTHETIC_JSON_KEY', 'fixture-only')
    fixture_http['response_override'] = {'choices': [{'message': {'content': '{"answer":"alpha"}'}}]}
    text = parse_prompt_pack(json.dumps(json_definition(fixture_http['base'])))
    image = parse_prompt_pack(json.dumps(definition(images['base'], template='{{ instruction }}')))
    def graph():
        return data.from_items([{'instruction': 'make a plan'}]).map_prompt_async(
            'answer', config=text, inputs=['instruction'], output='plan', max_requests=1,
            options={'trust_env': False, 'max_request_bytes': 8192, 'max_response_bytes': 8192, 'max_event_bytes': 8192}
        ).flat_map(planned_children).map_prompt_async(
            'generate', config=image, inputs=['instruction'], output='image', call_output='call',
            error_output='error', max_requests=2,
            options={'object_store': str(tmp_path/'assets'), 'image_encoding': 'preserve',
                'limits': {'max_images': 2, 'max_image_bytes': 4096, 'max_text_bytes': 1024,
                    'max_pixels': 64, 'max_total_pixels': 128, 'max_request_bytes': 8192,
                    'max_response_bytes': 8192}}
        ).map(fail_after_effect)
    limits = {'memory_bytes': 256*1024**2, 'max_row_bytes': 65536, 'max_batch_bytes': 262144,
              'checkpoint_rows': 512, 'checkpoint_interval_ms': 60000}
    try:
        monkeypatch.setenv('NATIVE_PLANNING_FAIL', '1')
        with pytest.raises(RuntimeError, match='planned image was saved'):
            with RunContext.start(tmp_path/'run', limits=limits) as run:
                reference = run.ref
                graph().materialize()
        assert len(fixture_http['requests']) == 1
        assert len(images['requests']) >= 1
        fixture_http['response_override'] = {'choices': [{'message': {'content': '{"answer":"beta"}'}}]}
        monkeypatch.delenv('NATIVE_PLANNING_FAIL')
        with RunContext.resume(reference):
            rows = graph().materialize().take_all()
        assert sorted(row['instruction'] for row in rows) == ['alpha:0', 'alpha:1']
        assert len(fixture_http['requests']) == 1
        assert len(images['requests']) == 2
        assert [r['body']['prompt'] for r in images['requests']] == ['alpha:0', 'alpha:1']
    finally:
        generator.close()


@pytest.mark.parametrize('confirmed_unsent', [True, False])
def test_only_durably_confirmed_unsent_request_can_be_reserved_again(tmp_path, confirmed_unsent):
    from demiflow.execution.journal.prompt import RunPromptJournal
    from demiflow.operator_llm.stream_recovery import CALL_RESERVATION
    from demiflow.operator_llm.journal import UncertainPromptCall
    reservation_token = CALL_RESERVATION.set(None)
    try:
        request = {'payload': 'same occurrence', 'native_response_bytes': 4096}
        with pytest.raises(RuntimeError, match='lost owner'):
            with RunContext.start(tmp_path/'run') as run:
                reference = run.ref
                journal = run._native_prompt_journal = RunPromptJournal(run)
                CALL_RESERVATION.set(('attempt-one', {'requests': (1, 10)}))
                assert journal.reserve(request)
                if confirmed_unsent:
                    error = RuntimeError('cancelled in transport batch queue')
                    error.call = {'request_sent': False, 'batch_cancelled_before_dispatch': True}
                    journal.failed(request, error, .01)
                raise RuntimeError('lost owner')
        with RunContext.resume(reference) as run:
            journal = run._native_prompt_journal = RunPromptJournal(run)
            if not confirmed_unsent:
                with pytest.raises(UncertainPromptCall, match='automatic regeneration is forbidden'):
                    journal.check_recovery()
                with pytest.raises(UncertainPromptCall):
                    journal.lookup(request)
                assert run.journal.budget_consumed('requests') == 1
                return
            journal.check_recovery()
            assert journal.lookup(request) is None
            CALL_RESERVATION.set(('attempt-two', {'requests': (1, 10)}))
            assert journal.reserve(request)
            journal.response(request, {'body': 'known result'})
            assert journal.lookup(request)['body'] == 'known result'
            assert run.journal.budget_consumed('requests') == 2
            with run.journal._connection() as db:
                assert db.execute('SELECT count(*) FROM recovery_events').fetchone()[0] == 1
                assert db.execute("SELECT value FROM journal_state WHERE name='native_response_reserved'").fetchone()[0] == '0'
    finally:
        CALL_RESERVATION.reset(reservation_token)


def test_control_capacity_rejection_precedes_provider_dispatch(tmp_path, fixture_http, monkeypatch):
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    monkeypatch.setenv('SYNTHETIC_JSON_KEY', 'fixture-only')
    config = parse_prompt_pack(json.dumps(json_definition(fixture_http['base'])))
    with pytest.raises(RecoveryContractError, match='before dispatch'):
        with RunContext.start(tmp_path/'run', limits={'max_control_bytes': 1024**2}):
            data.from_items([{'instruction': 'bounded'}]).map_prompt_async(
                'answer', config=config, inputs=['instruction'], output='answer', max_requests=1,
                options={'trust_env': False, 'max_response_bytes': 960*1024,
                         'max_request_bytes': 8192, 'max_event_bytes': 8192}
            ).materialize()
    assert fixture_http['requests'] == []


def test_received_image_finishes_durability_during_cooperative_cancel(tmp_path, fixture_http, monkeypatch):
    from demiflow.execution.outputs.object_sink import ObjectSink
    from test_managed_image_execution import native_dataset, run_limits
    original = ObjectSink.store_image
    cancelled = []

    def cancel_before_storage(store, raw, options):
        if not cancelled:
            cancelled.append(True)
            RunContext.cancel(store.run.ref)
        return original(store, raw, options)

    monkeypatch.setattr(ObjectSink, 'store_image', cancel_before_storage)
    with pytest.raises(Exception, match='cancellation requested'):
        with RunContext.start(tmp_path/'run', limits=run_limits()) as run:
            reference = run.ref
            native_dataset(tmp_path/'assets', fixture_http, 1).materialize()
    assert len(fixture_http['requests']) == 1
    with run.journal._connection() as db:
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 1
    with RunContext.resume(reference):
        rows = native_dataset(tmp_path/'assets', fixture_http, 1).materialize().take_all()
    assert len(rows) == 1 and rows[0]['pixels'] == fixture_http['raw']
    assert len(fixture_http['requests']) == 1
