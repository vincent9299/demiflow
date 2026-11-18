"""Shadow the workbench's journal and sealed-plan readers with the engine's.

``execution.run_reader`` and ``execution.sealed_plan_facts`` hold the queries
that ``workbench/runs/view.py``, ``storage.py`` and ``facts.py`` still carry.
Until the workbench is switched over both exist. Importing ``shadow`` into a
test module makes every host read in that module also run the engine reader on
the same journal and assert the same answer, so the switch is a provable no-op.

This is a test helper, not a test module. Delete it with the host copies.
"""
import base64
from collections import Counter
import json
from pathlib import Path

import pytest

import demiflow
from demiflow.execution import run_reader, sealed_plan_facts
from demiflow.execution.run_reader import RunReader, RunReadError, read_run
from demiflow.forge.schema import ForgeError

CALLS = Counter()
_PAGE_SCOPE = ('plan_id', 'dag_id', 'plan_revision')


def _attempt(call):
    try:
        return call(), None
    except Exception as exc:  # compared below, then re-raised
        return None, exc


def _same_error(host, engine):
    if isinstance(engine, RunReadError):
        # The host presents engine read refusals as ForgeError with the same
        # code and text; only the generic JSON byte-budget wording differs.
        return (isinstance(host, ForgeError) and host.code == (engine.code or 'operation_rejected')
                and (str(host) == str(engine) or 'byte budget' in str(host)))
    return type(host) is type(engine) and str(host) == str(engine)


def compare(name, host, engine):
    """Run both, require the same value or the same failure, behave as the host."""
    CALLS[name] += 1
    old, old_error = _attempt(host)
    new, new_error = _attempt(engine)
    if old_error is not None or new_error is not None:
        assert old_error is not None and new_error is not None and _same_error(old_error, new_error), \
            f'{name}: host raised {old_error!r}, engine raised {new_error!r}'
        CALLS[name + ':error'] += 1
        raise old_error
    assert new == old, f'{name}: engine {new!r} != host {old!r}'
    return old


def output_projection(candidates, attempt, active):
    """The host's output status decision, fed from ``RunReader.output_facts``."""
    facts, retained = candidates.get(attempt, {}), None
    def published(found):
        return ('stage_commit' in found or 'stage_recovered' in found
                or any(found.get(kind, (None,))[0] == 'committed' for kind in ('receipt', 'recovered')))
    if not any(kind in facts for kind in ('receipt', 'recovered', 'stage_commit', 'stage_recovered')):
        for found, previous in candidates.items():
            if found != attempt and published(previous):
                facts, retained = previous, found
                break
    status = ('committed' if published(facts)
              else (('writing' if active else 'unknown') if 'intent' in facts else 'not_started'))
    reference = (facts.get('recovered_result') or facts.get('result') or (None, None))[1]
    return status, retained, json.loads(reference) if reference else None


@pytest.fixture(autouse=True)
def shadow(monkeypatch):
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1]), \
        'tests are not importing the tree they live in'
    from demiflow.workbench.runs import facts, manager as runs, storage, view

    host_state, host_revision = view._state, view._observation_revision
    monkeypatch.setattr(view, '_state', lambda db, attempt: compare(
        'state', lambda: host_state(db, attempt), lambda: RunReader(db).state(attempt)))
    monkeypatch.setattr(view, '_observation_revision', lambda db, attempt: compare(
        'observation_revision', lambda: host_revision(db, attempt),
        lambda: RunReader(db).observation_revision(attempt)))

    host_storage = storage.storage_snapshot
    monkeypatch.setattr(storage, 'storage_snapshot', lambda directory: compare(
        'storage_snapshot', lambda: host_storage(directory), lambda: run_reader.storage_snapshot(directory)))

    host_observations = view.observations

    def observations(root, run_id, *, attempt, node_id=None, action_id=None, cursor=None, limit=32):
        result = host_observations(root, run_id, attempt=attempt, node_id=node_id,
                                   action_id=action_id, cursor=cursor, limit=limit)
        if result.get('availability') != 'available':
            return result
        after = json.loads(base64.b64decode(cursor, altchars=b'-_'))['after'] if cursor else None
        with read_run(runs.directory(root, run_id)) as reader:
            if reader.observation_revision(attempt)[0] != result['observation_revision']:
                CALLS['observations:changed'] += 1
                return result
            assert reader.observations_supported()
            rows = list(reader.observation_rows(attempt, node_id=node_id, action_id=action_id,
                                                after=after, limit=len(result['observations'])))
            assert rows == result['observations'], 'observation rows differ'
            scope = reader.observation_scope(attempt, node_id=node_id, action_id=action_id)
            assert {key: result.get(key) for key in _PAGE_SCOPE} == (scope or dict.fromkeys(_PAGE_SCOPE))
            identity = reader.manifest_identity()
            assert (identity['root'], identity['run_id'], identity['digest']) == (
                str(Path(root).absolute()), run_id, result['binding']['seal_digest'])
        CALLS['observations'] += 1
        CALLS['observation_rows'] += len(rows)
        return result

    host_snapshot = view.snapshot

    def snapshot(root, run_id, *, attempt=None, revision=None):
        result = host_snapshot(root, run_id, attempt=attempt, revision=revision)
        if result.get('availability') != 'available':
            if result.get('reason') == 'historical_runtime_contract':
                with read_run(runs.directory(root, run_id)) as reader:
                    assert reader.runtime_summary() is None
                CALLS['snapshot:historical'] += 1
            return result
        selected = result['attempt']
        with read_run(runs.directory(root, run_id)) as reader:
            state, current = reader.state(selected)
            if (state != result['state'] or reader.action_sequence(selected) != result['action_sequence']
                    or reader.observation_revision(selected) != (
                        result['observation_revision'], result['observation_count'], result['node_sequence'])):
                CALLS['snapshot:changed'] += 1
                return result
            assert current == result['current_attempt']
            summaries = reader.observation_summaries(selected)
            assert summaries[:64] == result['observations'], 'observation summaries differ'
            assert (len(summaries) > 64) == result['observations_truncated']
            summary = reader.runtime_summary()
            assert (summary['entry'], summary['digest'], summary['limits']) == (
                result['entry'], result['digest'], result['limits'])
            assert set(summary['outputs']) == {output['name'] for output in result['outputs']}
            for output in result['outputs']:
                projected = output_projection(reader.output_facts(selected, output['action_id']),
                                              selected, state['status'] in runs.ACTIVE)
                assert projected == (output['status'], output['retained_from_attempt'],
                                     output['result']['ref'] if output['result'] else None), output['name']
                CALLS['snapshot_outputs'] += 1
        CALLS['snapshot'] += 1
        return result

    host_events = view.events

    def events(root, run_id, *, attempt=None, cursor=None, limit=100):
        result = host_events(root, run_id, attempt=attempt, cursor=cursor, limit=limit)
        with read_run(runs.directory(root, run_id)) as reader:
            if result.get('availability') == 'unsupported':
                assert reader.control_sequence(reader.state(attempt)[0].get('attempt')) is None
                CALLS['events:unsupported'] += 1
                return result
            after = int(cursor.split(':')[2]) if cursor else 0
            rows = [dict(sequence=sequence, **json.loads(raw)) for sequence, raw in
                    reader.control_events(result['attempt'], after=after, limit=len(result['events']))]
            assert rows == result['events'], 'control events differ'
            assert reader.control_sequence(result['attempt']) >= (rows[-1]['sequence'] if rows else after)
        CALLS['events'] += 1
        return result

    host_context = view.context_facts

    def context_facts(root, run_id, *, attempt=None, deadline=None):
        result = host_context(root, run_id, attempt=attempt, deadline=deadline)
        with read_run(runs.directory(root, run_id)) as reader:
            selected, current = reader.state_projection(attempt)
            identity = reader.manifest_identity()
            assert (identity['digest'], identity['format']) == (result['seal_digest'], result['source_format'])
            assert current['attempt'] == result['current_attempt']
            if selected.get('revision') == result['state'].get('revision'):
                assert selected == result['state'], 'state projection differs'
                CALLS['context_facts'] += 1
        return result

    host_plan = facts.read_sealed_plan

    def read_sealed_plan(target, action_id):
        result = host_plan(target, action_id)
        if result.get('available'):
            engine = sealed_plan_facts.sealed_plan_view(target, action_id)
            if result.get('version') == 'v3':
                assert engine == {key: value for key, value in result.items() if key != 'available'}, \
                    'v3 sealed plan view differs'
                CALLS['plan:v3'] += 1
            else:
                assert engine is None
                binding = sealed_plan_facts.sealed_plan_binding(target, action_id)
                assert (binding['facts']['dag_id'], binding['facts']['plan_id'], binding['facts']['sealed_at']) == (
                    result['dag_id'], result['plan_id'], result['sealed_at'])
                assert sealed_plan_facts.recovery_restriction(target, action_id) == result['recovery_restriction']
                CALLS['plan:f1'] += 1
        elif result.get('reason') == facts.NO_FACTS:
            assert sealed_plan_facts.sealed_plan_view(target, action_id) is None
            assert sealed_plan_facts.sealed_plan_binding(target, action_id) is None
            CALLS['plan:absent'] += 1
        return result

    host_bridge = facts.read_identity_bridge

    def read_identity_bridge(target, action_id, *, dag_id, report_digest):
        result = host_bridge(target, action_id, dag_id=dag_id, report_digest=report_digest)
        def engine():
            return sealed_plan_facts.identity_bridge(target, action_id, max_bytes=facts.MAX_IDENTITY_BRIDGE_BYTES)
        if result.get('available'):
            assert engine() == dict(ref=result['ref'], bridge=result['bridge']), 'identity bridge differs'
            CALLS['bridge'] += 1
        elif result.get('reason') in ('identity_bridge_unsafe', 'identity_bridge_oversize', 'identity_bridge_corrupt'):
            with pytest.raises(sealed_plan_facts.PlanContractError) as caught:
                engine()
            assert facts._BRIDGE_REASON.get(caught.value.reason, 'identity_bridge_invalid') == result['reason']
            CALLS['bridge:error'] += 1
        return result

    for name, replacement in (('observations', observations), ('snapshot', snapshot), ('events', events),
                              ('context_facts', context_facts)):
        monkeypatch.setattr(view, name, replacement)
    monkeypatch.setattr(facts, 'read_sealed_plan', read_sealed_plan)
    monkeypatch.setattr(facts, 'read_identity_bridge', read_identity_bridge)


@pytest.fixture(scope='module', autouse=True)
def compared(request):
    """Comparisons made while this module's tests ran (a ``Counter``)."""
    start = CALLS.copy()
    return lambda: CALLS - start
