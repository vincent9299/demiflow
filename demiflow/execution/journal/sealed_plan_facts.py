"""Sealed plan facts of one action, for hosts that display history.

A host showing "the plan this action actually ran" should not need to know that
a v3 seal is recorded under ``<action>-dataflow-sealed``, an original F1 seal
under ``<action>-sealed-plan`` and an identity bridge under
``<action>-identity-bridge``, nor which loader and view builder belong to which
generation. These readers name those facts once, in the engine.

Read-only: the journal is only read, sealed artifacts are loaded through the
existing bounded readers, nothing is recompiled and no plan is re-sealed.
"""
from demiflow.execution.contracts.errors import PlanContractError
from demiflow.execution.storage.reading import read_f1_carrier

__all__ = ['PlanContractError', 'recovery_restriction', 'sealed_plan_view',
           'sealed_plan_binding', 'identity_bridge', 'read_action_plan', 'read_verified_identity_bridge']

_V3_SEAL = '-dataflow-sealed'
_F1_SEAL = '-sealed-plan'
_IDENTITY_BRIDGE = '-identity-bridge'
_RECOVERY_RESTRICTION = '-state-recovery-restriction'


def _journal(directory, journal):
    if journal is not None:
        return journal
    from demiflow.execution.run_journal import RunJournal
    return RunJournal(directory)


def recovery_restriction(directory, action_id, *, journal=None):
    """The recorded state-recovery restriction of an action, or ``None``."""
    return _journal(directory, journal).source_binding(action_id + _RECOVERY_RESTRICTION)


def sealed_plan_view(directory, action_id, *, journal=None):
    """The v3 sealed plan of an action as the single plan view, or ``None``.

    ``None`` means the action has no v3 seal (an original F1 run, or no seal at
    all); use :func:`sealed_plan_binding` for the original generation. The
    original artifacts are read back; current code is never recompiled to
    impersonate history.
    """
    journal = _journal(directory, journal)
    marker = journal.source_binding(action_id + _V3_SEAL)
    if marker is None:
        return None
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from demiflow.execution.journal.dataflow_history import build_plan_view
    bundle = load_sealed_plan(directory, action_id)
    view = build_plan_view(bundle['graph'], bundle['sealed'], bundle['physical'], observations=())
    return dict(version=bundle['version'], action_id=action_id,
                dag_id=bundle['graph'].dag_id, plan_id=bundle['physical'].plan_id,
                sealed_at=marker.get('sealed_at') if type(marker) is dict else None,
                plan_view=view,
                recovery_restriction=journal.source_binding(action_id + _RECOVERY_RESTRICTION))


def sealed_plan_binding(directory, action_id, *, journal=None):
    """The original (F1) sealed-plan binding of an action, or ``None``.

    Returns ``{'facts': <recorded binding>, 'engine_run_id': <recovery identity
    run id or None>}``. The binding is returned as recorded; the host validates
    its schema and reads the carriers it references.
    """
    journal = _journal(directory, journal)
    facts = journal.source_binding(action_id + _F1_SEAL)
    if facts is None:
        return None
    binding = journal.recovery_identity()
    return dict(facts=facts, engine_run_id=binding.get('run_id') if type(binding) is dict else None)


def identity_bridge(directory, action_id, *, max_bytes, journal=None):
    """The sealed identity bridge of an action, or ``None`` when none was sealed.

    Returns ``{'ref': <recorded reference>, 'bridge': <artifact document>}``.
    Raises ``PlanContractError`` (with ``reason`` and ``detail``) when a recorded
    bridge cannot be read safely. Schema and identity checks of the document
    stay with the host. The bridge is never generated here.
    """
    ref = _journal(directory, journal).source_binding(action_id + _IDENTITY_BRIDGE)
    if ref is None:
        return None
    from demiflow.execution.storage.reading import read_plan_artifact
    return dict(ref=ref, bridge=read_plan_artifact(directory, ref, max_bytes=max_bytes))


def _check_read(deadline):
    import time
    import math
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
        raise PlanContractError('invalid_contract', 'Invalid historical deadline')
    if deadline is not None and time.monotonic() >= deadline:
        raise PlanContractError('artifact_too_large', 'Historical read deadline exceeded')





def _physical_summary(physical):
    if 'worker_groups' not in physical and 'exchanges' in physical:
        return physical
    roles = {s:g['role'] for g in physical['worker_groups'] for s in g['stage_refs']}
    stages = {s['logical_node_id']:dict(stage_id=s['stage_id'], partition_count=len(s['partitions']),
              budgets=s['budgets'], role=roles.get(s['stage_id'])) for s in physical['stages']}
    channels = [dict(channel_id=c['channel_id'], exchange_mode=c['exchange_mode'], routing=c['routing'],
                     durability=c['durability']['declared'], producer_count=len(c['producer_partitions']),
                     consumer_count=len(c['consumer_partitions'])) for c in physical['channels']]
    return dict(plan_id=physical['plan_id'], plan_revision=physical['plan_revision'], recovery=physical['recovery'],
                stages=stages, channels=channels, worker_groups=[{k:g[k] for k in ('group_id','role','min_workers','initial_workers','max_workers')} for g in physical['worker_groups']])


def read_action_plan(directory, action_id, *, max_bytes, deadline=None):
    """Validated original F1/v3 history, without recompilation or execution."""
    from demiflow.execution.journal.run_reader import _ReadonlyJournal
    if type(action_id) is not str or not 1 <= len(action_id) <= 128 or type(max_bytes) is not int or not 0 < max_bytes <= 4 * 1024**2:
        raise PlanContractError('invalid_contract', 'Invalid historical read selector/budget')
    _check_read(deadline)
    journal = _ReadonlyJournal(directory, deadline=deadline)
    marker = journal.source_binding(action_id + _V3_SEAL)
    identity = journal.recovery_identity()
    restriction = journal.source_binding(action_id + _RECOVERY_RESTRICTION)
    if marker is not None:
        from demiflow.execution.seal.sealed_plans import load_sealed_plan
        from demiflow.execution.journal.dataflow_history import build_plan_view
        if type(marker) is not dict:
            raise PlanContractError('invalid_contract', 'Invalid v3 seal marker')
        references = [marker.get(key) for key in ('logical_ref', 'physical_ref', 'code_pin_ref', 'identity_bridge_ref', 'constants_ref')]
        for key in ('programs', 'source_refs'):
            values = marker.get(key)
            if type(values) is not list or len(values) > 4096:
                raise PlanContractError('invalid_contract', 'Invalid v3 carrier list')
            references.extend(values)
        for ref in references:
            _check_read(deadline)
            if type(ref) is not dict or type(ref.get('size_bytes')) is not int or ref['size_bytes'] < 0:
                raise PlanContractError('invalid_contract', 'Invalid v3 carrier size')
            if ref['size_bytes'] > max_bytes:
                raise PlanContractError('artifact_too_large', 'V3 carrier exceeds read budget')
        bundle = load_sealed_plan(directory, action_id, _reader=journal,
                                  _max_carrier_bytes=max_bytes, _deadline=deadline)
        view = build_plan_view(bundle['graph'], bundle['sealed'], bundle['physical'], observations=())
        result = dict(version=bundle['version'], action_id=action_id, engine_run_id=(identity or {}).get('run_id'),
                      dag_id=bundle['graph'].dag_id, plan_id=bundle['physical'].plan_id,
                      sealed_at=marker.get('sealed_at'), plan_view=view, artifact_manifest_ref=None,
                      identity_bridge_ref=view.get('identity_bridge_ref'), code_pin_ref=view.get('code_pin_ref'),
                      recovery_restriction=restriction)
    else:
        facts = journal.source_binding(action_id + _F1_SEAL)
        if facts is None:
            return None
        if type(facts) is not dict or facts.get('schema') != 'demiflow.sealed_plan_facts.v1':
            raise PlanContractError('invalid_contract', 'F1 schema is unsupported')
        if facts.get('run_id') != (identity or {}).get('run_id') or facts.get('action_id') != action_id:
            raise PlanContractError('identity_mismatch', 'F1 run/action identity differs')
        if not {'dag_id','plan_id','sealed_at','artifact_manifest_ref','logical_dag_ref','physical_plan_ref'} <= facts.keys():
            raise PlanContractError('invalid_contract', 'F1 required field is missing')
        logical = read_f1_carrier(directory, facts['logical_dag_ref'], max_bytes)
        _check_read(deadline)
        physical = read_f1_carrier(directory, facts['physical_plan_ref'], max_bytes)
        from demiflow.planning.dag import validate_logical_dag, validate_physical_execution_plan
        try:
            validate_logical_dag(logical); validate_physical_execution_plan(physical)
        except (ValueError, TypeError, KeyError) as exc:
            raise PlanContractError('invalid_contract', str(exc)) from exc
        if logical['dag_id'] != facts['dag_id'] or physical['plan_id'] != facts['plan_id'] or logical['action_id'] != action_id:
            raise PlanContractError('identity_mismatch', 'F1 carrier identity differs')
        result = dict(version='F1', action_id=action_id, engine_run_id=(identity or {}).get('run_id'),
                      dag_id=facts['dag_id'], plan_id=facts['plan_id'], sealed_at=facts['sealed_at'],
                      plan_view=dict(dag=logical, node_map={}, physical=_physical_summary(physical), node_observations=[]),
                      artifact_manifest_ref=facts['artifact_manifest_ref'], identity_bridge_ref=None, code_pin_ref=None,
                      recovery_restriction=restriction, logical_dag=logical, physical_plan=physical)
        if journal.source_binding(action_id + _F1_SEAL) != facts:
            raise PlanContractError('identity_mismatch', 'F1 changed while reading')
    if journal.source_binding(action_id + _V3_SEAL) != marker or journal.recovery_identity() != identity:
        raise PlanContractError('identity_mismatch', 'Historical identity changed while reading')
    _check_read(deadline)
    return result


def read_verified_identity_bridge(directory, action_id, *, expected_dag_id, expected_report_digest,
                                  max_bytes, deadline=None):
    from demiflow.execution.journal.run_reader import _ReadonlyJournal
    from demiflow.execution.storage.reading import read_plan_artifact
    def bounded(value, maximum=128):
        return type(value) is str and 1 <= len(value) <= maximum
    if (not bounded(action_id) or not bounded(expected_dag_id) or not bounded(expected_report_digest,64)
            or type(max_bytes) is not int or not 0 < max_bytes <= 65536):
        raise PlanContractError('invalid_contract', 'Invalid bridge read selector/budget')
    _check_read(deadline)
    journal = _ReadonlyJournal(directory, deadline=deadline)
    ref = journal.source_binding(action_id + _IDENTITY_BRIDGE)
    if ref is None:
        return None
    bridge = read_plan_artifact(directory, ref, max_bytes=max_bytes)
    if bridge.get('schema') != 'demiflow.identity_bridge.v1':
        raise PlanContractError('invalid_contract', 'Bridge schema is unsupported')
    if bridge.get('action_id') != action_id or bridge.get('dag_id') != expected_dag_id or bridge.get('report_digest') != expected_report_digest:
        raise PlanContractError('identity_mismatch', 'Bridge action/dag/report identity differs')
    entries = bridge.get('entries')
    if type(entries) is not list or len(entries) > 1024:
        raise PlanContractError('invalid_contract', 'Bridge entries exceed budget')
    reports, logicals = set(), set()
    for entry in entries:
        if (type(entry) is not dict or not {'report_node_id','report_occurrence','logical_node_id','occurrence_id','mode'} <= entry.keys()
                or not bounded(entry['report_node_id'],64) or not bounded(entry['logical_node_id']) or not bounded(entry['occurrence_id'])
                or type(entry['report_occurrence']) is not int or not 0 <= entry['report_occurrence'] < 2**31 or entry['mode'] not in ('execute','reuse')):
            raise PlanContractError('invalid_contract', 'Bridge entry field is invalid')
        report = (entry['report_node_id'],entry['report_occurrence']); logical = (entry['logical_node_id'],entry['occurrence_id'])
        if report in reports or logical in logicals:
            raise PlanContractError('invalid_contract', 'Bridge observation key is ambiguous')
        reports.add(report); logicals.add(logical)
    if journal.source_binding(action_id + _IDENTITY_BRIDGE) != ref:
        raise PlanContractError('identity_mismatch', 'Bridge changed while reading')
    _check_read(deadline)
    return dict(ref=ref, bridge=bridge)
