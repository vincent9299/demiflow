"""Read-only D1a sealed-plan facts (F1) for historical run graphs (gap⑤, B).

Consumes the engine's attempt-independent F1 binding (``chain.seal_plan_facts``)
and its content-addressed carriers. Zero engine writes: the workbench only reads
the single RunJournal authority (``runtime_sources``, opened ``mode=ro``) and
validates the sealed documents against the frozen dag contracts.

A missing F1 never re-projects current code to impersonate a historical execution
graph; it degrades to an explicitly labelled declared projection. C-space physical
facts (stages/channels/worker groups) join the executed logical nodes directly by
``logical_node_id``; A-space observation joining awaits the D1b identity bridge and
is never guessed here.
"""
import hashlib
import json

from demiflow.forge.platform import bounded_json
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.planning.dag import validate_logical_dag, validate_physical_execution_plan

SCHEMA = "demiflow.sealed_plan_facts.v1"
MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2  # mirrors chain.MAX_SEALED_PLAN_BYTES
ACTION_BYTES = 96 * 1024               # mirrors logical.ACTION_BYTES
TOTAL_BYTES = 192 * 1024               # mirrors logical.TOTAL_BYTES

NO_FACTS = 'no_sealed_plan_facts'
NO_FACTS_DETAIL = ('该 action 无封存计划（pre-D1a 运行或非 chain 终止 action）；'
                   '以下为依据封存报告投影的声明图，非本次执行封存计划。')


class _CarrierError(Exception):
    """Internal carrier failure carrying a precise public reason code."""

    def __init__(self, reason, detail):
        self.reason = reason
        super().__init__(detail)


def _unavailable(action_id, reason, detail):
    return dict(action_id=action_id, status='unavailable', reason=reason, detail=detail)


def _carrier(target, ref):
    """Bounded, sha-gated, symlink-refusing read of one content-addressed carrier."""
    from demiflow.workbench.projects.io import relative_path
    if type(ref) is not dict or set(ref) != {'sha256', 'path', 'bytes'}:
        raise _CarrierError('sealed_plan_facts_invalid', 'sealed-plan reference key set is unsupported')
    if type(ref['sha256']) is not str or len(ref['sha256']) != 64:
        raise _CarrierError('sealed_plan_facts_invalid', 'sealed-plan reference sha256 is invalid')
    if type(ref['bytes']) is not int or not 0 <= ref['bytes'] <= MAX_SEALED_PLAN_BYTES:
        raise _CarrierError('sealed_plan_facts_invalid', 'sealed-plan reference bytes is invalid')
    try:
        path = relative_path(target, ref['path'])
    except ForgeError:
        raise _CarrierError('sealed_plan_carrier_unsafe', 'sealed-plan carrier path escapes the run root')
    try:
        raw = read_bounded(path, MAX_SEALED_PLAN_BYTES)
    except ForgeError as exc:
        if exc.code == 'resource_limit':
            raise _CarrierError('sealed_plan_carrier_oversize', 'sealed-plan carrier exceeds byte budget')
        if exc.code == 'conflict':
            raise _CarrierError('sealed_plan_carrier_corrupt', 'sealed-plan carrier changed while reading')
        raise _CarrierError('sealed_plan_carrier_unsafe', 'sealed-plan carrier is unsafe or unreadable')
    if len(raw) != ref['bytes'] or hashlib.sha256(raw).hexdigest() != ref['sha256']:
        raise _CarrierError('sealed_plan_carrier_corrupt', 'sealed-plan carrier digest differs')
    try:
        return json.loads(raw)
    except ValueError:
        raise _CarrierError('sealed_plan_carrier_corrupt', 'sealed-plan carrier is not valid JSON')


def read_sealed_plan(target, action_id):
    """Read one action's F1 + carriers. Never raises; returns available/unavailable.

    ``target`` is the run root (``manager.directory``); F1 carrier paths are relative
    to it. The lookup key is the recorded action id (``value.identity`` == a report
    node id), never a recomputed identity.
    """
    if type(action_id) is not str or not 1 <= len(action_id) <= 128:
        return _unavailable(action_id, 'invalid_action_id', 'action id is not a bounded string')
    try:
        from demiflow.execution.run_journal import RunJournal
        journal = RunJournal(target)
        facts = journal.source_binding(action_id + '-sealed-plan')
        if facts is None:
            return _unavailable(action_id, NO_FACTS, NO_FACTS_DETAIL)
        if type(facts) is not dict or facts.get('schema') != SCHEMA:
            return _unavailable(action_id, 'sealed_plan_facts_invalid', 'F1 schema is unsupported')
        # F1.run_id is the engine recovery identity (uuid), not the workbench
        # directory basename; bind it to this run's immutable engine identity.
        binding = journal.recovery_identity()
        engine_run_id = binding.get('run_id') if type(binding) is dict else None
        if facts.get('run_id') != engine_run_id or facts.get('action_id') != action_id:
            return _unavailable(action_id, 'sealed_plan_facts_invalid', 'F1 run/action identity differs')
        for key in ('dag_id', 'plan_id', 'sealed_at', 'artifact_manifest_ref',
                    'logical_dag_ref', 'physical_plan_ref'):
            if key not in facts:
                return _unavailable(action_id, 'sealed_plan_facts_invalid', 'F1 missing ' + key)
        logical_dag = _carrier(target, facts['logical_dag_ref'])
        physical = _carrier(target, facts['physical_plan_ref'])
        validate_logical_dag(logical_dag)
        validate_physical_execution_plan(physical)
        if logical_dag['dag_id'] != facts['dag_id'] or physical['plan_id'] != facts['plan_id']:
            return _unavailable(action_id, 'sealed_plan_identity_mismatch',
                                'sealed dag_id/plan_id differs from F1')
        if logical_dag['action_id'] != action_id:
            return _unavailable(action_id, 'sealed_plan_identity_mismatch',
                                'sealed dag action_id differs from F1')
        return dict(available=True, action_id=action_id, dag_id=facts['dag_id'],
                    plan_id=facts['plan_id'], sealed_at=facts['sealed_at'],
                    artifact_manifest_ref=facts['artifact_manifest_ref'],
                    logical_dag=logical_dag, physical_plan=physical)
    except _CarrierError as exc:
        return _unavailable(action_id, exc.reason, str(exc)[:512])
    except ForgeError as exc:
        reason = 'sealed_plan_carrier_oversize' if exc.code == 'resource_limit' else 'sealed_plan_facts_invalid'
        return _unavailable(action_id, reason, str(exc)[:512])
    except (ValueError, KeyError, TypeError) as exc:
        return _unavailable(action_id, 'sealed_plan_contract_invalid', str(exc)[:512])
    except Exception as exc:
        return _unavailable(action_id, 'sealed_plan_read_failed', str(exc)[:512])


def _physical_summary(physical):
    """Compact C-space execution facts that join executed logical nodes directly."""
    role_by_stage = {}
    for group in physical['worker_groups']:
        for stage_ref in group['stage_refs']:
            role_by_stage[stage_ref] = group['role']
    stages = {}
    for stage in physical['stages']:
        stages[stage['logical_node_id']] = dict(
            stage_id=stage['stage_id'], partition_count=len(stage['partitions']),
            budgets=stage['budgets'], role=role_by_stage.get(stage['stage_id']))
    channels = [dict(channel_id=c['channel_id'], exchange_mode=c['exchange_mode'],
                     routing=c['routing'], durability=c['durability']['declared'],
                     producer_count=len(c['producer_partitions']),
                     consumer_count=len(c['consumer_partitions']))
                for c in physical['channels']]
    return dict(plan_id=physical['plan_id'], plan_revision=physical['plan_revision'],
                recovery=physical['recovery'], stages=stages, channels=channels,
                worker_groups=[dict(group_id=g['group_id'], role=g['role'],
                                    min_workers=g['min_workers'], initial_workers=g['initial_workers'],
                                    max_workers=g['max_workers'])
                               for g in physical['worker_groups']])


def _sealed_entry(result):
    entry = dict(action_id=result['action_id'], status='sealed', dag=result['logical_dag'],
                 node_map={}, plan_id=result['plan_id'], sealed_at=result['sealed_at'],
                 artifact_manifest_ref=result['artifact_manifest_ref'],
                 physical=_physical_summary(result['physical_plan']))
    bounded_json(entry, ACTION_BYTES)
    return entry


def overlay_sealed(root, run_id, declared_view):
    """Overlay executed sealed plans onto the declared projection for a run graph.

    Per action: F1 present -> executed C-space entry; F1 absent -> keep the declared
    projection with an explicit fallback marker (never impersonate); F1 corrupt ->
    structured unavailable. One action's failure never fails the whole graph.
    """
    from demiflow.workbench.runs import manager
    target = manager.directory(root, run_id)
    view = dict(declared_view)
    entries, sealed_any, used = [], False, 0
    for entry in declared_view['actions']:
        action_id = entry.get('action_id')
        result = read_sealed_plan(target, action_id) if type(action_id) is str else None
        if result is not None and result.get('available'):
            try:
                new = _sealed_entry(result)
                sealed_any = True
            except ForgeError:
                new = _unavailable(action_id, 'sealed_plan_budget', '执行封存计划超出展示预算')
        elif result is not None and result['reason'] == NO_FACTS:
            new = dict(entry)
            if new.get('status') == 'projected':
                new['fallback'] = dict(reason=NO_FACTS, detail=NO_FACTS_DETAIL)
        elif result is not None:
            new = result
        else:
            new = dict(entry)
        size = len(bounded_json(new, ACTION_BYTES)) + 1
        if used + size > TOTAL_BYTES:
            view['truncated'] = True
            break
        entries.append(new)
        used += size
    view['actions'] = entries
    view['basis'] = 'executed' if sealed_any else 'declared'
    view['run_sealed'] = dict(available=sealed_any,
                              reason=None if sealed_any else NO_FACTS,
                              detail=None if sealed_any else NO_FACTS_DETAIL)
    return view