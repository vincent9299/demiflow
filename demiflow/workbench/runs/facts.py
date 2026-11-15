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

BRIDGE_SCHEMA = 'demiflow.identity_bridge.v1'
MAX_IDENTITY_BRIDGE_BYTES = 64 * 1024
NO_BRIDGE = 'identity_bridge_missing'
NO_BRIDGE_DETAIL = ('该 action 尚无节点身份桥（旧运行或本次未生成桥）；'
                    '封存计划仍可查看，执行节点与报告观测的对应不猜测。')

# PlanContractError.reason (parallel-contracts-v1 §2.3) -> workbench unavailable reason.
_BRIDGE_REASON = {
    'missing_plan': NO_BRIDGE,
    'missing_bindings': NO_BRIDGE,
    'unsafe_artifact': 'identity_bridge_unsafe',
    'artifact_too_large': 'identity_bridge_oversize',
    'corrupt_artifact': 'identity_bridge_corrupt',
    'identity_mismatch': 'identity_bridge_mismatch',
    'invalid_contract': 'identity_bridge_invalid',
    'unsupported_layout': 'identity_bridge_invalid',
    'invalid_assignment': 'identity_bridge_invalid',
}
_BRIDGE_ENTRY_KEYS = {'report_node_id', 'report_occurrence', 'logical_node_id', 'occurrence_id', 'mode'}


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
        v3_marker = journal.source_binding(action_id + '-dataflow-sealed')
        if v3_marker is not None:
            # v3 native seal: read the original artifacts (no recompile) and project
            # them through the single plan-view authority. History stays read-only.
            from demiflow.execution.sealed_plans import load_sealed_plan
            from demiflow.execution.dataflow_history import build_plan_view
            bundle = load_sealed_plan(target, action_id)
            view = build_plan_view(bundle['graph'], bundle['sealed'], bundle['physical'],
                                   observations=())
            return dict(available=True, version='v3', action_id=action_id,
                        dag_id=bundle['graph'].dag_id, plan_id=bundle['physical'].plan_id,
                        sealed_at=v3_marker.get('sealed_at') if type(v3_marker) is dict else None,
                        plan_view=view,
                        recovery_restriction=journal.source_binding(action_id + '-state-recovery-restriction'))
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
                    logical_dag=logical_dag, physical_plan=physical,
                    recovery_restriction=journal.source_binding(action_id + '-state-recovery-restriction'))
    except _CarrierError as exc:
        return _unavailable(action_id, exc.reason, str(exc)[:512])
    except ForgeError as exc:
        reason = 'sealed_plan_carrier_oversize' if exc.code == 'resource_limit' else 'sealed_plan_facts_invalid'
        return _unavailable(action_id, reason, str(exc)[:512])
    except (ValueError, KeyError, TypeError) as exc:
        return _unavailable(action_id, 'sealed_plan_contract_invalid', str(exc)[:512])
    except Exception as exc:
        return _unavailable(action_id, 'sealed_plan_read_failed', str(exc)[:512])


def _bridge_unavailable(reason, detail):
    return dict(available=False, reason=reason, detail=detail)


def _bounded_str(value, maximum=128):
    return type(value) is str and 1 <= len(value) <= maximum


def read_identity_bridge(target, action_id, *, dag_id, report_digest):
    """Read one action's sealed identity bridge (C-produced) without generating it.

    Returns ``{available: True, ref, bridge}`` on success, or
    ``{available: False, reason, detail}`` when the bridge is absent/invalid. The
    carrier is read through B's neutral bounded reader (``read_plan_artifact``);
    the action/dag/report triple identity is re-checked here. A missing bridge is
    an honest degradation, never a guessed correspondence. Unexpected program
    errors (e.g. the reader module being absent while a ref exists) propagate.
    """
    if not _bounded_str(action_id):
        return _bridge_unavailable('identity_bridge_invalid', 'action id is not a bounded string')
    if not _bounded_str(dag_id) or not _bounded_str(report_digest, 64):
        return _bridge_unavailable('identity_bridge_invalid', 'dag_id/report_digest identity is not bounded')
    from demiflow.execution.run_journal import RunJournal
    try:
        ref = RunJournal(target).source_binding(action_id + '-identity-bridge')
    except (OSError, ValueError, ForgeError) as exc:
        return _bridge_unavailable('identity_bridge_read_failed', str(exc)[:512])
    if ref is None:
        return _bridge_unavailable(NO_BRIDGE, NO_BRIDGE_DETAIL)
    # B's reader/exception are imported lazily: historical runs carry no bridge
    # ref, so this dependency is only touched when a real bridge exists.
    from demiflow.execution.sealed_plans import read_plan_artifact
    from demiflow.execution.plan_interpreter import PlanContractError
    try:
        bridge = read_plan_artifact(target, ref, max_bytes=MAX_IDENTITY_BRIDGE_BYTES)
    except PlanContractError as exc:
        reason = _BRIDGE_REASON.get(exc.reason, 'identity_bridge_invalid')
        return _bridge_unavailable(reason, str(exc.detail)[:512])
    if type(bridge) is not dict or bridge.get('schema') != BRIDGE_SCHEMA:
        return _bridge_unavailable('identity_bridge_invalid', 'identity bridge schema is unsupported')
    if (bridge.get('action_id') != action_id or bridge.get('dag_id') != dag_id
            or bridge.get('report_digest') != report_digest):
        return _bridge_unavailable('identity_bridge_mismatch', 'bridge action/dag/report identity differs')
    entries = bridge.get('entries')
    if type(entries) is not list or len(entries) > 1024:
        return _bridge_unavailable('identity_bridge_invalid', 'bridge entries are not a bounded list')
    report_keys, logical_keys = set(), set()
    for entry in entries:
        if type(entry) is not dict or not _BRIDGE_ENTRY_KEYS <= set(entry):
            return _bridge_unavailable('identity_bridge_invalid', 'bridge entry key set is unsupported')
        if (not _bounded_str(entry['report_node_id'], 64) or not _bounded_str(entry['logical_node_id'])
                or not _bounded_str(entry['occurrence_id'])
                or type(entry['report_occurrence']) is not int or not 0 <= entry['report_occurrence'] < 2**31
                or entry['mode'] not in ('execute', 'reuse')):
            return _bridge_unavailable('identity_bridge_invalid', 'bridge entry field is invalid')
        report_key = (entry['report_node_id'], entry['report_occurrence'])
        logical_key = (entry['logical_node_id'], entry['occurrence_id'])
        if report_key in report_keys or logical_key in logical_keys:
            return _bridge_unavailable('identity_bridge_invalid', 'bridge observation key is ambiguous')
        report_keys.add(report_key)
        logical_keys.add(logical_key)
    return dict(available=True, ref=ref, bridge=bridge)


def join_node_observations(bridge, observations, *, action_id, attempt):
    """Join one observation page onto a bridge by exact report observation key.

    ``observations`` is a complete ``view.observations`` page document. The page's
    attempt/action selection scope is checked first; rows then match by
    ``(action_id, report_node_id, report_occurrence)``. Each output item keeps the
    bridge entry and adds ``observation`` (the original row or None) and
    ``availability`` (``observed``/``not_in_page``). A page absence is NOT evidence
    that the run never observed the node; cursor/revision stay with the existing
    query. Scope mismatches raise rather than guess.
    """
    if type(bridge) is not dict or bridge.get('action_id') != action_id or type(bridge.get('entries')) is not list:
        raise ForgeError('Identity bridge does not belong to this action', code='invalid_request')
    if type(observations) is not dict or observations.get('format') != 'demiflow_forge_node_observation_page_v1':
        raise ForgeError('Observation page format is unsupported', code='invalid_request')
    if observations.get('attempt') != attempt:
        raise ForgeError('Observation page attempt differs from the selected attempt', code='conflict')
    page_action = observations.get('action_id')
    if page_action is not None and page_action != action_id:
        raise ForgeError('Observation page is scoped to a different action', code='conflict')
    rows = observations.get('observations')
    if type(rows) is not list:
        raise ForgeError('Observation page rows are invalid', code='invalid_request')
    # Composite key is action/attempt/occurrence/plan: rows are matched by report
    # node identity + occurrence + plan, never merged by function name. A row from
    # a different plan than the page is not joined (old pages without plan_id keep
    # the previous occurrence-only behaviour).
    page_plan = observations.get('plan_id')
    index = {}
    for row in rows:
        if type(row) is not dict or row.get('action_id') != action_id:
            continue
        row_plan = row.get('plan_id', page_plan)
        if page_plan is not None and row_plan != page_plan:
            continue
        key = (row.get('node_id'), row.get('occurrence'), row_plan)
        if key not in index:
            index[key] = row
    joined = []
    for entry in bridge['entries']:
        key = (entry.get('report_node_id'), entry.get('report_occurrence'), page_plan)
        row = index.get(key)
        joined.append(dict(entry, observation=row, availability='observed' if row is not None else 'not_in_page'))
    return tuple(joined)


def _physical_summary(physical):
    """Compact C-space execution facts that join executed logical nodes directly.

    A v3 physical view (projected by ``build_plan_view``) already carries the real
    stage profile/partition counts, exchange mode/routing/reason and recovery
    limitations; it is passed through unchanged so nothing is re-derived or
    inferred. The v1/v2 layout below is preserved for historical runs.
    """
    if 'worker_groups' not in physical and 'exchanges' in physical:
        return physical
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


def _bridge_node_map(entries):
    """Unambiguous logical_node_id -> report_node_id; multi-instance ids stay out.

    A logical node reused at several call sites maps to several report nodes; a
    single map cannot represent that without guessing, so those ids are omitted
    and the frontend resolves them per occurrence from the full bridge entries.
    """
    targets = {}
    for entry in entries:
        targets.setdefault(entry['logical_node_id'], set()).add(entry['report_node_id'])
    return {logical: next(iter(report)) for logical, report in targets.items() if len(report) == 1}


def _sealed_entry(result, *, bridge=None):
    if result.get('version') == 'v3':
        view = result['plan_view']
        entry = dict(action_id=result['action_id'], status='sealed', binding_level='sealed',
                     version='v3', dag=view['dag'], node_map=dict(view.get('node_map') or {}),
                     plan_id=result['plan_id'], sealed_at=result.get('sealed_at'),
                     physical=_physical_summary(view['physical']),
                     node_observations=view['node_observations'],
                     identity_bridge_ref=view.get('identity_bridge_ref'),
                     code_pin_ref=view.get('code_pin_ref'))
    else:
        entry = dict(action_id=result['action_id'], status='sealed', binding_level='sealed',
                     dag=result['logical_dag'], node_map={}, plan_id=result['plan_id'],
                     sealed_at=result['sealed_at'],
                     artifact_manifest_ref=result['artifact_manifest_ref'],
                     physical=_physical_summary(result['physical_plan']))
    if result.get('recovery_restriction') is not None:
        entry['recovery_restriction'] = result['recovery_restriction']
    if bridge is not None and bridge.get('available'):
        entry['node_map'] = _bridge_node_map(bridge['bridge']['entries'])
        if result.get('version') == 'v3':
            entry['node_map'].update({e['occurrence_id']: e['report_node_id'] for e in bridge['bridge']['entries']})
        entry['identity_bridge'] = bridge
    else:
        entry['identity_bridge'] = bridge if bridge is not None else _bridge_unavailable(NO_BRIDGE, NO_BRIDGE_DETAIL)
    bounded_json(entry, ACTION_BYTES)
    return entry


def overlay_sealed(root, run_id, declared_view, *, report_digest=None):
    """Overlay executed sealed plans onto the declared projection for a run graph.

    Per action: F1 present -> executed C-space entry; F1 absent -> keep the declared
    projection with an explicit fallback marker (never impersonate); F1 corrupt ->
    structured unavailable. One action's failure never fails the whole graph. When
    ``report_digest`` is given, each sealed action also reads its identity bridge
    (attempt-independent); observation joining stays attempt-scoped in the caller.
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
                bridge = (read_identity_bridge(target, action_id, dag_id=result['dag_id'],
                                               report_digest=report_digest)
                          if report_digest is not None else None)
                new = _sealed_entry(result, bridge=bridge)
                sealed_any = True
            except ForgeError:
                new = _unavailable(action_id, 'sealed_plan_budget', '执行封存计划超出展示预算')
        elif result is not None and result['reason'] == NO_FACTS:
            new = dict(entry)
            new['binding_level'] = 'declared'
            if new.get('status') == 'projected':
                new['fallback'] = dict(reason=NO_FACTS, detail=NO_FACTS_DETAIL)
        elif result is not None:
            new = result
        else:
            new = dict(entry)
            new['binding_level'] = 'declared'
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
