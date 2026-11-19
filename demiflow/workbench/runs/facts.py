"""Presentation of validated sealed plans and identity bridges.

The native inspection owner reads and validates original historical carriers.
This adapter supplies unavailable labels, declared-view fallback, bounded graph
rendering and occurrence-based observation joins; it never invents history.
"""

from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError

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


def _unavailable(action_id, reason, detail):
    return dict(action_id=action_id, status='unavailable', reason=reason, detail=detail)


def _bridge_unavailable(reason, detail):
    return dict(available=False, reason=reason, detail=detail)


def read_sealed_plan(target, action_id):
    from demiflow.execution.host.inspection import read_action_plan, PlanContractError
    if type(action_id) is not str or not 1 <= len(action_id) <= 128:
        return _unavailable(action_id, 'invalid_action_id', 'action id is not a bounded string')
    try:
        result = read_action_plan(target, action_id, max_bytes=MAX_SEALED_PLAN_BYTES)
        return dict(available=True, **result) if result is not None else _unavailable(action_id, NO_FACTS, NO_FACTS_DETAIL)
    except PlanContractError as exc:
        reason = {'unsafe_artifact':'sealed_plan_carrier_unsafe', 'artifact_too_large':'sealed_plan_carrier_oversize',
                  'corrupt_artifact':'sealed_plan_carrier_corrupt', 'identity_mismatch':'sealed_plan_identity_mismatch'}.get(exc.reason, 'sealed_plan_contract_invalid')
        return _unavailable(action_id, reason, str(exc.detail)[:512])
    except Exception as exc:
        return _unavailable(action_id, 'sealed_plan_read_failed', str(exc)[:512])


def read_identity_bridge(target, action_id, *, dag_id, report_digest):
    from demiflow.execution.host.inspection import read_verified_identity_bridge, PlanContractError
    try:
        result = read_verified_identity_bridge(target, action_id, expected_dag_id=dag_id,
                    expected_report_digest=report_digest, max_bytes=MAX_IDENTITY_BRIDGE_BYTES)
        return dict(available=True, **result) if result is not None else _bridge_unavailable(NO_BRIDGE, NO_BRIDGE_DETAIL)
    except PlanContractError as exc:
        return _bridge_unavailable(_BRIDGE_REASON.get(exc.reason, 'identity_bridge_invalid'), str(exc.detail)[:512])
    except (OSError, ValueError, ForgeError) as exc:
        return _bridge_unavailable('identity_bridge_read_failed', str(exc)[:512])


def join_node_observations(bridge, observations, *, action_id, attempt, plan_id=None, dag_id=None, plan_revision=None):
    """Join one observation page onto a bridge by exact report observation key.

    ``observations`` is a complete ``view.observations`` page document. The page's
    attempt/action selection scope is checked first; rows then match by
    ``(action_id, report_node_id, report_occurrence)``. Each output item keeps the
    bridge entry and adds ``observation`` (the original row or None) and
    ``availability`` (``observed``/``not_in_page``). A page absence is NOT evidence
    that the run never observed the node; cursor/revision stay with the existing
    query. Scope mismatches raise rather than guess.

    When a v3 ``plan_id`` scope is given, the page must carry and match that plan
    identity (and dag/revision when present); a page without it is rejected so the
    caller degrades honestly instead of promoting unverified rows. The current
    plan is never backfilled onto legacy rows. With no scope (legacy v1/v2) the
    original occurrence-only behaviour is preserved.
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
    if plan_id is not None:
        if observations.get('plan_id') != plan_id:
            raise ForgeError('Observation page lacks the sealed plan scope; cannot verify this-run association', code='conflict')
        if dag_id is not None and observations.get('dag_id') != dag_id:
            raise ForgeError('Observation page dag scope differs from the sealed plan', code='conflict')
        if plan_revision is not None and observations.get('plan_revision') != plan_revision:
            raise ForgeError('Observation page plan revision differs from the sealed plan', code='conflict')
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
        if plan_id is not None:
            # v3 scope: a row must explicitly carry the verified plan; a plan-less
            # legacy row is never promoted by defaulting it to the current plan.
            if (row.get('plan_id') != plan_id
                    or dag_id is not None and row.get('dag_id') != dag_id
                    or plan_revision is not None and row.get('plan_revision') != plan_revision):
                continue
            row_plan = plan_id
        else:
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
        if plan_id is not None and row is not None and row.get('occurrence_id') != entry.get('occurrence_id'):
            row = None
        joined.append(dict(entry, observation=row, availability='observed' if row is not None else 'not_in_page'))
    return tuple(joined)


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
                     physical=view['physical'],
                     node_observations=view['node_observations'],
                     identity_bridge_ref=view.get('identity_bridge_ref'),
                     code_pin_ref=view.get('code_pin_ref'))
    else:
        entry = dict(action_id=result['action_id'], status='sealed', binding_level='sealed',
                     dag=result['logical_dag'], node_map={}, plan_id=result['plan_id'],
                     sealed_at=result['sealed_at'],
                     artifact_manifest_ref=result['artifact_manifest_ref'],
                     physical=result['plan_view']['physical'])
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
