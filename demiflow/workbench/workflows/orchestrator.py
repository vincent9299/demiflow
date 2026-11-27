"""Detached per-workflow DAG supervisor process (zero background daemon; exits on completion)."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
import time

# Support running directly via `python -I -B .../orchestrator.py run <host_root> <wf_run_id> <attempt>`.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from demiflow.forge.schema import ForgeError
from demiflow.forge.workflow import resolve_node_overrides
from demiflow.forge.process import process_identity
from demiflow.forge.runs import manager as runs
from demiflow.workbench.runs.authority import from_bootstrap
from demiflow.workbench.workflows import store
from demiflow.workbench.workflows.resolver import build_target_authority

_POLL_INTERVAL_S = 0.15
_BUSY_RETRY_S = 0.4


def _is_project_busy_error(exc: Exception) -> bool:
    if isinstance(exc, ForgeError) and exc.code == 'busy':
        return True
    msg = str(exc).lower()
    return 'already active' in msg or 'execution lock' in msg or 'another run' in msg


def _child_error_message(child: dict) -> str:
    state = child.get('state') or {}
    err = state.get('error')
    if isinstance(err, dict):
        typ = err.get('type') or ''
        msg = err.get('message') or ''
        text = f'{typ}: {msg}'.strip(': ')
        if text:
            return text[:2048]
    elif isinstance(err, str) and err:
        return err[:2048]
    reason = state.get('reason')
    if isinstance(reason, str) and reason:
        return f'{child.get("status", "failed")}: {reason}'[:2048]
    return f'子管线结束状态：{child.get("status", "failed")}'


def _cancel_active_children(manifest: dict, active: dict[str, dict], cancelled_sent: set[str],
                            run_dir: Path, attempt: int) -> None:
    for node_id, info in list(active.items()):
        if node_id in cancelled_sent:
            continue
        child_run_id = info.get('child_run_id')
        if not child_run_id:
            continue
        cancelled_sent.add(node_id)
        try:
            auth = build_target_authority(manifest, info['project_alias'], 'run_control')
            runs.cancel(info['project_root'], child_run_id, expected_attempt=info.get('child_attempt'), authority=auth)
            store.append_event(run_dir, attempt, 'node.cancel_requested', node_id=node_id,
                               detail={'child_run_id': child_run_id})
        except Exception as exc:
            store.append_event(run_dir, attempt, 'node.cancel_notice', node_id=node_id,
                               detail={'child_run_id': child_run_id, 'message': str(exc)[:512]})


def run_attempt(host_root: Path | str, wf_run_id: str, attempt: int, *, host_bindings=None) -> int:
    run_dir = store.run_directory(host_root, wf_run_id)
    manifest = store.read_manifest(run_dir)
    spec = manifest['spec']
    policy = spec.get('policy') or {'max_parallel': 2, 'on_failure': 'fail_fast'}
    max_parallel = int(policy.get('max_parallel', 2))
    on_failure = policy.get('on_failure', 'fail_fast')
    concurrency_groups = spec.get('concurrency_groups') or {}
    nodes_by_id = {n['id']: n for n in spec['nodes']}
    topo_order = list(spec.get('topo_order') or [n['id'] for n in spec['nodes']])
    resolved_projects = manifest['resolved_projects']
    inputs_values = manifest.get('inputs') or {}

    stop_signal = [False]

    def _handle_signal(_signum, _frame):
        stop_signal[0] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    with store.hold_orchestrator_lock(run_dir):
        snap = store.read_run_snapshot(run_dir, attempt=attempt)
        if snap['status'] in store.TERMINAL_WORKFLOW_STATUSES:
            return 0
        store.update_workflow_status(
            run_dir, attempt, 'running',
            orchestrator=process_identity(os.getpid()),
        )
        store.append_event(run_dir, attempt, 'workflow.started', detail={'pid': os.getpid(), 'attempt': attempt})

        node_states = dict(snap['nodes'])
        active: dict[str, dict] = {}
        prepared_waiting: dict[str, dict] = {}
        cancelled_sent: set[str] = set()
        fail_fast_triggered = False

        # Re-adopt any child runs that were still alive across orchestrator recovery.
        for nid, st in node_states.items():
            if st['status'] == 'running' and st.get('child_run_id'):
                active[nid] = {
                    'project_alias': st['project_alias'],
                    'project_root': st['project_root'],
                    'child_run_id': st['child_run_id'],
                    'child_attempt': st.get('child_attempt'),
                    'concurrency_group': nodes_by_id[nid].get('concurrency_group'),
                }

        try:
            while True:
                stop_requested = stop_signal[0] or store.is_stop_requested(run_dir, attempt)
                if stop_requested:
                    _cancel_active_children(manifest, active, cancelled_sent, run_dir, attempt)

                # 1. Poll active child runs.
                for nid in list(active):
                    info = active[nid]
                    target_root = info['project_root']
                    child_run_id = info['child_run_id']
                    child_attempt = info.get('child_attempt')
                    try:
                        child = runs.show(target_root, child_run_id, attempt=child_attempt)
                    except Exception as exc:
                        err_msg = f'查询子管线状态失败：{exc}'[:2048]
                        store.update_node(run_dir, attempt, nid, status='failed', error=err_msg, finished_at=time.time())
                        store.append_event(run_dir, attempt, 'node.failed', node_id=nid, detail={'error': err_msg})
                        node_states[nid]['status'] = 'failed'
                        node_states[nid]['error'] = err_msg
                        del active[nid]
                        if on_failure == 'fail_fast':
                            fail_fast_triggered = True
                        continue

                    c_status = child.get('status')
                    if c_status == 'succeeded':
                        try:
                            art = runs.run_artifacts(target_root, child_run_id, attempt=child.get('attempt'))
                        except Exception as exc:
                            err_msg = f'核验子管线产物失败：{exc}'[:2048]
                            store.update_node(run_dir, attempt, nid, status='failed', error=err_msg, finished_at=time.time())
                            store.append_event(run_dir, attempt, 'node.failed', node_id=nid, detail={'error': err_msg})
                            node_states[nid]['status'] = 'failed'
                            node_states[nid]['error'] = err_msg
                            del active[nid]
                            if on_failure == 'fail_fast':
                                fail_fast_triggered = True
                            continue
                        now = time.time()
                        store.update_node(
                            run_dir, attempt, nid,
                            status='succeeded',
                            child_attempt=child.get('attempt'),
                            artifacts=art,
                            error=None,
                            finished_at=now,
                        )
                        store.append_event(
                            run_dir, attempt, 'node.succeeded', node_id=nid,
                            detail={
                                'child_run_id': child_run_id,
                                'attempt': child.get('attempt'),
                                'outputs': sorted((art.get('outputs') or {}).keys()),
                                'models': len(art.get('models_list') or ()),
                            },
                        )
                        node_states[nid]['status'] = 'succeeded'
                        node_states[nid]['artifacts'] = art
                        node_states[nid]['error'] = None
                        del active[nid]
                    elif c_status in {'failed', 'cancelled', 'interrupted'}:
                        err_msg = _child_error_message(child)
                        now = time.time()
                        store.update_node(
                            run_dir, attempt, nid,
                            status=c_status,
                            child_attempt=child.get('attempt'),
                            error=err_msg,
                            finished_at=now,
                        )
                        store.append_event(
                            run_dir, attempt, f'node.{c_status}', node_id=nid,
                            detail={'child_run_id': child_run_id, 'error': err_msg},
                        )
                        node_states[nid]['status'] = c_status
                        node_states[nid]['error'] = err_msg
                        del active[nid]
                        if on_failure == 'fail_fast' and c_status != 'cancelled':
                            fail_fast_triggered = True
                    elif c_status in {'starting', 'running', 'stopping'} and child.get('supervisor_alive') is False:
                        try:
                            runs.reconcile(target_root, child_run_id)
                        except Exception:
                            pass

                if fail_fast_triggered and active:
                    _cancel_active_children(manifest, active, cancelled_sent, run_dir, attempt)

                # 2. Skip downstream nodes whose upstream failed / skipped / cancelled / interrupted.
                for nid in topo_order:
                    st = node_states[nid]
                    if st['status'] not in {'pending', 'pending_resume', 'waiting_project'}:
                        continue
                    node_def = nodes_by_id[nid]
                    bad_deps = [
                        dep for dep in node_def['deps']
                        if node_states[dep]['status'] in {'failed', 'skipped', 'cancelled', 'interrupted'}
                    ]
                    if bad_deps:
                        reason = f'上游节点未成功（{", ".join(bad_deps[:4])}），已跳过'
                        now = time.time()
                        store.update_node(run_dir, attempt, nid, status='skipped', error=reason, finished_at=now)
                        store.append_event(run_dir, attempt, 'node.skipped', node_id=nid, detail={'deps': bad_deps})
                        st['status'] = 'skipped'
                        st['error'] = reason
                        prepared_waiting.pop(nid, None)

                # 3. Schedule ready nodes (respecting max_parallel, concurrency_groups, and per-project lock).
                if not stop_requested and not fail_fast_triggered:
                    active_by_root: dict[str, int] = {}
                    active_by_group: dict[str, int] = {}
                    for info in active.values():
                        r = info['project_root']
                        active_by_root[r] = active_by_root.get(r, 0) + 1
                        g = info.get('concurrency_group')
                        if g:
                            active_by_group[g] = active_by_group.get(g, 0) + 1

                    for nid in topo_order:
                        if len(active) >= max_parallel:
                            break
                        st = node_states[nid]
                        if st['status'] not in {'pending', 'pending_resume', 'waiting_project'}:
                            continue
                        node_def = nodes_by_id[nid]
                        if not all(node_states[dep]['status'] in {'succeeded', 'reused'} for dep in node_def['deps']):
                            continue
                        grp = node_def.get('concurrency_group')
                        if grp and active_by_group.get(grp, 0) >= concurrency_groups.get(grp, 1):
                            continue
                        proj_alias = node_def['project']
                        target_root = resolved_projects[proj_alias]['root']
                        if active_by_root.get(target_root, 0) >= 1:
                            if st['status'] != 'waiting_project':
                                store.update_node(run_dir, attempt, nid, status='waiting_project')
                                st['status'] = 'waiting_project'
                            continue

                        # Respect brief backoff when retrying a project busy with an external run.
                        wait_meta = prepared_waiting.get(nid)
                        if wait_meta and time.monotonic() < wait_meta.get('next_try', 0.0):
                            continue

                        # Try launching this node!
                        try:
                            submit_auth = (from_bootstrap(host_bindings[proj_alias], target_root) if host_bindings is not None
                                           else build_target_authority(manifest, proj_alias, 'run_submit'))
                            started = None

                            # Case A: resume an interrupted child run preserving P2/P4 checkpoints.
                            if st['status'] == 'pending_resume' and st.get('child_run_id'):
                                try:
                                    prev_child = runs.show(target_root, st['child_run_id'])
                                    resumed = runs.resume(
                                        target_root,
                                        st['child_run_id'],
                                        expected=prev_child['digest'],
                                        expected_attempt=prev_child['attempt'],
                                        authority=submit_auth,
                                    )
                                    started = resumed
                                    store.append_event(
                                        run_dir, attempt, 'node.child_resumed', node_id=nid,
                                        detail={'child_run_id': resumed['run_id'], 'attempt': resumed['attempt']},
                                    )
                                except Exception as resume_exc:
                                    if _is_project_busy_error(resume_exc):
                                        prepared_waiting[nid] = {'next_try': time.monotonic() + _BUSY_RETRY_S}
                                        if st['status'] != 'waiting_project':
                                            store.update_node(run_dir, attempt, nid, status='waiting_project')
                                            st['status'] = 'waiting_project'
                                        continue
                                    # Fall back cleanly to fresh prepare + start if child run is not checkpoint-resumable.
                                    store.append_event(
                                        run_dir, attempt, 'node.resume_fallback', node_id=nid,
                                        detail={'reason': str(resume_exc)[:512]},
                                    )

                            # Case B: previously prepared child waiting for external run on target project.
                            if started is None and wait_meta and wait_meta.get('prepared'):
                                prep = wait_meta['prepared']
                                try:
                                    started = runs.start(
                                        target_root,
                                        prep['run_id'],
                                        expected=prep['digest'],
                                        expected_attempt=prep['attempt'],
                                        authority=submit_auth,
                                    )
                                except Exception as start_exc:
                                    if _is_project_busy_error(start_exc):
                                        wait_meta['next_try'] = time.monotonic() + _BUSY_RETRY_S
                                        continue
                                    raise

                            # Case C: fresh resolve -> prepare -> start.
                            if started is None:
                                upstream_artifacts = {
                                    dep: (node_states[dep].get('artifacts') or {})
                                    for dep in node_def['deps']
                                }
                                resolved_overrides, bindings = resolve_node_overrides(
                                    node_def,
                                    inputs_values=inputs_values,
                                    upstream_artifacts=upstream_artifacts,
                                )
                                now = time.time()
                                store.update_node(
                                    run_dir, attempt, nid,
                                    status='preparing',
                                    resolved_overrides=resolved_overrides,
                                    bindings=bindings,
                                    error=None,
                                    started_at=now,
                                )
                                store.append_event(
                                    run_dir, attempt, 'node.preparing', node_id=nid,
                                    detail={'project': proj_alias, 'entry': node_def['entry']},
                                )
                                st['status'] = 'preparing'
                                st['resolved_overrides'] = resolved_overrides
                                st['bindings'] = bindings

                                output_prefix = f'workflows/{wf_run_id}/a{attempt}_{nid}'
                                prep = runs.prepare(
                                    target_root,
                                    node_def['entry'],
                                    params=node_def['params'],
                                    overrides=(resolved_overrides if resolved_overrides else None),
                                    output_prefix=output_prefix,
                                    new_outputs=True,
                                    authority=submit_auth,
                                )
                                store.update_node(
                                    run_dir, attempt, nid,
                                    child_run_id=prep['run_id'],
                                    child_attempt=prep['attempt'],
                                )
                                st['child_run_id'] = prep['run_id']
                                st['child_attempt'] = prep['attempt']
                                try:
                                    started = runs.start(
                                        target_root,
                                        prep['run_id'],
                                        expected=prep['digest'],
                                        expected_attempt=prep['attempt'],
                                        authority=submit_auth,
                                    )
                                except Exception as start_exc:
                                    if _is_project_busy_error(start_exc):
                                        prepared_waiting[nid] = {
                                            'prepared': prep,
                                            'next_try': time.monotonic() + _BUSY_RETRY_S,
                                        }
                                        store.update_node(run_dir, attempt, nid, status='waiting_project')
                                        store.append_event(
                                            run_dir, attempt, 'node.waiting_project', node_id=nid,
                                            detail={'project': proj_alias, 'child_run_id': prep['run_id']},
                                        )
                                        st['status'] = 'waiting_project'
                                        continue
                                    raise

                            # Launch succeeded!
                            prepared_waiting.pop(nid, None)
                            store.update_node(
                                run_dir, attempt, nid,
                                status='running',
                                child_run_id=started['run_id'],
                                child_attempt=started['attempt'],
                                error=None,
                            )
                            store.append_event(
                                run_dir, attempt, 'node.running', node_id=nid,
                                detail={
                                    'project': proj_alias,
                                    'child_run_id': started['run_id'],
                                    'attempt': started['attempt'],
                                },
                            )
                            st['status'] = 'running'
                            st['child_run_id'] = started['run_id']
                            st['child_attempt'] = started['attempt']
                            active[nid] = {
                                'project_alias': proj_alias,
                                'project_root': target_root,
                                'child_run_id': started['run_id'],
                                'child_attempt': started['attempt'],
                                'concurrency_group': grp,
                            }
                            active_by_root[target_root] = active_by_root.get(target_root, 0) + 1
                            if grp:
                                active_by_group[grp] = active_by_group.get(grp, 0) + 1
                        except Exception as exc:
                            err_msg = str(exc)[:2048]
                            now = time.time()
                            prepared_waiting.pop(nid, None)
                            store.update_node(run_dir, attempt, nid, status='failed', error=err_msg, finished_at=now)
                            store.append_event(run_dir, attempt, 'node.failed', node_id=nid, detail={'error': err_msg})
                            st['status'] = 'failed'
                            st['error'] = err_msg
                            if on_failure == 'fail_fast':
                                fail_fast_triggered = True
                                break

                # 4. Termination check.
                if not active:
                    remaining = [
                        nid for nid in topo_order
                        if node_states[nid]['status'] in {'pending', 'pending_resume', 'waiting_project'}
                    ]
                    if not remaining:
                        break
                    if stop_requested:
                        now = time.time()
                        for nid in remaining:
                            store.update_node(run_dir, attempt, nid, status='cancelled', error='编排已停止', finished_at=now)
                            store.append_event(run_dir, attempt, 'node.cancelled', node_id=nid)
                            node_states[nid]['status'] = 'cancelled'
                        break
                    if fail_fast_triggered:
                        now = time.time()
                        for nid in remaining:
                            store.update_node(run_dir, attempt, nid, status='skipped', error='fail_fast 触发，未执行', finished_at=now)
                            store.append_event(run_dir, attempt, 'node.skipped', node_id=nid)
                            node_states[nid]['status'] = 'skipped'
                        break

                time.sleep(_POLL_INTERVAL_S)

            # 5. Compute final workflow status.
            statuses = {st['status'] for st in node_states.values()}
            if stop_signal[0] or store.is_stop_requested(run_dir, attempt):
                final_status = 'succeeded' if statuses <= {'succeeded', 'reused'} else 'cancelled'
            elif statuses <= {'succeeded', 'reused'}:
                final_status = 'succeeded'
            elif 'failed' in statuses:
                final_status = 'failed'
            elif 'interrupted' in statuses:
                final_status = 'interrupted'
            elif 'cancelled' in statuses:
                final_status = 'cancelled'
            else:
                final_status = 'failed'

            failed_nodes = [nid for nid in topo_order if node_states[nid]['status'] == 'failed']
            final_error = None
            if failed_nodes:
                first = failed_nodes[0]
                final_error = f'节点 {first} 失败：{node_states[first].get("error") or "未知错误"}'[:2048]
            elif final_status == 'cancelled':
                final_error = '编排已取消'

            store.update_workflow_status(run_dir, attempt, final_status, error=final_error, finished=True)
            store.append_event(
                run_dir, attempt, 'workflow.finished',
                detail={'status': final_status, 'failed_nodes': failed_nodes},
            )
            return 0 if final_status == 'succeeded' else 1
        except Exception as exc:
            err_msg = f'编排调度器异常：{type(exc).__name__}: {exc}'[:2048]
            store.update_workflow_status(run_dir, attempt, 'failed', error=err_msg, finished=True)
            store.append_event(run_dir, attempt, 'workflow.error', detail={'error': err_msg})
            return 1


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    bindings = None
    if args[-1:] == ['--host-bindings']:
        raw = sys.stdin.buffer.read(store.MAX_MANIFEST_BYTES + 1)
        if len(raw) > store.MAX_MANIFEST_BYTES:
            raise ForgeError('Workflow host bindings exceed their byte budget', code='resource_limit')
        bindings = json.loads(raw)
        if not isinstance(bindings, dict):
            raise ForgeError('Invalid workflow host bindings', code='invalid_request')
        args = args[:-1]
    if len(args) != 4 or args[0] != 'run':
        sys.stderr.write('usage: orchestrator.py run <host_root> <wf_run_id> <attempt>\n')
        return 2
    _, host_root, wf_run_id, raw_attempt = args
    try:
        attempt = int(raw_attempt)
    except ValueError:
        sys.stderr.write('invalid attempt number\n')
        return 2
    return run_attempt(host_root, wf_run_id, attempt, host_bindings=bindings)


if __name__ == '__main__':
    raise SystemExit(main())
