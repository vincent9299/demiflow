"""Forge owns engineering turn decisions; coding harnesses own model/tool loops.

Hosts supply truthful author completion, source observations and authorized I/O.
This module has no knowledge of a browser, conversation database or provider RPC.
"""
from contextlib import nullcontext
from hashlib import sha256
import json
from pathlib import Path
from uuid import uuid4

from .pipeline_check import check_project, result as check_result
from .project_rules import diagnostic
from .platform import content_digest
from .platform_environment import read_bounded
from .review import review_project, MAX_REVIEW_CALLS, REVIEW_SECONDS
from .schema import ForgeError, canonical

def has_code_changes(changes):
    """True when at least one non-documentation business code/config file changed."""
    code_dirs = {'flows', 'operators', 'prompts', 'data_tools', 'configs', 'parameters', 'tests', 'src', 'lib', 'packages'}
    code_exts = {'.py', '.yaml', '.yml', '.json', '.toml', '.js', '.ts'}
    for change in changes or ():
        rel = str(change.get('path') or '').lstrip('/')
        if not rel:
            continue
        parts = rel.split('/')
        if parts[0] in code_dirs or (parts[0] == 'workflows' and Path(rel).suffix == '.json'):
            return True
        if len(parts) == 1 and Path(rel).suffix.lower() in code_exts:
            return True
    return False


def project_has_business_python(project_root, *, project_python=None) -> bool | None:
    """Return True if a business Python is configured in workbench or workspace.json, False if omitted, None if unavailable."""
    if isinstance(project_python, str) and project_python.strip():
        return True
    if not project_root:
        return None
    ws_file = Path(project_root) / 'workspace.json'
    if not ws_file.is_file():
        return None
    try:
        doc = json.loads(read_bounded(ws_file, 256 * 1024).decode('utf-8'))
    except (OSError, ValueError):
        return None
    if not isinstance(doc, dict):
        return None
    env = doc.get('environment') if isinstance(doc.get('environment'), dict) else {}
    py = doc.get('python') or env.get('python')
    return isinstance(py, str) and bool(py.strip())


def build_rework_prompt(pipeline_checks, review_report, *, project_root=None, project_python=None,
                         protected_violations=()):
    """Construct a bounded, deduplicated structured rework prompt for hard check failures or blocking findings."""
    issues = []
    for rel in (protected_violations or ()):
        issues.append(f"- [受保护文件钩子] `{rel}` 属于 Forge 受保护范围（`.env*` / `.forge/state.json` / `workspace.json` / `AGENTS.md` / `PIPELINE_SPEC.md` / `data_tools/`），开发助手不能直接修改，钩子已自动还原；登记入口请使用 `demiflow forge add flow --root DIR --register --entry FILE:FUNCTION`，其他目录操作查询 `demiflow forge add --help`。保留未获授权的项目规范、环境及数据工具内容。")
    if isinstance(pipeline_checks, dict):
        for key, label in (('native_flow', '原生数据流检查'), ('project_layout', '项目目录检查')):
            item = pipeline_checks.get(key)
            if isinstance(item, dict) and item.get('status') == 'failed':
                diags = item.get('diagnostics') or []
                if diags:
                    grouped = {}
                    for d in diags:
                        loc = (d.get('path') or '') + (f":{d['line']}" if d.get('line') else '')
                        rule = d.get('rule', 'ERROR')
                        detail = d.get('detail', '')
                        line = f"- [{label} · {rule}] {loc} {detail}".strip()
                        grouped[line] = grouped.get(line, 0) + 1
                    for line, count in grouped.items():
                        issues.append(f"{line}（共 {count} 处）" if count > 1 else line)
                else:
                    issues.append(f"- [{label}] 检查未通过，请排查并修复相关改动。")
    if isinstance(review_report, dict) and review_report.get('status') == 'invalid' and review_report.get('current', True):
        seen_findings = set()
        for f in (review_report.get('findings') or []):
            if f.get('severity') != 'blocking':
                continue
            loc = (f.get('path') or '') + (f":{f['line']}" if f.get('line') else '')
            parts = [p for p in (f.get('trigger'), f.get('impact')) if p]
            sug = f"（建议：{f['suggestion']}）" if f.get('suggestion') else ''
            line = f"- [独立评审阻断] {loc} {' · '.join(parts)}{sug}".strip()
            if line in seen_findings:
                continue
            seen_findings.add(line)
            issues.append(line)
    if not issues:
        return None
    has_python = project_has_business_python(project_root, project_python=project_python)
    if has_python is False:
        header = '[Forge 自动返工 1/1] 上一轮静态检查或独立评审发现以下阻断项，请直接修复并复跑 demiflow forge check --root . --json 确认清零后直接交付（该项目未配置业务 Python，无需探测宿主机环境或强行运行 pytest）：\n'
    else:
        header = '[Forge 自动返工 1/1] 上一轮静态检查或独立评审发现以下阻断项，请直接修复并复跑 demiflow forge check；项目测试验证仅使用项目明确的命令和业务 Python，不探测宿主解释器：\n'
    prompt = header + '\n'.join(issues)
    if len(prompt.encode()) > 30000:
        raise ForgeError('完整阻断反馈超过30KB，自动修正未派发；请查看完整检查和评审报告后拆分修正范围。', code='resource_limit')
    return prompt



def review_requested(*, changes, auto_review, selection):
    """A manual selection and the automatic hook share one candidate evaluation."""
    return selection is not None or auto_review and has_code_changes(changes)


def candidate_identity(snapshot):
    return content_digest({name: (fact or {}).get('sha256') for name, fact in snapshot.items()})


def dependency_candidate_digest(root, *, allowed_roots=None, denied_paths=()):
    """Current content digest for declared project dependencies, including external local_ref trees."""
    from .dependencies import declaration, inspect_dependencies
    if declaration(root) is None:
        return None
    inspected = inspect_dependencies(
        root,
        allowed_roots=allowed_roots,
        denied_paths=denied_paths,
        check_imports=False,
        check_environment=False,
        allow_partial=True,
    )
    return inspected['digest']


def feedback_current(root, options, author, *, allowed_roots=None, denied_paths=()):
    """Recheck the original author, project source tree, and declared dependencies before feedback transport."""
    from .dependencies import declaration
    from .source_snapshot import capture
    expected = options.get('author') or {}
    if any(expected.get(key) != author.get(key) for key in ('provider', 'session')):
        return False
    basis = options.get('candidate')
    if not isinstance(basis, dict):
        return False
    if candidate_identity(capture(root, basis['watch'], exclude=basis['exclude'])) != basis['digest']:
        return False
    try:
        if 'dependencies_digest' in basis:
            current_dep = dependency_candidate_digest(
                root, allowed_roots=allowed_roots, denied_paths=denied_paths,
            )
            if current_dep != basis['dependencies_digest']:
                return False
        else:
            config = declaration(root)
            if config is not None and any(
                item.get('source') == 'local_ref' for item in (config.get('packages') or {}).values()
            ):
                return False
    except (OSError, ValueError):
        return False
    return True


def _static_review_observation(source, check, definitions, stage):
    detail = dict(stage=stage, status=check['status'], diagnostic_count=check.get('diagnostic_count', 0))
    samples = {'diagnostics': check.get('diagnostics', ())[:4]}
    if source == 'project_layout':
        workflows = [dict(path=item['path'], status=item['status']) for item in definitions
                     if item['path'].startswith('workflows/') and 'status' in item]
        if workflows:
            detail['workflow_count'] = len(workflows)
            samples['workflow_checks'] = workflows[:4]
    detail.update({field: [] for field in samples})
    for field, rows in samples.items():
        for row in rows:
            detail[field].append(row)
            if len(canonical(detail)) > 2048:
                detail[field].pop()
    return dict(source=source, detail=canonical(detail).decode())


def assess_turn(root, *, author, goal, changes=(), prior_sources=None, selection=None,
                auto_review=False, round=0, budget=None, reference=None, exclude=(),
                reviewer=None, check_cancel=None, source_current=None, protected_violations=(),
                project_python=None, submission=None, on_review=None, inspect=True, on_budget=None, observations=None,
                allowed_roots=None, denied_paths=()):
    """Finish one engineering interval and produce a durable, candidate-bound next action.

    ``author`` contains provider/session and completed/acknowledged/cleanup facts.
    ``goal`` may be a host callback returning (goal, observed preimages); it runs
    only after the author is eligible. ``source_current`` revalidates the host's
    observation before any executable feedback is returned. Hosts must persist
    the outcome and next action before transport; they do not decide rework policy.
    """
    if type(round) is not int or not 0 <= round <= 1:
        raise ForgeError('Invalid engineering repair round', code='invalid_request')
    root = Path(root).absolute()
    budget = dict(budget or dict(calls=MAX_REVIEW_CALLS, seconds=float(REVIEW_SECONDS)))
    receipt = dict(format='demiflow_forge_development_v1', id=submission or uuid4().hex,
                   root=str(root), author=dict(author), round=round, status='incomplete',
                   budget=budget, checks=None, review=None, feedback=None,
                   options=dict(auto_review=bool(auto_review), selection=selection))
    if check_cancel:
        check_cancel()
    if not (author.get('completed') and author.get('acknowledged') and author.get('cleanup') == 'complete'):
        receipt['reason'] = '作者回合未完成、输入未确认或清理未知。'
        return receipt
    want_review = review_requested(changes=changes, auto_review=auto_review, selection=selection)
    selected = {} if auto_review and has_code_changes(changes) else selection or {}
    paths = [item['path'] for item in changes]
    definitions = ()
    if inspect and (paths or want_review or protected_violations):
        try:
            # A dependency change can affect every registered entry, even when no Flow file was edited.
            project_wide = any(p == 'pyproject.toml' or p.startswith('packages/') for p in paths)
            check = check_project(root, paths=paths if 0 < len(paths) <= 256 and not project_wide else (),
                                  reference=reference, exclude=exclude, include_hygiene=True,
                                  python=project_python, allowed_roots=allowed_roots, denied_paths=denied_paths)
            definitions = check.get('definitions', ())
            receipt['checks'] = {key: check[key] for key in (
                'status', 'native_flow', 'project_layout', 'catalog_id', 'rules_id',
                'source_digest', 'source_count', 'check_digest', 'coverage', 'project_dependencies') if key in check}
        except (OSError, ValueError, KeyError, RecursionError) as exc:
            incomplete = check_result([diagnostic('DF-01', '', str(exc), status='incomplete')], ())
            receipt['checks'] = dict(status='incomplete', native_flow=incomplete, project_layout=incomplete)
    if source_current is not None and not source_current():
        receipt['reason'] = 'SDK 或源码在检查期间改变，需重新核验。'
        return receipt
    if want_review:
        try:
            text, prior = goal() if callable(goal) else (goal, prior_sources)
            if on_review:
                on_review()
            if reviewer is None:
                raise ForgeError('独立评审模型尚未配置。', code='inspection_incomplete')
            observations = list(observations(definitions)) if observations else []
            stage = 'recheck_after_feedback' if round else 'author_submission'
            for key in ('native_flow', 'project_layout'):
                item = (receipt['checks'] or {}).get(key)
                if item:
                    observations.append(_static_review_observation(key, item, definitions, stage))
            receipt['review'] = review_project(root, goal=text, **selected,
                paths=() if selected else tuple(prior or paths), prior_sources=None if selected else prior,
                exclude=exclude, observations=observations, reviewer=reviewer, budget=budget,
                check_cancel=check_cancel, on_budget=on_budget,
                allowed_roots=allowed_roots, denied_paths=denied_paths)
        except (OSError, ValueError, RecursionError) as exc:
            receipt['review'] = dict(status='incomplete', current=False, complete=False, findings=[],
                                    diagnostics=[dict(code=getattr(exc, 'code', 'inspection_incomplete'), detail=str(exc)[:2048])])
    if check_cancel:
        check_cancel()
    current = source_current is None or source_current()
    report = receipt['review']
    if not current or report is not None and not report.get('current', False):
        receipt['reason'] = '候选已变化或评审证据未验真，不能据此自动修改。'
        # Static checks can still be actionable when only the model is unavailable,
        # provided the host's complete source observation remains current.
        if not current:
            return receipt
    try:
        message = build_rework_prompt(receipt['checks'], report, project_root=root,
                                      project_python=project_python, protected_violations=protected_violations)
    except ForgeError as exc:
        receipt.update(status='invalid', reason=str(exc))
        return receipt
    checks = receipt['checks'] or {}
    incomplete = any(checks.get(k, {}).get('status') == 'incomplete' for k in ('native_flow', 'project_layout'))
    if message:
        receipt['status'] = 'invalid'
        if round == 0:
            receipt['feedback'] = dict(id='forge-' + sha256(receipt['id'].encode()).hexdigest()[:40],
                text=message, status='pending', author=dict(author), round=1,
                options=dict(auto_review=bool(auto_review), selection=selected if want_review and (
                    auto_review or report and report.get('current') and report['status'] == 'invalid') else None,
                             budget=dict(budget), round=1))
    elif incomplete or report is not None and report['status'] != 'passed':
        receipt['status'] = 'incomplete'
    else:
        receipt['status'] = 'passed' if report is not None else 'checked'
    return receipt


def receipt_summary(receipt):
    """Small projection for a host terminal record; full evidence is saved separately."""
    result = {key: receipt[key] for key in ('format', 'id', 'root', 'author', 'round', 'status', 'budget', 'options')}
    if receipt.get('reason'):
        result['reason'] = receipt['reason']
    if receipt.get('feedback'):
        result['feedback'] = {key: receipt['feedback'][key] for key in ('id', 'status', 'round')}
    return result


def run_development(store, *, goal, binding, author, reviewer, auto_review=True,
                    seconds=3600, review_seconds=REVIEW_SECONDS, resume=False, project_python=None,
                    allowed_roots=None, denied_paths=(), author_controls=None):
    """Drive the complete CLI engineering loop, persisting before every author send.

    The native author callback executes one harness interval. This function owns
    the repair bound and all engineering decisions, also used by host turn hooks.
    """
    import fcntl
    import os
    import time
    from .development_state import claim_writer, start_writer, retire_writer, writer_key
    from .source_snapshot import capture, differences, read_source, IGNORED_DIRS
    from .project_guard import capture_protected_files, enforce_protected_files, prepare_control_writes, apply_control_writes
    from .review import MAX_GOAL_BYTES

    root = Path(store.root['path'])
    if resume:
        state = store.load()
        if state['binding'] != binding:
            raise ForgeError('Original author executable/profile/configuration changed', code='conflict')
        if state['phase'] not in {'ready', 'feedback_pending', 'assessing'}:
            if state.get('writer') and state['author'].get('cleanup') == 'complete':
                retire_writer(state['writer'], terminal=True, cleanup='complete')
            return state  # A sending/unknown interval is deliberately never replayed.
        if state.get('author', {}).get('cleanup') not in (None, 'complete'):
            raise ForgeError('Original author cleanup is unknown', code='busy')
    else:
        if store.path.exists():
            raise ForgeError('Use --resume or --status for this existing engineering operation', code='conflict')
        if not isinstance(goal, str) or not goal.strip() or len(goal.encode()) > MAX_GOAL_BYTES:
            raise ForgeError('A complete development goal within 128KiB is required', code='invalid_request')
        state = dict(format='demiflow_forge_development_state_v1', id=uuid4().hex, root=store.root,
                     binding=binding, goal=goal, auto_review=auto_review, phase='ready', status='incomplete',
                     round=0, deadline=time.time() + seconds, budget=dict(calls=MAX_REVIEW_CALLS, seconds=float(review_seconds)),
                     author=dict(provider=binding['provider'], session=None, cleanup='complete'), history=[])
        store.save(state)
    if state['deadline'] <= time.time():
        state.update(status='incomplete', reason='工程交付总时间预算已用尽。')
        store.save(state)
        return state
    old_claim = state.get('writer')
    if old_claim:
        if state['author'].get('cleanup') != 'complete':
            raise ForgeError('Previous author interval has no cleanup proof', code='busy')
        retire_writer(old_claim, terminal=True, cleanup='complete')
    unit = store.directory / ('unit-' + uuid4().hex)
    fd = os.open(unit, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(fd, fcntl.LOCK_SH)
    info = os.fstat(fd)
    key = None
    try:
        key = writer_key(state['id'])
        state['writer'] = key
        store.save(state)
        claim_writer(root, state['id'], unit, dict(device=info.st_dev, inode=info.st_ino))
        start_writer(key)

        def feedback_status(status):
            feedback = state.get('feedback')
            if feedback is not None:
                feedback['status'] = status
                for item in state['history']:
                    if (item.get('feedback') or {}).get('id') == feedback['id']:
                        item['feedback']['status'] = status

        def roots():
            values = []
            with os.scandir(root) as entries:
                for entry in entries:
                    if entry.name.startswith('.') or entry.name in IGNORED_DIRS | {'notebooks'}:
                        continue
                    if len(values) >= 256:
                        raise ForgeError('Author root inventory exceeds budget', code='resource_limit')
                    values.append(entry.name)
            return sorted(values)

        while True:
            if time.time() >= state['deadline']:
                state.update(status='incomplete', phase='finished', reason='工程交付总时间预算已用尽。')
                store.save(state)
                break
            source_dir = store.directory / ('sources-' + str(state['round']))
            if state['phase'] != 'assessing':
                if state['phase'] == 'feedback_pending':
                    if not feedback_current(root, state['feedback'], state['author'],
                                            allowed_roots=allowed_roots, denied_paths=denied_paths):
                        state.update(status='incomplete', phase='finished', reason='修正反馈对应的源码或作者已变化。')
                        store.save(state)
                        break
                    prompt = state['feedback']['text']
                else:
                    prompt = state['goal']
                watched = roots()
                before = capture(root, watched, source_dir=source_dir)
                guarded = capture_protected_files(root)
                control_path = store.directory / ('controls-' + str(state['round']) + '.json')
                if author_controls is None:
                    prepare_control_writes(root, control_path, state['id'] + '-' + str(state['round']))
                state.update(phase='author_sending', before=before, watch=watched, delivery='prepared')
                state['author']['cleanup'] = 'unknown'
                store.save(state)

                def notify(facts):
                    if 'session' in facts:
                        state['author']['session'] = facts['session']
                    if 'delivery' in facts:
                        state['delivery'] = facts['delivery']
                        feedback_status(facts['delivery'])
                    store.save(state)

                controls = author_controls(guarded) if author_controls else nullcontext()
                with controls as authorized_writes:
                    result = author(root, prompt, binding=binding, session=state['author'].get('session'),
                                    lease_fd=fd, deadline=state['deadline'], notify=notify,
                                    control_path=None if author_controls else control_path)
                if author_controls is None:
                    authorized_writes = apply_control_writes(root, control_path, state['id'] + '-' + str(state['round']), guarded)
                state['author'] = result
                state['protected_violations'] = enforce_protected_files(root, guarded, authorized=authorized_writes)
                if not result.get('completed') or result['cleanup'] != 'complete':
                    state.update(phase='unknown' if state.get('delivery') == 'sending' else 'finished',
                                 status='incomplete', reason=result.get('reason', '作者回合未完成。'))
                    if state.get('delivery') == 'sending':
                        state['delivery'] = 'unknown'
                        feedback_status('unknown')
                    store.save(state)
                    break
                state.update(phase='assessing', delivery='acknowledged')
                store.save(state)
            before = state['before']
            watched = sorted(set(state['watch']) | set(roots()))
            after = capture(root, watched, source_dir=source_dir)
            changes = list(differences(before, after, watched, root=root))
            # Keep the original basis across the repair interval, including files
            # fixed/deleted in the first turn, so the second review cannot shrink.
            prior = dict(state.get('prior', {}))
            for change in changes:
                name = change['path']
                original = prior.get(name, {}).get('before') if name in prior else read_source(source_dir, before.get(name))
                prior[name] = dict(before=original, after_sha256=(after.get(name) or {}).get('sha256'))
            state['prior'] = prior
            identity = candidate_identity(after)
            try:
                dep_digest = dependency_candidate_digest(
                    root, allowed_roots=allowed_roots, denied_paths=denied_paths,
                )
                dep_digest_valid = True
            except (OSError, ValueError):
                dep_digest = None
                dep_digest_valid = False

            def current():
                if candidate_identity(capture(root, watched)) != identity:
                    return False
                if not dep_digest_valid:
                    return False
                try:
                    return dependency_candidate_digest(
                        root, allowed_roots=allowed_roots, denied_paths=denied_paths,
                    ) == dep_digest
                except (OSError, ValueError):
                    return False
            review_deadline = min(state['deadline'], state.get('review_deadline', time.time() + state['budget']['seconds']))
            state['review_deadline'] = review_deadline
            state['budget'] = dict(state['budget'], seconds=max(0.0, review_deadline - time.time()))
            store.save(state)
            def save_budget(remaining):
                state['budget'] = dict(remaining)
                store.save(state)
            def review_once(prompt):
                remaining = review_deadline - time.time()
                if remaining <= 0:
                    raise ForgeError('Independent review budget exhausted', code='resource_limit')
                return reviewer(prompt, timeout=remaining)
            options = state['feedback']['options'] if state['round'] else dict(auto_review=state['auto_review'], selection=None)
            receipt = assess_turn(root, submission=state['id'] + '-' + str(state['round']),
                author=state['author'], goal=state['goal'], changes=changes, prior_sources=prior or None,
                # A requested code review continues on the same original scope
                # even when a repair turn changes no additional files.
                selection=options['selection'],
                auto_review=options['auto_review'], round=state['round'], budget=state['budget'],
                reviewer=review_once, source_current=current, protected_violations=state.get('protected_violations', ()),
                project_python=project_python, on_budget=save_budget,
                allowed_roots=allowed_roots, denied_paths=denied_paths)
            state['budget'] = dict(receipt['budget'])
            state['history'].append(receipt)
            state['status'] = receipt['status']
            feedback = receipt['feedback']
            if feedback is None:
                state['phase'] = 'finished'
                store.save(state)
                break
            feedback['candidate'] = dict(watch=watched, exclude=[], digest=identity, dependencies_digest=dep_digest)
            # Store a bounded next action before calling the author again.
            state.update(phase='feedback_pending', feedback=feedback, round=feedback['round'])
            store.save(state)
    except (OSError, ValueError, KeyboardInterrupt) as exc:
        state.update(status='incomplete', reason=str(exc)[:2048] or '工程交付已取消。')
        if state.get('delivery') == 'sending':
            state.update(phase='unknown', delivery='unknown')
            feedback_status('unknown')
        store.save(state)
    finally:
        os.close(fd)
        if key and state['author'].get('cleanup') == 'complete':
            try:
                retire_writer(key, terminal=True, cleanup='complete')
            except (OSError, ValueError):
                pass  # The durable cleanup receipt lets --resume retry retirement.
    return state
