"""One local experiment: immutable preparation, native execution, fixed results.

The native RunJournal owns run facts. This module never schedules graph nodes,
interprets Python for execution, writes Lance data, or replaces native receipts.
"""
from contextlib import contextmanager
from contextlib import nullcontext
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.process import process_identity
from demiflow.workbench.runs.authority import LocalAuthority
from demiflow.workbench.runs.admission import inputs as admit_inputs
from demiflow.forge import audit
from demiflow.forge.platform import content_digest
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.platform_environment import platform_declaration
from demiflow.forge.platform_environment import environment_inventory
from demiflow.forge.schema import ForgeError
from demiflow.forge import operation_budget
from demiflow.workbench.runs.limits import DEFAULT_LIMITS
from demiflow.workbench.runs.limits import limits_with
from demiflow.execution.host.control import RunJournal
from demiflow.execution.host.inspection import read_run, RunReadError
from demiflow.errors import ExecutionConflict

FORMAT = 'demiflow_forge_run_manifest_v5'
PREVIOUS_FORMAT = 'demiflow_forge_run_manifest_v4'
LEGACY_EXECUTABLE_FORMAT = 'demiflow_forge_run_manifest_v3'
ACTIVE = {'starting', 'running', 'stopping'}
TERMINAL = {'succeeded', 'failed', 'cancelled', 'interrupted'}
_CURRENT_ATTEMPT = object()


def _attempt(observed, expected):
    """Capture current once, or require an explicit modern/legacy identity."""
    if expected is _CURRENT_ATTEMPT:
        return observed
    if expected is not None and (type(expected) is not int or not 1 <= expected <= 32):
        raise ForgeError('Attempt must be an integer in 1..32 or explicit legacy null', code='invalid_request')
    if expected != observed:
        raise ForgeError('Execution attempt changed', code='conflict')
    return observed


def directory(root, run_id):
    if not isinstance(run_id, str) or not re.fullmatch(r'r[0-9a-f]{24}', run_id):
        raise ForgeError('Invalid run identity')
    return ws.relative_path(Path(root).absolute(), '.forge/runs/' + run_id)


def journal(root, run_id):
    return RunJournal(directory(root, run_id))


def attempt_directory(target, kind, attempt=None):
    if kind not in {'work', 'logs', 'diagnostics'} or attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
        raise ForgeError('Invalid attempt directory')
    parent = Path(target) / kind
    return parent / ('attempt-' + str(attempt)) if attempt is not None else parent


@contextmanager
def execution_lock(root, *, create=True):
    path = Path(root) / '.forge/execution.lock'
    ws.no_symlinks(path)
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield fd
    except BlockingIOError:
        raise ForgeError('Another execution operation owns this workspace', code='busy') from None
    finally:
        os.close(fd)


def _run_ids(root, *, maximum=4096):
    """Bounded one-time legacy lease adoption, never ordinary list/poll work."""
    parent = Path(root) / '.forge/runs'
    ws.no_symlinks(parent)
    if not parent.exists():
        return []
    values = []
    with os.scandir(parent) as entries:
        for entry in entries:
            if len(values) >= maximum or entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                raise ForgeError('Run store entry/count limit exceeded')
            directory(root, entry.name)
            values.append(entry.name)
    return sorted(values)


def _lease_owner(root, lease):
    raw = os.pread(lease, 4097, 0)
    if not raw:
        # Old workspaces have an empty lease file. Adopt its actual unresolved
        # journal once; a derived list index never authorizes execution.
        active, deadline = [], time.monotonic() + 15
        for identity in _run_ids(root):
            if time.monotonic() > deadline:
                raise ForgeError('Legacy execution-owner adoption exceeded its time budget')
            if _load(root, identity, detail='identity')[1]['state']['status'] in ACTIVE:
                active.append(identity)
        if len(active) > 1:
            raise ForgeError('Multiple legacy active journals require explicit reconciliation')
        _claim_lease(lease, active[0] if active else None)
        return active[0] if active else None
    value = json.loads(raw)
    if len(raw) > 4096 or type(value) is not dict or set(value) != {'format', 'run_id'} or value['format'] != 'demiflow_forge_execution_owner_v1':
        raise ForgeError('Invalid execution lease owner')
    if value['run_id'] is not None:
        directory(root, value['run_id'])
    return value['run_id']


def _claim_lease(lease, run_id):
    raw = bounded_json(dict(format='demiflow_forge_execution_owner_v1', run_id=run_id), 4096)
    os.pwrite(lease, raw, 0)
    os.ftruncate(lease, len(raw))
    os.fsync(lease)


def refresh_index(root, run_id, *, deadline=None):
    """Best-effort activity hint; source journal and execution lease own state."""
    from demiflow.workbench.projects import index
    try:
        if deadline is not None and time.monotonic() >= deadline:
            return 'Index refresh deferred after maintenance deadline'
        with ws.control_lock(root):
            if deadline is not None and time.monotonic() >= deadline:
                return 'Index refresh deferred after maintenance deadline'
            marker = index.begin(root, 'run', run_id)
            if deadline is not None and time.monotonic() >= deadline:
                return 'Index publication deferred after maintenance deadline'
            return index.publish(root, marker)
    except Exception as error:
        return str(error)[:1024]


def _copy_checked(source, target, expected, maximum):
    operation_budget.check()
    raw = read_bounded(source, maximum)
    operation_budget.check()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ForgeError('Source changed while sealing: ' + source.name)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    target.chmod(0o400)


def _tree_bytes(path, *, entries_limit=65536):
    if not path.exists():
        return 0
    total = count = 0
    pending = [(path, 0)]
    while pending:
        base, depth = pending.pop()
        if depth > 24:
            raise ForgeError('Run directory depth limit exceeded')
        try:
            entries = os.scandir(base)
        except FileNotFoundError:
            continue  # Native spill/temporary directories can disappear mid-sample.
        with entries:
            for entry in entries:
                count += 1
                if count > entries_limit or entry.is_symlink():
                    raise ForgeError('Run directory entry limit or symlink violation')
                if entry.is_dir(follow_symlinks=False):
                    pending.append((Path(entry.path), depth + 1))
                elif entry.is_file(follow_symlinks=False):
                    try:
                        total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError:
                        pass
                elif not Path(entry.path).exists():
                    continue
                else:
                    raise ForgeError('Unexpected run directory file')
    return total


def durable_tree(target):
    """Persist sealed files and every new directory entry before admission."""
    from demiflow.forge.io import fsync_directory
    count = 0
    pending, directories = [Path(target)], []
    while pending:
        parent = pending.pop()
        directories.append(parent)
        with os.scandir(parent) as entries:
            for entry in entries:
                operation_budget.check()
                count += 1
                if count > 65536 or entry.is_symlink():
                    raise ForgeError('Sealed tree durability budget exceeded')
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    fd = os.open(entry.path, os.O_RDONLY | os.O_NOFOLLOW)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                else:
                    raise ForgeError('Unexpected sealed file')
    for parent in reversed(directories):
        operation_budget.check()
        fsync_directory(parent)
    fsync_directory(Path(target).parent)


@operation_budget.controlled
def prepare(root, entry, *, params, outputs=None, new_outputs=True, limits=None, inputs=None, request_id=None, telemetry=True, authority=None):
    from demiflow.workbench.worker import operation
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_submit")
    from demiflow.workbench.runs.domain import preparation_lock
    from functools import partial
    args = dict(params=params, outputs=outputs, new_outputs=new_outputs, limits=limits, inputs=inputs,telemetry=telemetry)
    from demiflow.workbench.runs.native_resources import trusted_deployment, capacity_domain
    args['capacity_domain'] = capacity_domain()
    native_deployment = trusted_deployment(workspace=root)
    if native_deployment is not None:
        args['native_deployment'] = native_deployment
    with preparation_lock() as lease:
        perform=partial(operation,_pass_fds=(lease,), _authority=authority.bootstrap())
        if request_id is not None:
            from demiflow.workbench.runs.requests import prepare as with_receipt
            return with_receipt(root, entry, args, perform, request_id)
        return perform('run_prepare', root, entry=entry, **args)


def _fact_actions(report, outputs):
    # Checkpoint sink commits/results use the save_lance node identity, which
    # is distinct from its enclosing run_stream action. Reserve both upfront.
    return list(dict.fromkeys([*report['actions'],*(o['action_id'] for o in outputs.values())]))


def _prepare(root, entry, *, params, outputs=None, new_outputs=True, limits=None, inputs=None,
             reserved_run_id=None, prepare_request_id=None, telemetry=True, authority=None, native_deployment=None,
             capacity_domain=None):
    operation_budget.check()
    if type(telemetry) is not bool:
        raise ForgeError('telemetry must be boolean',code='invalid_request')
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, 'run_submit')
    from demiflow.workbench.conversations import admission as development_admission
    development_acceptance = development_admission.check(root, entry=entry)
    root, workspace, platform = ws.load_workspace(root)
    change_scope = workspace.get('audit', {}).get('change_scope')
    audit.require_scope(ws._check_change_scope(root), expected_scope=change_scope)
    requested_limits = limits
    limits, parameter_path = admit_inputs(root, workspace, entry, params, limits)
    payload_defaults = {'max_row_bytes', 'max_batch_bytes'} - set(requested_limits or {}) - set(workspace.get('budgets') or {})
    if type(new_outputs) is not bool:
        raise ForgeError('new_outputs must be boolean')
    original = ws.read_control(parameter_path) if parameter_path else copy.deepcopy(params)
    # Existing resolver validates the closed parameter format before modifications.
    ws.resolve_arguments(root, workspace, original)
    effective = copy.deepcopy(original)
    if inputs is not None:
        if type(inputs) is not dict or len(inputs) > 32:
            raise ForgeError('Input overrides must map entry arguments to fixed table references')
        for name, alias in inputs.items():
            arg = effective['arguments'].get(name)
            if type(alias) is not str or type(arg) is not dict or arg.get('kind') != 'table':
                raise ForgeError('Input override must name an existing table argument')
            effective['arguments'][name] = {'kind': 'table', 'alias': alias}
        ws.resolve_arguments(root, workspace, effective)
    run_id = reserved_run_id or 'r' + uuid.uuid4().hex[:24]
    directory(root, run_id)
    output_specs = {}
    metadata = outputs or {}
    if type(metadata) is not dict:
        raise ForgeError('outputs must map output names to table metadata')

    def visit(value, names=()):
        if type(value) is dict and value.get('kind') == 'output':
            name = '_'.join(names)
            ws.identifier(name)
            if name in output_specs:
                raise ForgeError('Output names must be unique')
            supplied = metadata.get(name, {})
            if type(supplied) is not dict or set(supplied) - {'dataset_id', 'schema_name', 'schema_version'}:
                raise ForgeError('Invalid output table metadata')
            info = dict(dataset_id=name, schema_name=name, schema_version='1')
            info.update(supplied)
            for item in info.values():
                if not isinstance(item, str) or not item or item != item.strip() or len(item) > 128:
                    raise ForgeError('Output metadata requires normalized text of at most 128 characters')
            mode, expected_version = value.get('mode', 'create'), value.get('expected_version')
            if new_outputs and mode in {'create', 'checkpoint'}:
                value['relative_uri'] = f'runs/{run_id}/{name}.lance'
            base = ws.location_root(root, workspace['locations'][value['location_id']])
            uri = ws.relative_path(base, value['relative_uri'])
            if mode in {'create', 'checkpoint'} and uri.exists():
                raise ForgeError('Create target already exists: ' + str(uri))
            if mode == 'overwrite':
                from demiflow.lance.storage import inspect_lance
                if inspect_lance(str(uri)).resolved_version != expected_version:
                    raise ForgeError('Overwrite CAS baseline changed')
            output_specs[name] = {**info, **value, 'uri': str(uri), 'mode': mode, 'expected_version': expected_version}
        elif type(value) is dict:
            for key, child in value.items():
                visit(child, (*names, key))
        elif type(value) is list:
            for i, child in enumerate(value):
                visit(child, (*names, str(i)))

    visit(effective['arguments'])
    if not output_specs or len(output_specs) > 32 or set(metadata) - set(output_specs):
        raise ForgeError('Run requires 1..32 declared outputs with matching metadata')
    report = ws._inspect_flow(root, entry, params=effective, save=True)
    if report.get('status') != 'checked':
        raise ForgeError('Flow is not checked: ' + json.dumps(report.get('diagnostics', []), ensure_ascii=False)[:4096])
    from demiflow.workbench.runs import native_resources
    capacity_domain = native_resources.capacity_domain(capacity_domain)
    native_execution = native_resources.select(report, native_deployment, workspace=root)
    if native_execution and native_execution['resource_root'] != capacity_domain:
        raise ForgeError('Native deployment and capacity domain differ')
    if not 1 <= len(report['actions']) <= 32:
        raise ForgeError('Run action budget exceeded')
    from demiflow.execution.host.inspection import VERSION as capability_version
    from demiflow.execution.host.inspection import stream_residency
    by_id = {node['id']: node for node in report['nodes']}
    parents = {identity: [] for identity in by_id}
    for edge in sorted(report['edges'], key=lambda value: value['port']):
        parents[edge['to']].append(edge['from'])
    for node in report['nodes']:
        capability = node.get('capability', {})
        if capability.get('version') != capability_version or not capability.get('supported') or capability.get('backend') != 'local':
            raise ForgeError('Run requires current supported local native capabilities')
        if node['kind'] == 'write_table' and node['parameters']['mode'] not in {'create', 'overwrite'}:
            raise ForgeError('This action requires create or fixed-baseline overwrite')
        if node['role'] in {'action', 'bridge'} and capability['driver'] == 'stream':
            facts = []
            current = parents[node['id']][0]
            while by_id[current]['role'] == 'transform' and by_id[current]['kind'] not in {'join', 'union'}:
                upstream = by_id[current]
                facts.append({**upstream['capability_fact'],
                              'native': upstream['analysis'].get('native', {}),
                              'arguments': upstream['analysis'].get('stream_arguments', upstream['normalized_parameters']),
                              'reserved_bytes': upstream['analysis'].get('reserved_bytes', 0)})
                current = parents[current][0]
            try:
                if payload_defaults:
                    from demiflow.execution.host.control import stream_limits
                    # Seal the admitted defaults into the original manifest; every
                    # reader, checked call and physical worker receives these caps.
                    limits = stream_limits(facts, limits, adjustable_fields=payload_defaults)
                else:
                    stream_residency(facts, limits)
            except ValueError as exc:
                raise ForgeError(str(exc)) from exc
    for operator in report['operators']:
        if set(operator['hooks']) - {'astart', 'astop', 'aclose'}:
            raise ForgeError('Actor resource lifecycle must use the native astart/astop/aclose protocol')
    ledgers = [r for r in report['binding'].get('resources', {}).values() if r['kind'] == 'ledger']
    checkpoint_bytes = sum(n['parameters']['max_bytes'] for n in report['nodes'] if n['kind'] == 'open_checkpoint')
    if sum(r['max_bytes'] for r in ledgers) + checkpoint_bytes > limits['max_evidence_bytes']:
        raise ForgeError('Persistent evidence reservation exceeds run budget')
    if sum(n['parameters']['max_index_bytes'] for n in report['nodes'] if n['kind'] == 'save_lance') > limits['max_scratch_bytes']:
        raise ForgeError('Stream key index reservation exceeds scratch budget')
    budgets, credentials = {}, set()
    distributions = {d['name'] for d in platform['environment']['dependencies'] if d['status'] == 'installed'}
    for node in report['nodes']:
        missing = set(node['analysis'].get('required_distributions', ())) - distributions
        if missing:
            raise ForgeError('Native dependencies are outside the platform snapshot; bind the corresponding extras: ' + ', '.join(sorted(missing)))
        drain = node['analysis'].get('drain_timeout_s', 0)
        if drain and limits['cancel_grace_s'] < drain + 2:
            raise ForgeError('Declared native drain exceeds cancel_grace_s')
        budget = node['analysis'].get('request_budget')
        if budget:
            key = budget.get('path', node['id'])
            if key in budgets and budgets[key] != budget:
                raise ForgeError('Shared durable request ledger has conflicting budgets')
            budgets[key] = budget if 'path' in budget else {**budget, 'node': node['id']}
        credentials.update(r['name'] for r in node['analysis'].get('resources', []) if r['kind'] == 'credential' and r['name'])
    if len(credentials) > 16:
        raise ForgeError('Credential name budget exceeded')
    depths = {}
    for node in report['nodes']:
        depths[node['id']] = 1 + max((depths[parent] for parent in parents[node['id']]), default=0)
    if max(depths.values(), default=0) > 128:
        raise ForgeError('Run native plan depth exceeds 128')
    from demiflow.execution.host.inspection import execution_instances
    from demiflow.execution.host.inspection import OBSERVATION_COUNT
    try:
        observation_instances = execution_instances(report['nodes'], report['edges'], report['actions'], maximum=OBSERVATION_COUNT)
    except ValueError as exc:
        raise ForgeError(str(exc)) from exc
    actual_outputs = {n['parameters']['uri']: n['id'] for n in report['nodes'] if n['kind'] in {'write_table', 'save_lance'}}
    if set(actual_outputs) != {o['uri'] for o in output_specs.values()}:
        raise ForgeError('Every declared output must have one checked write action')
    for output in output_specs.values():
        output['action_id'] = actual_outputs[output['uri']]
        sink = by_id[output['action_id']]
        if sink['kind'] == 'save_lance' and output['mode'] != 'checkpoint':
            output.update(publication='native_stage_v3', stage=sink['parameters']['stage'],
                          schema_identity=sink['normalized_parameters']['schema']['identity'])
        if output['mode'] == 'checkpoint':
            sink = by_id[output['action_id']]
            owners = [n for n in report['nodes'] if n['kind'] == 'open_checkpoint'
                      and any(o['uri'] == output['uri'] for o in n['normalized_parameters']['outputs'].values())]
            if sink['kind'] != 'save_lance' or len(owners) != 1:
                raise ForgeError('Stage output requires a unique checkpoint-owned sink')
            output.update(stage=sink['parameters']['stage'], checkpoint_node=owners[0]['id'],
                          schema_identity=sink['normalized_parameters']['schema']['identity'])
    if sum(t['row_count'] for t in report['binding']['tables'].values()) > limits['max_input_rows']:
        raise ForgeError('Input row admission budget exceeded')
    from demiflow.workbench.runs.domain import SEALED_BYTES
    platform_root = Path(__file__).resolve().parents[2]
    seal_files = [(ws.relative_path(root,name), Path('source')/name, digest,1024**2)
                  for name,digest in report['binding']['sources'].items()]
    seal_files.extend((ws.relative_path(root,r['path']),Path('source')/r['path'],r['sha256'],r.get('max_bytes',1024**2))
                      for r in report['binding'].get('resources',{}).values() if r['kind'] in {'config','file'})
    seal_files.extend((platform_root/item['path'],Path('runtime/demiflow')/item['path'],item['sha256'],platform['limits']['file_bytes'])
                      for item in platform['platform']['files'])
    if len(seal_files)>16384 or sum(source.stat().st_size for source,_,_,_ in seal_files)>SEALED_BYTES:
        raise ForgeError('Combined source/runtime seal exceeds 192 MiB budget',code='resource_limit')
    if shutil.disk_usage(root).free < limits['max_scratch_bytes'] + limits['max_output_bytes'] + limits['max_evidence_bytes'] + 64 * 1024**2:
        raise ForgeError('Insufficient workspace disk headroom')
    from demiflow.workbench.runs.storage import object_locations
    store = root / '.forge/runs'
    from demiflow.workbench.projects import index
    with ws.control_lock(root):
        operation_budget.check()
        store.mkdir(exist_ok=True)
        marker = index.begin(root, 'run', run_id)
        target = directory(root, run_id)
        target.mkdir()
        try:
            from demiflow.workbench.runs.domain import history_admission
            from demiflow.workbench.host_partition import policy
            from demiflow.workbench.host_partition import location
            history_admission(root,ceiling=policy(location())['history_bytes'])
            (target / 'work').mkdir()
            for source,relative,digest,maximum in seal_files:
                _copy_checked(source,target/relative,digest,maximum)
            declaration = platform_declaration(platform_root, platform['limits'])
            ws.write_control(target / 'inspection.json', report, create=True)
            ws.write_control(target / 'effective-params.json', effective, create=True)
            if change_scope is not None:
                ws.write_control(target / 'change-baseline.json', ws._scope_baseline(root, workspace), create=True)
            manifest = dict(format=FORMAT, run_id=run_id, root=str(root), entry=entry,
                runtime_contract='demiflow_forge_runtime_v1', prepare_request_id=prepare_request_id, telemetry_enabled=telemetry,
                change_scope=change_scope,
                journal_version=2,
                created_at=time.time(), report_digest=report['digest'], original_parameters=original,
                parameter_path=str(parameter_path.relative_to(root)) if parameter_path else None,
                outputs=output_specs, limits=limits, workspace=workspace, capacity_domain=capacity_domain,
                observation_instances=observation_instances,
                credential_names=sorted(credentials), evidence=ledgers, request_budgets=list(budgets.values()),
                object_locations=object_locations(report=report),
                platform=dict(digest=workspace['platform']['digest'], origin=str(platform_root),
                    declaration=declaration, environment=platform['environment'], files=platform['platform']['files'], limits=platform['limits']))
            if native_execution is not None:
                native_resources.seal(target, native_execution)
                manifest['native_execution'] = native_execution
            if development_acceptance is not None:
                manifest['development_acceptance'] = development_acceptance
            manifest['digest'] = content_digest(manifest)
            operation_budget.check()
            # Expensive freshness checks precede the authorization commit guard.
            ws.verify_run_basis(root, manifest)
            durable_tree(target)
            with authority.guard(root, 'run_submit'), development_admission.guard(root, entry=entry, expected=development_acceptance):
                run = RunJournal(target)
                run.initialize(manifest, attempts=True)
                run = RunJournal(target, attempt=1)
                run.reserve_nodes([(item['action_id'], item['node_id'], item['occurrence']) for item in observation_instances])
                run.reserve_actions(_fact_actions(report,output_specs))
                run.transition({None}, 'prepared', prepared_at=time.time())
                run.record_usage(elapsed_ms=0, output_bytes=0, complete=True)
                durable_tree(target)
                index.publish(root, marker)
        except BaseException:
            shutil.rmtree(target)
            raise
    return show(root, run_id, include_report=True)


def _load(root, run_id, *, detail='full', action_ids=None, attempt=None):
    target = directory(root, run_id)
    try:
        with read_run(target, deadline=operation_budget.current_deadline()) as reader:
            value = reader.execution_view(detail=detail, action_ids=action_ids, attempt=attempt)
    except RunReadError as exc:
        raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc
    m = value['manifest']
    if not m or m.get('format') not in {FORMAT, PREVIOUS_FORMAT, LEGACY_EXECUTABLE_FORMAT, 'demiflow_forge_run_manifest_v1', 'demiflow_forge_run_manifest_v2'} or m.get('run_id') != run_id or m.get('root') != str(Path(root).absolute()):
        raise ForgeError('Invalid or relocated run manifest')
    if content_digest({k: v for k, v in m.items() if k != 'digest'}) != m['digest']:
        raise ForgeError('Run manifest digest differs')
    return target, value


def _report(target, manifest):
    value = ws.read_control(target / 'inspection.json', 8 * 1024**2)
    if value.get('digest') != manifest['report_digest'] or content_digest({k: v for k, v in value.items() if k != 'digest'}) != value['digest']:
        raise ForgeError('Run inspection digest differs')
    return value


def show(root, run_id, *, include_report=False, attempt=None, compact=False):
    if type(compact) is not bool:
        raise ForgeError('compact must be boolean',code='invalid_request')
    target, value = _load(root, run_id, attempt=attempt,detail='summary' if compact else 'full')
    result = _describe(run_id, value)
    if compact:
        result.pop('actions',None)
        result.pop('observations',None)
        result['observation_scope']='paged_run_observations'
    if include_report or compact:
        with read_run(target, deadline=operation_budget.current_deadline()) as reader:
            history = reader.list_attempts()
        result['current_attempt'] = history[-1]['attempt'] if history else None
        result['attempts'] = history
    if include_report:
        result.update(report=_report(target, value['manifest']), original_parameters=value['manifest']['original_parameters'])
    return result


def _describe(run_id, value):
    m, state = value['manifest'], value['state']
    outputs = []
    for name, spec in m['outputs'].items():
        facts = value['output_projections'][spec['action_id']]
        status = 'committed' if facts['committed'] else 'unknown' if facts['has_intent'] else 'not_started'
        if status == 'unknown' and state['status'] in ACTIVE and not facts['failed']:
            status = 'writing'
        result = facts['result']
        outputs.append(dict(name=name, **spec, status=status,
            receipt=facts.get('receipt'), commit=facts.get('commit'),
            retained_from_attempt=facts['retained_from_attempt'], result=result,
            result_digest=content_digest(result['ref']) if result else None, error=facts['error']))
    result = dict(run_id=run_id, digest=m['digest'], status=state['status'], state=state,
        attempt=state.get('attempt'),
        entry=m['entry'], created_at=m['created_at'], parameter_path=m['parameter_path'],
        outputs=outputs, actions=value['actions'], limits=m['limits'], report_digest=m['report_digest'])
    result['observations'] = [dict(item, run_id=run_id,
                                  completeness='incomplete' if state['status'] in TERMINAL and item['state'] in {'pending', 'running'} else 'recorded')
                              for item in value.get('observations', [])]
    if state['status'] in ACTIVE:
        result['supervisor_alive'] = process_matches(state.get('supervisor'))
    return result


def list_runs(root, *, offset=0, limit=20, cursor=None):
    from demiflow.workbench.projects import index
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
        raise ForgeError('Invalid run page')
    info = index.status(root)
    if info.get('reason') == 'index_not_initialized':
        if cursor is not None:
            raise ForgeError('Legacy directory requires explicit index rebuild before cursor paging')
        entries = []
        for identity in _run_ids(root, maximum=128):
            _, value = _load(root, identity, detail='identity')
            entries.append(dict(identity=identity, digest=value['manifest']['digest'], created_at=value['manifest']['created_at']))
        entries.sort(key=lambda item: (item['created_at'], item['identity']), reverse=True)
        page = dict(entries=entries[offset:offset + limit], total=len(entries), next_cursor=None, index=info)
    else:
        page = index.page(root, 'run', offset=offset, limit=limit, cursor=cursor)
    values, size = [], 0
    for item in page['entries']:
        run_id, digest = item['identity'], item['digest']
        _, value = _load(root, run_id, detail='identity')
        outputs = value['manifest']['outputs']
        if len(outputs) > 32:
            raise ForgeError('Output summary exceeds action budget')
        actions = tuple(spec['action_id'] for spec in outputs.values())
        _, value = _load(root, run_id, detail='summary', action_ids=actions)
        if value['manifest']['digest'] != digest:
            raise ForgeError('Run manifest changed while listing; retry')
        item = _describe(run_id, value)
        item = {k: item[k] for k in ('run_id', 'status', 'entry', 'created_at', 'parameter_path', 'outputs', 'state')}
        size += len(bounded_json(item, 8 * 1024**2))
        if size > 8 * 1024**2:
            raise ForgeError('Run page exceeds byte budget; request fewer runs')
        values.append(item)
    return dict(status='listed', runs=values, total=page['total'], next_cursor=page['next_cursor'], index=page['index'])


def process_matches(identity):
    return bool(identity and process_identity(identity['pid']) == identity)


def verify_seal(target, manifest, *, environment=True):
    report = _report(target, manifest)
    from demiflow.workbench.runs.native_resources import verify
    verify(target, manifest.get('native_execution'))
    if manifest.get('format') not in {FORMAT, PREVIOUS_FORMAT, LEGACY_EXECUTABLE_FORMAT} or report.get('format') != 'demiflow_forge_flow_inspection_v3':
        raise ForgeError('Historical execution records are read-only; prepare a newly inspected run')
    if manifest['format'] in {FORMAT, PREVIOUS_FORMAT}:
        if 'change_scope' not in manifest:
            raise ForgeError('Run has no change scope binding')
        scope = manifest['change_scope']
        audit.scope_digest(scope)
        if scope is not None:
            audit.verify_baseline(ws.read_control(target / 'change-baseline.json', audit.BASELINE_BYTES),
                                  scope=scope, root=manifest['root'], project_id=manifest['workspace']['id'])
    for name, digest in report['binding']['sources'].items():
        if hashlib.sha256(read_bounded(target / 'source' / name, 1024**2)).hexdigest() != digest:
            raise ForgeError('Sealed candidate source differs')
    for resource in report['binding'].get('resources', {}).values():
        if resource['kind'] in {'config', 'file'} and hashlib.sha256(read_bounded(target / 'source' / resource['path'], resource.get('max_bytes', 1024**2))).hexdigest() != resource['sha256']:
            raise ForgeError('Sealed resource differs')
    for item in manifest['platform']['files']:
        if hashlib.sha256(read_bounded(target / 'runtime/demiflow' / item['path'], manifest['platform']['limits']['file_bytes'])).hexdigest() != item['sha256']:
            raise ForgeError('Sealed platform source differs')
    if environment:
        p = manifest['platform']
        if environment_inventory(p['declaration'], p['limits'], extras=p['environment']['extras']) != p['environment']:
            raise ForgeError('Run dependency metadata changed')
    return report


@contextmanager
def _launch_unit(target, attempt):
    from demiflow.forge.io import fsync_directory
    import stat
    path = target / ('unit-' + str(attempt) + '.lock')
    unit = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(unit)
        if not stat.S_ISREG(info.st_mode):
            raise ForgeError('Invalid execution unit fence')
        try:
            fcntl.flock(unit, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ForgeError('Execution unit is still owned', code='busy') from None
        os.fsync(unit)
        fsync_directory(target)
        yield unit, {'device': info.st_dev, 'inode': info.st_ino}
    finally:
        os.close(unit)


@operation_budget.controlled
def start(root, run_id, *, expected, expected_attempt=_CURRENT_ATTEMPT, authority=None):
    from demiflow.workbench.worker import operation
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_submit")
    root = Path(root).absolute()
    # Potentially expensive source/platform checks run in the bounded inspector.
    existing = show(root, run_id)
    if existing['digest'] != expected:
        raise ForgeError('Prepared digest differs', code='conflict')
    expected_attempt = _attempt(existing['attempt'], expected_attempt)
    if existing['status'] != 'prepared':
        return existing
    from demiflow.workbench.runs import domain as run_domain
    from demiflow.resources.process_observation import require_pidfds
    require_pidfds()
    operation_budget.check()
    with run_domain.lock() as (domain, capacity), execution_lock(root) as lease:
        target, value = _load(root, run_id)
        if value['manifest']['digest'] != expected:
            raise ForgeError('Prepared digest differs', code='conflict')
        _attempt(value['state'].get('attempt'), expected_attempt)
        if value['state']['status'] != 'prepared':
            return show(root, run_id)
        other = _lease_owner(root, lease)
        if other is not None and other != run_id and _load(root, other, detail='identity')[1]['state']['status'] in ACTIVE:
            raise ForgeError('Another run is active or needs explicit reconciliation')
        if value['manifest']['format'] != FORMAT:
            raise ForgeError('Older prepared seals have no managed runtime contract; prepare a new run')
        attempt = value['state'].get('attempt')
        from demiflow.workbench.conversations import admission as development_admission
        if attempt == 1:
            development_admission.check(root, entry=value['manifest']['entry'], expected=value['manifest'].get('development_acceptance'))
        run = RunJournal(target, attempt=attempt)
        from demiflow.workbench.projects import index
        with ws.control_lock(root):
            operation_budget.check()
            with run_domain.preparation_lock() as preparation_lease:
                preflight = operation('run_preflight', root, run_id=run_id, expected=expected,_pass_fds=(preparation_lease,), _authority=authority.bootstrap())
            if preflight['status'] != 'checked':
                raise ForgeError('Run changed before admission', code='conflict')
            marker = index.begin(root, 'run', run_id)
            run_domain.history_admission(root, reserve=0, admitting=run_id, ceiling=capacity['history_bytes'])
            operation_budget.check()
            # Establish the durable execution fence before any visible admission.
            # A prepared retry may reuse an unlocked fence left before reservation.
            development_guard = (development_admission.guard(root, entry=value['manifest']['entry'], expected=value['manifest'].get('development_acceptance'))
                                 if attempt == 1 else nullcontext())
            with authority.guard(root, 'run_submit'), development_guard, _launch_unit(target, attempt) as (unit, unit_identity):
                process = reservation = None
                spawn_attempted = False
                try:
                    operation_budget.check()
                    reservation = run_domain.reserve(domain, capacity, root, value['manifest'], attempt)
                    operation_budget.check()
                    _claim_lease(lease, run_id)
                    run.transition({'prepared'}, 'starting', started_at=time.time(), deployment=reservation,
                                   execution_stopped=False, unit_identity=unit_identity, launch_phase='admitted')
                    index.publish(root, marker)
                    # Persist uncertainty before Popen; an interrupted launch may
                    # have forked even when no process identity was recorded yet.
                    usage = value['state'].get('usage', {})
                    run.record_usage(elapsed_ms=usage.get('elapsed_ms', 0),
                                     output_bytes=usage.get('output_bytes', 0), complete=False)
                    for _ in range(2):
                        state = run.execution_status()
                        if state['status'] not in {'starting', 'stopping'}:
                            raise ForgeError('Launch state changed', code='conflict')
                        if run.transition({state['status']}, state['status'], launch_phase='spawning'):
                            break
                    else:
                        raise ForgeError('Launch state changed', code='conflict')
                    worker_path = next((item['path'] for item in value['manifest']['platform']['files']
                                        if item['path'] in {'workbench/runs/worker.py', 'forge/run_worker.py'}), None)
                    if worker_path is None:
                        raise ForgeError('Sealed run has no supported supervisor entry', code='operation_rejected')
                    command = [sys.executable, '-I', '-B', str(target / 'runtime/demiflow' / worker_path), 'supervise', str(root), run_id, str(lease)]
                    if attempt is not None:
                        command.append(str(attempt))
                    env = worker_environment(target, value['manifest']['limits'], value['manifest'].get('credential_names', []),
                                             native_execution=value['manifest'].get('native_execution'),
                                             capacity_domain=value['manifest'].get('capacity_domain'))
                    env['DEMIFLOW_EXECUTION_UNIT_FD'] = str(unit)
                    operation_budget.check()
                    spawn_attempted = True
                    process = subprocess.Popen(command, cwd=target, env=env, stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        start_new_session=True, pass_fds=(lease, unit))
                    run.transition({'starting'}, 'starting', supervisor=process_identity(process.pid))
                except BaseException as exc:
                    if not spawn_attempted and reservation is not None:
                        usage = value['state'].get('usage', {})
                        run.record_usage(elapsed_ms=usage.get('elapsed_ms', 0),
                                         output_bytes=usage.get('output_bytes', 0), complete=True)
                        state = run.execution_status()
                        cleanup = 'pending' if attempt_directory(target, 'work', attempt).exists() else 'complete'
                        closed = run.transition({'prepared', 'starting', 'stopping'},
                            'cancelled' if state['status'] == 'stopping' else 'failed',
                            error=str(exc)[:2048], recoverable=True, reason='launch_failed',
                            execution_stopped=True, cleanup=cleanup, stop_phase='confirmed')
                        if closed and cleanup == 'complete':
                            run_domain.abandon(domain, reservation['slot'], run_id=run_id, attempt=attempt)
                    # Once spawned, retain uncertain state and ownership. The
                    # durable unit permits explicit recovery after launcher loss.
                    raise
    return show(root, run_id)


def _preflight(root, run_id, *, expected, authority=None):
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_submit")
    operation_budget.check()
    target, value = _load(root, run_id)
    m = value['manifest']
    if m['digest'] != expected:
        raise ForgeError('Prepared digest differs', code='conflict')
    if value['state']['status'] != 'prepared':
        return {'status': value['state']['status']}
    if value['state'].get('attempt') == 1:
        from demiflow.workbench.conversations import admission as development_admission
        development_admission.check(root, entry=m['entry'], expected=m.get('development_acceptance'))
    identity = m.get('prepare_request_id')
    if identity and not value['state'].get('preparation_ready'):
        from demiflow.workbench.runs.requests import read
        receipt = read(root, identity)
        if receipt['status'] != 'ready' or receipt.get('digest') != m['digest']:
            raise ForgeError('Preparation receipt is not durably ready', code='conflict')
    _check_basis(root, target, m)
    validate_output_targets(value)
    remaining_limits(m['limits'], value['state'])
    return {'status': 'checked'}


def _check_basis(root, target, m, *, phase='preflight'):
    verify_seal(target, m)
    ws.verify_run_basis(root, m)


def _output_metadata(uri, version):
    """Keep output IO in the existing bounded metadata worker."""
    from demiflow.workbench.worker import operation
    return operation('metadata', uri, version=version)


def _verified_output(target, manifest, output, *, attempt, recover=False,
                     require_current_head=False, deadline=None):
    from demiflow.execution.host.inspection import verify_write_receipt, verify_published_output
    options = dict(expected_uri=output['uri'], attempt=attempt, recover=recover,
                   require_current_head=require_current_head, deadline=deadline,
                   metadata_reader=_output_metadata)
    if output.get('mode') == 'checkpoint' or output.get('publication') == 'native_stage_v3':
        return verify_published_output(target, output['action_id'],
            expected_schema_identity=output['schema_identity'],
            expected_checkpoint_node=output.get('checkpoint_node'),
            publication=output.get('publication'), report_digest=manifest['report_digest'],
            parents=list(_report(target, manifest)['actions']) if recover else None, **options)
    return verify_write_receipt(target, output['action_id'], **options)


def validate_output_targets(value, *, deadline=None):
    """An existing target must be proven owned, never merely named in a seal."""
    from demiflow.lance.storage import inspect_lance
    if deadline is None:
        deadline = operation_budget.current_deadline()
    manifest = value['manifest']
    target = directory(manifest['root'], manifest['run_id'])
    details = _describe(value['manifest']['run_id'], value)
    for output in details['outputs']:
        operation_budget.check(deadline)
        ws.no_symlinks(Path(output['uri']))
        if output.get('publication') == 'native_stage_v3':
            from .native_stages import admit_prepared
            owned = admit_prepared(target, RunJournal(target), manifest, output)
            if owned is not None:
                if inspect_lance(output['uri']).resolved_version != owned['version']:
                    raise ForgeError('Owned native stage head changed outside its original publication')
                continue  # Admission only; original coordinator still owns ACK/recovery.
        if output['status'] == 'committed':
            verified = _verified_output(target, manifest, output,
                attempt=value['state'].get('attempt'), require_current_head=True,
                deadline=deadline)
            if verified is None:
                raise ForgeError('Owned output has no verified native publication')
            continue
        if output.get('mode', 'create') in {'create', 'checkpoint'} and Path(output['uri']).exists():
            raise ForgeError('Create target already exists')
        if output.get('mode') == 'overwrite':
            if inspect_lance(output['uri']).resolved_version != output['expected_version']:
                raise ForgeError('Overwrite CAS baseline changed')
    operation_budget.check(deadline)


def remaining_limits(limits, state):
    """Cumulative time/allocation budgets; peak and size limits remain absolute."""
    usage = state.get('usage', {})
    elapsed = usage.get('elapsed_ms', 0)
    output = usage.get('output_bytes', 0)
    if (elapsed >= limits['timeout_s'] * 1000 or output >= limits['max_output_bytes']):
        raise ForgeError('Cumulative run budget exhausted; resume cannot reset it')
    result = dict(limits)
    result['timeout_s'] = (limits['timeout_s'] * 1000 - elapsed) / 1000
    result['max_output_bytes'] -= output
    return result


@operation_budget.controlled
def resume(root, run_id, *, expected, expected_attempt, authority=None):
    from demiflow.workbench.worker import operation
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_submit")
    # Same-engine resume admission with a concrete reason before any business start.
    # History is read-only: this never rewrites source nor creates a replacement
    # successful run; per-action compatibility is enforced by the recovery path.
    from demiflow.execution.host.inspection import platform_identity
    binding = RunJournal(directory(root, run_id)).recovery_identity()
    if binding is None:
        raise ForgeError('该运行没有统一恢复契约，不能恢复历史；请新建运行。', code='conflict')
    if binding.get('platform_revision') != platform_identity():
        raise ForgeError('引擎实现已变更，恢复需要原引擎版本；不能重写源或创建替代成功运行。', code='conflict')
    from demiflow.workbench.runs.domain import preparation_lock
    with preparation_lock() as lease:
        prepared = operation('run_resume', root, run_id=run_id, expected=expected, expected_attempt=expected_attempt,_pass_fds=(lease,), _authority=authority.bootstrap())
    return start(root, run_id, expected=expected, expected_attempt=prepared['attempt'], authority=authority)


def _resume(root, run_id, *, expected, expected_attempt, authority=None):
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_submit")
    operation_budget.check()
    if type(expected_attempt) is not int or not 1 <= expected_attempt < 32:
        raise ForgeError('Resume requires the stopped attempt identity', code='invalid_request')
    with execution_lock(root):
        target, value = _load(root, run_id)
        state, manifest = value['state'], value['manifest']
        if manifest['digest'] != expected or state.get('attempt') != expected_attempt:
            raise ForgeError('Run binding or execution attempt changed', code='conflict')
        if state['status'] not in {'cancelled', 'interrupted'} and not (state['status'] == 'failed' and state.get('recoverable') is True):
            raise ForgeError('Only stopped interruptions or identified technical failures can resume')
        if manifest['format'] != FORMAT:
            raise ForgeError('Older seals cannot acquire the managed runtime contract; prepare a new run')
        if not execution_stopped(target, state):
            raise ForgeError('Owned execution unit has not stopped')
        if any(process_matches(state.get(name)) for name in ('supervisor', 'worker')):
            raise ForgeError('Run processes have not stopped')
        if state.get('usage_complete') is not True:
            raise ForgeError('Final resource accounting is unavailable; resume cannot grant an unknown budget')
        if state.get('cleanup') != 'complete':
            raise ForgeError('Explicitly reconcile pending cleanup before resuming', code='conflict')
        remaining_limits(manifest['limits'], state)
        from demiflow.execution.host.inspection import action_recovery_facts
        for action_id in _fact_actions(_report(target, manifest), manifest['outputs']):
            facts = action_recovery_facts(target, action_id, attempt=expected_attempt,
                                          deadline=operation_budget.current_deadline())
            if facts['recovery_restricted']:
                raise ForgeError('该执行的资源 actor 未声明状态恢复钩子，不能从中断处恢复；请新建运行。')
        _check_basis(root, target, manifest, phase='resume')
        from demiflow.workbench.runs.storage import cleanup_deadline, clean_stopped
        deadline = cleanup_deadline()
        errors = register_results(root, run_id, recover=True, maintenance_deadline=deadline)
        if errors:
            raise ForgeError('Resume reconciliation failed: ' + '; '.join(errors))
        _, value = _load(root, run_id)
        clean_stopped(RunJournal(target, attempt=expected_attempt),
            expected_revision=value['state']['revision'], deadline=deadline,
            confirm_stopped=lambda snapshot: execution_stopped(target, snapshot))
        _, value = _load(root, run_id)
        _attempt(value['state'].get('attempt'), expected_attempt)
        if value['state'].get('cleanup') != 'complete' or value['state'].get('usage_complete') is not True:
            raise ForgeError('Resume cleanup or final accounting is no longer complete', code='conflict')
        remaining_limits(manifest['limits'], value['state'])
        with read_run(target, deadline=deadline) as reader:
            native = reader.native_maintenance_state()
        if not native['owner_released'] or not native['gc_settled']:
            raise ForgeError('Native maintenance is not settled', code='conflict')
        validate_output_targets(value, deadline=deadline)
        control = RunJournal(target)
        from demiflow.workbench.projects import index
        with ws.control_lock(root):
            _check_basis(root, target, manifest, phase='resume')
            operation_budget.check(deadline)
            marker = index.begin(root, 'run', run_id)
            report = _report(target, manifest)
            operation_budget.check(deadline)
            with authority.guard(root, 'run_submit'):
                control.begin_attempt(expected_attempt=expected_attempt,
                    expected_revision=value['state']['revision'],
                    nodes=[(item['action_id'], item['node_id'], item['occurrence'])
                           for item in manifest['observation_instances']],
                    actions=_fact_actions(report, manifest['outputs']))
                index.publish(root, marker)
    return show(root, run_id)


def worker_environment(target, limits=None, credential_names=(), *, native_execution=None, capacity_domain=None):
    limits = limits or DEFAULT_LIMITS
    result = dict(PATH=os.path.dirname(sys.executable) + ':/usr/bin:/bin', LANG='C.UTF-8',
        PYTHONDONTWRITEBYTECODE='1', PYTHONNOUSERSITE='1',
        PYTHONPATH=str(target / 'runtime') + os.pathsep + str(target / 'source'),
        TMPDIR=str(target / 'work'), OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1',
        MKL_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1', RAYON_NUM_THREADS='2',
        # Bound glibc's per-thread virtual arena reservations before startup,
        # retaining the worker's independent address-space and RSS protections.
        MALLOC_ARENA_MAX='2',
        TOKIO_WORKER_THREADS='2', LANCE_CPU_THREADS='2', CUDA_VISIBLE_DEVICES='',
        LANCE_MEM_POOL_SIZE=str(limits['memory_bytes']), LANCE_MAX_TEMP_DIRECTORY_SIZE=str(limits['max_scratch_bytes']))
    from demiflow.workbench.runs.native_resources import environment
    result.update(environment(target, native_execution))
    if capacity_domain is not None:
        from demiflow.workbench.runs.native_resources import capacity_domain as validate_domain
        root = validate_domain(capacity_domain)
        if result.get('DEMIFLOW_RESOURCE_ROOT', root) != root:
            raise ForgeError('Native deployment and capacity domain differ')
        result['DEMIFLOW_RESOURCE_ROOT'] = root
    for name in credential_names:
        if not re.fullmatch(r'[A-Z_][A-Z0-9_]{0,127}', name) or name in result or name in {'HOME', 'LD_PRELOAD', 'LD_LIBRARY_PATH'} or name.startswith(('PYTHON', 'DEMIFLOW_', 'CUDA_')):
            raise ForgeError('Credential name conflicts with worker control environment')
        value = os.environ.get(name)
        if not value or len(value.encode()) > 16384:
            raise ForgeError('Declared credential is unavailable or exceeds budget: ' + name)
        result[name] = value
    return operation_budget.clear_worker_environment(result)


def evidence(root, run_id, reference):
    """Read one native evidence reference from this run's declared ledger."""
    target, value = _load(root, run_id)
    from demiflow.workbench.catalog import validate_reference
    from demiflow.workbench.catalog import read_evidence
    validate_reference(reference)
    path = Path(reference['journal_path'])
    path = path if path.is_absolute() else target / path
    ws.no_symlinks(path)
    allowed = next((r for r in value['manifest'].get('evidence', []) if target / r['path'] == path), None)
    if allowed is None or not path.is_file() or path.stat().st_size > allowed['max_bytes']:
        raise ForgeError('Evidence is not a declared bounded resource of this run')
    if allowed['format'] != 'demiflow_prompt_sqlite_v1':
        raise ForgeError('Native evidence format has no bounded reader')
    document = read_evidence(reference, path)
    return {'run_id': run_id, 'reference': reference, 'format': allowed['format'], 'document': document}


def cancel(root, run_id, *, expected_attempt=_CURRENT_ATTEMPT,compact=False, authority=None):
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_control")
    if type(compact) is not bool:raise ForgeError('compact must be boolean',code='invalid_request')
    from demiflow.workbench.runs.view import control_state
    from demiflow.workbench.runs.view import snapshot
    target, value = control_state(root,run_id)
    attempt = _attempt(value['state'].get('attempt'), expected_attempt)
    run = RunJournal(target, attempt=attempt)
    try:
        with authority.guard(root, 'run_control'):
            run.transition({'starting', 'running'}, 'stopping', cancel_requested_at=time.time(), stop_phase='requested')
    except ExecutionConflict as exc:
        raise ForgeError(str(exc), code='conflict') from exc
    return snapshot(root,run_id,attempt=attempt) if compact and value.get('managed') else show(root, run_id, attempt=attempt)


def result_table(root, run_id, name, *, attempt=None):
    target, value = _load(root, run_id, attempt=attempt)
    item = next((o for o in _describe(run_id, value)['outputs'] if o['name'] == name), None)
    if item is None or item['result'] is None:
        raise ForgeError('This output has no verified fixed table reference')
    from demiflow.lance.refs import DatasetRef
    ref = DatasetRef.from_dict(item['result']['ref'])
    verified = _verified_output(target, value['manifest'], item,
        attempt=value['state'].get('attempt'), deadline=operation_budget.current_deadline())
    if (verified is None or verified['version'] != ref.lance_version
            or verified['schema_hash'] != ref.schema_hash or verified['row_count'] != ref.row_count
            or ref.relative_uri != item['relative_uri'] or ref.store_id != item['location_id']):
        raise ForgeError('Run result reference differs from its verified native publication')
    root, workspace, _ = ws.load_workspace(root)
    if ref.store_id not in workspace['locations']:
        raise ForgeError('Result location is no longer declared')
    base = ws.location_root(root, workspace['locations'][ref.store_id])
    uri = ws.relative_path(base, ref.relative_uri)
    if str(uri) != item['uri']:
        raise ForgeError('Result location changed from the sealed output')
    alias = f'run:{run_id}:{name}' + (f':{attempt}' if attempt is not None else '')
    return {'format': 'demiflow_forge_table_v1', 'alias': alias,
            'location_id': ref.store_id, 'ref': ref.to_dict()}, ref, uri


def attach_result(root, run_id, output, alias, *, attempt, expected_result):
    """Name one observed, verified result without replacing an existing alias."""
    if attempt is None:
        _attempt(show(root, run_id)['attempt'], None)
    elif type(attempt) is not int or not 1 <= attempt <= 32:
        raise ForgeError('Attempt must be an integer in 1..32', code='invalid_request')
    if type(expected_result) is not str or not re.fullmatch(r'[0-9a-f]{64}', expected_result):
        raise ForgeError('Invalid fixed result digest', code='invalid_request')
    _, ref, _ = result_table(root, run_id, output, attempt=attempt)
    if content_digest(ref.to_dict()) != expected_result:
        raise ForgeError('Fixed result differs from the observed reference', code='conflict')
    return ws.attach_table(root, alias, relative_uri=ref.relative_uri, version=ref.lance_version,
                           location_id=ref.store_id, dataset_id=ref.dataset_id,
                           schema_name=ref.schema_name, schema_version=ref.schema_version,
                           expected_ref=ref)


def register_results(root, run_id, *, recover=False, attempt=None, maintenance_deadline=None):
    from demiflow.lance.refs import DatasetRef
    from demiflow.execution.host.inspection import action_recovery_facts
    from demiflow.execution.host.control import admit_absent_create
    target, value = _load(root, run_id, attempt=attempt)
    selected = value['state'].get('attempt')
    run = RunJournal(target, attempt=selected)
    if recover and maintenance_deadline is None:
        from demiflow.workbench.runs.storage import cleanup_deadline
        maintenance_deadline = cleanup_deadline()
    deadline = maintenance_deadline if recover else operation_budget.current_deadline()
    errors = []
    for name, output in value['manifest']['outputs'].items():
        projection = value['output_projections'][output['action_id']]
        evidence = projection['evidence']
        if projection['result'] and (output.get('mode') != 'checkpoint'
                and output.get('publication') != 'native_stage_v3' or evidence is not None
                and projection['result_ref']['lance_version'] == evidence['version']):
            continue
        try:
            verified = _verified_output(target, value['manifest'], output,
                attempt=selected, recover=recover, deadline=deadline)
            if verified is None:
                if recover and output.get('mode', 'create') == 'create':
                    facts = action_recovery_facts(target, output['action_id'],
                                                  attempt=selected, deadline=deadline)
                    if facts['absence_eligible']:
                        admit_absent_create(run, action_id=output['action_id'],
                            expected_attempt=selected, expected_revision=facts['revision'],
                            expected_uri=output['uri'], deadline=deadline,
                            confirm_stopped=lambda snapshot: execution_stopped(target, snapshot))
                continue
            recording = verified['recording']
            if recording is not None:
                if (recording['action_id'] != output['action_id']
                        or recording['attempt'] != (selected if selected is not None else 1)
                        or recording['kind'] not in {'recovered', 'stage_recovered'}):
                    raise ForgeError('Recovered publication is bound to a different action or attempt')
                run.action_event(output['action_id'], recording['kind'], recording['value'])
            if verified['action_id'] != output['action_id'] or verified['uri'] != output['uri']:
                raise ForgeError('Verified publication differs from the sealed output')
            ref = DatasetRef(output['dataset_id'], output['relative_uri'], verified['version'],
                output['schema_name'], output['schema_version'], verified['schema_hash'],
                verified['row_count'], store_id=output['location_id'])
            if verified['result_kind'] not in {'result', 'recovered_result'}:
                raise ForgeError('Invalid result registration kind')
            run.action_event(output['action_id'], verified['result_kind'],
                             {'ref': ref.to_dict(), 'location_id': output['location_id']})
        except Exception as exc:
            errors.append(name + ': ' + str(exc)[:512])
    return errors


def execution_stopped(target, state):
    from demiflow.resources.process_observation import unit_stopped
    if any(process_matches(state.get(name)) for name in ('supervisor', 'worker')):
        return False
    try:
        path = target / ('unit-' + str(state.get('attempt')) + '.lock')
        return unit_stopped(path, state.get('unit_identity'))
    except FileNotFoundError:
        # Only a positively recorded pre-spawn failure can lack a unit file.
        return state.get('status')=='prepared' or state.get('execution_stopped') is True and state.get('reason') == 'launch_failed'
def reconcile(root, run_id, *, expected_attempt=_CURRENT_ATTEMPT):
    from demiflow.workbench.worker import operation
    from demiflow.workbench.runs.domain import preparation_lock
    expected_attempt = _attempt(show(root, run_id)['attempt'], expected_attempt)
    with preparation_lock() as lease:
        return operation('run_reconcile', root, run_id=run_id, expected_attempt=expected_attempt,_pass_fds=(lease,))


def _reconcile(root, run_id, *, expected_attempt):
    from demiflow.workbench.runs.storage import cleanup_deadline, remove_attempt_work, clean_stopped
    with execution_lock(root):
        target, value = _load(root, run_id)
        state = value['state']
        _attempt(state.get('attempt'), expected_attempt)
        if process_matches(state.get('supervisor')) or process_matches(state.get('worker')):
            raise ForgeError('Run processes have not stopped')
        modern = value['manifest']['format'] == FORMAT
        if modern and not execution_stopped(target, state):
            raise ForgeError('Owned execution unit has not stopped; capacity remains reserved')
        run = RunJournal(target, attempt=state.get('attempt'))
        completed = state.get('cleanup') == 'complete'
        work = attempt_directory(target, 'work', state.get('attempt'))
        if completed and os.path.lexists(work):
            raise ForgeError('Completed cleanup contradicts remaining private work', code='conflict')
        if not completed:
            from demiflow.workbench.runs.domain import reserved
            if state['status'] == 'prepared' and reserved(root, run_id, state.get('attempt')):
                run.transition({'prepared'}, 'failed', reason='launch_failed',
                               recoverable=True, execution_stopped=True)
            if state['status'] in ACTIVE:
                if state.get('launch_phase') == 'admitted':
                    usage = state.get('usage', {})
                    run.record_usage(elapsed_ms=usage.get('elapsed_ms', 0),
                                     output_bytes=usage.get('output_bytes', 0), complete=True)
                    run.transition(ACTIVE, 'cancelled' if state['status'] == 'stopping' else 'failed',
                                   reason='launch_failed', recoverable=True,
                                   execution_stopped=True, stop_phase='confirmed')
                else:
                    run.transition(ACTIVE, 'interrupted', reason='supervisor_lost',
                                   execution_stopped=True, stop_phase='confirmed')
        deadline = cleanup_deadline()

        def stopped(snapshot):
            if modern:
                return execution_stopped(target, snapshot)
            # Known historical hosts predate execution-unit files. Retain their
            # original process-identity proof; the native runtime lease is also held.
            return not any(process_matches(snapshot.get(name)) for name in ('supervisor', 'worker'))

        try:
            errors = register_results(root, run_id, recover=True, maintenance_deadline=deadline)
            current = run.execution_status()
            clean_stopped(run, expected_revision=current['revision'],
                          confirm_stopped=stopped, deadline=deadline)
            if not completed:
                remove_attempt_work(work, deadline=deadline)
            if not completed:
                current = run.execution_status()
                if not run.transition({current['status']}, current['status'],
                                      execution_stopped=True, stop_phase='confirmed', cleanup='complete'):
                    raise ForgeError('Cleanup state changed before final registration', code='conflict')
        except Exception as exc:
            if not completed:
                try:
                    current = run.execution_status()
                    run.transition({current['status']}, current['status'], cleanup='pending',
                        cleanup_error=dict(type=type(exc).__name__, message=str(exc)[:1024]))
                except (OSError, ValueError):
                    pass  # A failed final registration never establishes completion.
            raise
        finally:
            refresh_index(root, run_id, deadline=deadline)
    result = show(root, run_id)
    result['reconciliation_errors'] = errors
    return result


def compare(root, left, right):
    a, b = show(root, left, include_report=True), show(root, right, include_report=True)
    return dict(status='compared', left=a, right=b,
        same_source=a['report']['binding']['sources'] == b['report']['binding']['sources'],
        same_inputs=a['report']['binding']['tables'] == b['report']['binding']['tables'],
        quality_evaluation='not_defined')


def source(root, run_id, path):
    target, value = _load(root, run_id,detail='identity')
    report = _report(target, value['manifest'])
    expected = report['binding']['sources'].get(path)
    if expected is None:
        resource = report['binding'].get('resources', {}).get(path, {})
        if resource.get('kind') == 'config':
            expected = resource['sha256']
    if expected is None:
        raise ForgeError('Source is outside this run closure')
    raw = read_bounded(ws.relative_path(target / 'source', path), 1024**2)
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ForgeError('Sealed source differs')
    return dict(path=path, text=raw.decode('utf-8'))


def retain_logs(target, attempt, budget):
    """Evict only stopped prior-attempt logs before reserving the next tail."""
    parent=target/'logs'
    parent.mkdir(exist_ok=True)
    retained=[]
    with os.scandir(parent) as entries:
        for count,entry in enumerate(entries):
            if count>=32 or entry.is_symlink() or not entry.is_dir(follow_symlinks=False) or not re.fullmatch(r'attempt-[0-9]{1,2}',entry.name):
                raise ForgeError('Invalid managed log history')
            number=int(entry.name.split('-')[1])
            if number>=attempt:
                continue
            retained.append((number,Path(entry.path),_tree_bytes(Path(entry.path),entries_limit=8)))
    # The current attempt tail cannot exceed the cumulative 64 MiB policy.
    if budget>64*1024**2:
        raise ForgeError('Attempt log reservation exceeds cumulative retention capacity')
    used=sum(size for _,_,size in retained)
    for _,path,size in sorted(retained):
        if used+budget<=64*1024**2:
            break
        shutil.rmtree(path)
        used-=size
        # A tiny explicit tombstone allows old readers to distinguish eviction.
        path.mkdir()
        ws.write_control(path/'retention.json',{'evicted':True},create=True)


def logs(root, run_id, *, offset=0, limit=65536, attempt=None):
    target, value = _load(root, run_id, attempt=attempt,detail='identity')
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 65536:
        raise ForgeError('Invalid log range')
    parent = attempt_directory(target, 'logs', value['state'].get('attempt'))
    ws.no_symlinks(parent)
    if not parent.exists():
        return dict(text='', next_offset=offset, truncated=False)
    if (parent/'retention.json').exists():
        return dict(text='',next_offset=offset,truncated=True,evicted=True)
    paths=[]
    with os.scandir(parent) as entries:
        for entry in entries:
            if len(paths)>=5:
                raise ForgeError('Invalid run log segment count')
            paths.append(Path(entry.path))
    paths.sort()
    if len(paths) > 5 or any(not re.fullmatch(r'[0-9]{20}\.log', p.name) for p in paths):
        raise ForgeError('Invalid run log segments')
    base = int(paths[0].stem) if paths else 0
    position = max(offset, base)
    chunks = []
    remaining = limit
    for path in paths:
        ws.no_symlinks(path)
        start = int(path.stem)
        try:
            with path.open('rb') as stream:
                stream.seek(max(0, position - start))
                raw = stream.read(remaining)
        except FileNotFoundError:
            continue  # Rotation won the race; the next poll reports the new base.
        if raw:
            chunks.append(raw)
            remaining -= len(raw)
            position = max(position, start) + len(raw)
        if not remaining:
            break
    import codecs
    raw=b''.join(chunks)
    skipped=0
    while skipped<min(3,len(raw)) and raw[skipped]&0xc0==0x80:
        skipped+=1
    decoder=codecs.getincrementaldecoder('utf-8')(errors='replace')
    text=decoder.decode(raw[skipped:],final=value['state']['status'] in TERMINAL and remaining>0)
    pending,_=decoder.getstate()
    return dict(text=text,next_offset=position-len(pending),
                truncated=offset<base or skipped>0,base_offset=base)
