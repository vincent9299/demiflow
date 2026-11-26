"""One local experiment: immutable preparation, native execution, fixed results.

The native RunJournal owns run facts. This module never schedules graph nodes,
interprets Python for execution, writes Lance data, or replaces native receipts.
"""
from contextlib import contextmanager
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
from demiflow.workbench.python_sources import bound_files, sealed_root, source_digest
from demiflow.workbench.process import process_identity
from demiflow.workbench.runs.authority import LocalAuthority
from demiflow.workbench.runs.admission import inputs as admit_inputs
from demiflow.forge import audit
from demiflow.forge.platform import content_digest
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform_environment import read_bounded, regular_file_chunks
from demiflow.forge.platform_environment import platform_declaration
from demiflow.forge.platform_environment import environment_inventory
from demiflow.forge.project_bindings import extract_arguments, is_direct_table_ref, resolve_arguments, resolve_output_uri
from demiflow.forge.schema import ForgeError
from demiflow.forge import operation_budget
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
from demiflow.workbench.pipeline_policy import limits_with
from demiflow.execution.host.control import RunJournal, RunContext
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


def _visible_logs_root(target):
    path = Path(target)
    if path.parent.name == 'runs' and path.parent.parent.name == '.forge':
        return path.parents[2] / 'logs' / path.name
    return path / 'logs'


def attempt_directory(target, kind, attempt=None):
    if kind not in {'work', 'logs', 'diagnostics'} or attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
        raise ForgeError('Invalid attempt directory')
    parent = _visible_logs_root(target) if kind == 'logs' else Path(target) / kind
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


def run_index_entry(root, run_id, *, deadline=None):
    """Read run-owned facts for the derived directory; missing is not unreadable."""
    from demiflow.execution.host.inspection import observe_storage
    current = operation_budget.current_deadline()
    deadline = current if deadline is None else deadline if current is None else min(deadline, current)
    operation_budget.check(deadline)
    target = directory(root, run_id)
    if not target.exists():
        return None
    try:
        with observe_storage(target, deadline=deadline) as observation:
            if not observation.exists:
                return None
    except RunReadError as exc:
        raise ForgeError(str(exc), code=exc.code or 'operation_rejected') from exc
    operation_budget.check(deadline)
    _, value = _load(root, run_id, detail='identity', deadline=deadline)
    manifest = value['manifest']
    return dict(kind='run', identity=run_id, created_at=manifest['created_at'],
                digest=manifest['digest'], active=value['state']['status'] in ACTIVE)


def publish_run_index(root, marker, *, deadline=None):
    """Optional cache work never undoes source publication or the original error."""
    from demiflow.workbench.projects import index
    try:
        operation_budget.check(deadline)
        entry = run_index_entry(root, marker['identity'], deadline=deadline)
        operation_budget.check(deadline)
        error = index.publish(root, marker, entry)
    except Exception as exc:
        error = str(exc)[:1024]
    if error:
        try:
            import logging
            logging.getLogger(__name__).warning('Directory run %s: %s; demiflow workbench index ROOT --synchronize',
                                                marker['identity'], error[:1024])
        except Exception:
            pass
    return error


def run_index_entries(root, *, check, deadline):
    """Explicit rebuild only: stream this owner's sources within shared budgets."""
    parent = ws.relative_path(root, '.forge/runs')
    if parent.exists():
        with os.scandir(parent) as entries:
            for entry in entries:
                check()
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                    raise ForgeError('Invalid run directory entry')
                value = run_index_entry(root, entry.name, deadline=deadline)
                if value is None:
                    raise ForgeError('Directory source is incomplete: ' + entry.name)
                yield value


def refresh_index(root, run_id, *, deadline=None):
    """Best-effort activity hint; source journal and execution lease own state."""
    from demiflow.workbench.projects import index
    current = operation_budget.current_deadline()
    deadline = current if deadline is None else deadline if current is None else min(deadline, current)
    try:
        if deadline is not None and time.monotonic() >= deadline:
            return 'Index refresh deferred after maintenance deadline'
        with ws.control_lock(root):
            if deadline is not None and time.monotonic() >= deadline:
                return 'Index refresh deferred after maintenance deadline'
            marker = index.begin(root, 'run', run_id)
            if deadline is not None and time.monotonic() >= deadline:
                return 'Index publication deferred after maintenance deadline'
            return publish_run_index(root, marker, deadline=deadline)
    except Exception as error:
        return str(error)[:1024]


def _copy_checked(source, target, expected, maximum):
    from demiflow.forge.io import publish_chunks

    def checked_chunks():
        digest = hashlib.sha256()
        for chunk in regular_file_chunks(source, maximum):
            operation_budget.check()
            digest.update(chunk)
            yield chunk
        operation_budget.check()
        if digest.hexdigest() != expected:
            raise ForgeError('Source changed while sealing: ' + source.name)

    operation_budget.check()
    target.parent.mkdir(parents=True, exist_ok=True)
    publish_chunks(target, checked_chunks())
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
def prepare(root, entry, *, params, outputs=None, new_outputs=True, limits=None, inputs=None,
            overrides=None, output_prefix=None, request_id=None, telemetry=True, authority=None):
    from demiflow.workbench.worker import operation
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, "run_submit")
    from demiflow.workbench.runs.domain import preparation_lock
    from functools import partial
    args = dict(params=params, outputs=outputs, new_outputs=new_outputs, limits=limits, inputs=inputs,
                overrides=overrides, output_prefix=output_prefix, telemetry=telemetry)
    from demiflow.workbench.runs.native_resources import trusted_deployment, capacity_domain, trusted_process_environment
    args['capacity_domain'] = capacity_domain()
    args.update(trusted_process_environment(workspace=root))
    if os.environ.get('DEMIFLOW_TRAINING_DEPLOYMENT'):
        args['training_location'] = os.environ['DEMIFLOW_TRAINING_DEPLOYMENT']
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


_OUTPUT_PREFIX_RE = re.compile(r'^[A-Za-z0-9_-]{1,64}(?:/[A-Za-z0-9_-]{1,64}){0,3}$')


def _validate_output_prefix(prefix):
    if prefix is None:
        return None
    if type(prefix) is not str or not _OUTPUT_PREFIX_RE.fullmatch(prefix):
        raise ForgeError('output_prefix must be 1..4 safe non-hidden path segments', code='invalid_request')
    return prefix


def _prepare(root, entry, *, params, outputs=None, new_outputs=True, limits=None, inputs=None,
             overrides=None, output_prefix=None,
             reserved_run_id=None, prepare_request_id=None, telemetry=True, authority=None, native_deployment=None,
             capacity_domain=None, environment_names=None, executable_dirs=None, source_modules=None, training_location=None):
    operation_budget.check()
    if type(telemetry) is not bool:
        raise ForgeError('telemetry must be boolean',code='invalid_request')
    output_prefix = _validate_output_prefix(output_prefix)
    authority = authority if authority is not None else LocalAuthority(str(Path(root).absolute()))
    authority.require(root, 'run_submit')
    from demiflow.workbench.projects.registry import project_dependencies
    dependencies = project_dependencies(root, authority=authority.bootstrap())
    root, workspace, platform = ws.load_workspace(root)
    platform = ws.current_platform_snapshot(platform)
    change_scope = workspace.get('audit', {}).get('change_scope')
    audit.require_scope(ws._check_change_scope(root), expected_scope=change_scope)
    requested_limits = limits
    limits, parameter_path = admit_inputs(root, workspace, entry, params, limits)
    explicit_limits = sorted(set(requested_limits or ()) | set(workspace.get('budgets') or ()))
    payload_defaults = {'max_row_bytes', 'max_batch_bytes'} - set(explicit_limits)
    if type(new_outputs) is not bool:
        raise ForgeError('new_outputs must be boolean')
    original = ws.read_control(parameter_path) if parameter_path else copy.deepcopy(params)
    if overrides is None:
        # Existing resolver validates the closed parameter format before modifications.
        resolve_arguments(root, workspace, original, table_resolver=_table_resolver())
    else:
        if type(overrides) is not dict or len(overrides) > 64:
            raise ForgeError('Parameter overrides must be an object of at most 64 entries', code='invalid_request')
        bounded_json(overrides, 256 * 1024)
        extract_arguments(original)
    effective = copy.deepcopy(original)
    eff_args, _ = extract_arguments(effective)
    if overrides is not None:
        for key, val in overrides.items():
            if type(key) is not str or not key or len(key) > 128:
                raise ForgeError('Override parameter names must be non-empty strings', code='invalid_request')
            eff_args[key] = copy.deepcopy(val)
        resolve_arguments(root, workspace, effective, table_resolver=_table_resolver())
    if inputs is not None:
        if type(inputs) is not dict or len(inputs) > 32:
            raise ForgeError('Input overrides must map entry arguments to fixed table references')
        for name, alias in inputs.items():
            arg = eff_args.get(name)
            is_tbl = type(arg) is dict and (arg.get('kind') == 'table' or is_direct_table_ref(arg))
            if type(alias) is not str or not is_tbl:
                raise ForgeError('Input override must name an existing table argument')
            if alias.endswith('.lance'):
                eff_args[name] = {'uri': alias}
            else:
                eff_args[name] = {'kind': 'table', 'alias': alias}
        resolve_arguments(root, workspace, effective, table_resolver=_table_resolver())
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
            if 'uri' in value and 'location_id' not in value:
                if new_outputs and mode in {'create', 'checkpoint'}:
                    base_prefix = f'data/{output_prefix}/' if output_prefix else 'data/'
                    candidate = f'{base_prefix}{name}.lance'
                    suffix = 2
                    while ws.relative_path(root, candidate).exists():
                        candidate = f'{base_prefix}{name}-{suffix}.lance'
                        suffix += 1
                    value['uri'] = candidate
                uri = resolve_output_uri(root, value['uri'])
                data_root = (root / 'data').absolute()
                if mode in {'create', 'checkpoint'}:
                    uri.parent.mkdir(parents=True, exist_ok=True)
                extra_spec = {
                    'location_id': 'data',
                    'location_root': str(data_root),
                    'relative_uri': uri.relative_to(data_root).as_posix(),
                }
            else:
                base = ws.location_root(root, workspace['locations'][value['location_id']])
                if new_outputs and mode in {'create', 'checkpoint'}:
                    sub = f'{output_prefix}/' if output_prefix else ''
                    prefix = sub if base.name == 'data' else f'data/{sub}'
                    candidate = f'{prefix}{name}.lance'
                    suffix = 2
                    while ws.relative_path(base, candidate).exists():
                        candidate = f'{prefix}{name}-{suffix}.lance'
                        suffix += 1
                    value['relative_uri'] = candidate
                uri = ws.relative_path(base, value['relative_uri'])
                if mode in {'create', 'checkpoint'}:
                    uri.parent.mkdir(parents=True, exist_ok=True)
                extra_spec = {}
            if mode in {'create', 'checkpoint'} and uri.exists():
                raise ForgeError('Create target already exists: ' + str(uri))
            if mode == 'overwrite':
                from demiflow.lance.storage import inspect_lance
                if inspect_lance(str(uri)).resolved_version != expected_version:
                    raise ForgeError('Overwrite CAS baseline changed')
            output_specs[name] = {**info, **value, **extra_spec, 'uri': str(uri), 'mode': mode, 'expected_version': expected_version}
        elif type(value) is dict:
            for key, child in value.items():
                visit(child, (*names, key))
        elif type(value) is list:
            for i, child in enumerate(value):
                visit(child, (*names, str(i)))

    visit(eff_args)
    from demiflow.workbench.runs import native_submission
    native_flow = not output_specs
    if native_flow:
        report, output_specs = native_submission.prepare(root, entry, parameters=effective, limits=limits,
                                                         metadata=metadata, explicit_limits=explicit_limits, source_modules=source_modules,
                                                         dependencies=dependencies)
        ledgers, budgets, credentials = [], {}, set(report['native_contract']['credential_names'])
        observation_instances = report['observation_instances']
    else:
        if not output_specs or len(output_specs) > 32 or set(metadata) - set(output_specs):
            raise ForgeError('Run requires 1..32 declared outputs with matching metadata')
        shared = None
        if dependencies is not None:
            from demiflow.workbench.python_sources import SharedSources
            shared = SharedSources(root, source_modules or {}, dependencies=dependencies).binding()
        report = ws._inspect_flow(root, entry, params=effective, save=True, table_resolver=_table_resolver(),
                                  shared_sources=shared, dependencies=dependencies)
        if report.get('status') != 'checked':
            raise ForgeError('Flow is not checked: ' + json.dumps(report.get('diagnostics', []), ensure_ascii=False)[:4096])
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
            observation_instances = execution_instances(report['nodes'], report['edges'], report['actions'],
                    maximum=OBSERVATION_COUNT, value_edges=report.get('value_edges', ()))
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
    from demiflow.workbench.runs.domain import MANAGEMENT_RESERVE, SEALED_BYTES
    from demiflow.workbench.runs import native_resources
    from demiflow.execution.host.control import training_deployment_reference
    training_deployment = None
    if any(node.get('kind') == 'fit' for node in report['nodes']):
        if not training_location:
            raise ForgeError('Training run requires its project deployment')
        training_deployment = training_deployment_reference(training_location, project_root=root)
    capacity_domain = native_resources.capacity_domain(capacity_domain)
    native_execution = native_resources.select(report, native_deployment, workspace=root,
                                                training_deployment=training_deployment)
    if native_execution and native_execution['resource_root'] != capacity_domain:
        raise ForgeError('Native deployment and capacity domain differ')
    from demiflow.workbench.runs.native_resources import environment_names as validate_environment_names
    credentials.update(validate_environment_names(environment_names or []))
    executable_dirs = native_resources.executable_directories(executable_dirs or [], workspace=root)
    platform_root = Path(__file__).resolve().parents[2]
    seal_root = Path(report['binding'].get('source_root', root))
    sealed = sealed_root(Path('.'), report['binding'])
    seal_files = [(ws.relative_path(seal_root,name), sealed/name, digest,None)
                  for name,digest in report['binding']['sources'].items()]
    seal_files.extend((ws.relative_path(seal_root,r['path']),sealed/r['path'],r['sha256'],r.get('max_bytes',1024**2))
                      for r in report['binding'].get('resources',{}).values() if r['kind'] in {'config','file'})
    from demiflow.forge.dependencies import module_files
    managed_modules = module_files(root, dependencies)
    seal_files.extend((ws.no_symlinks(origin), relative, digest, managed_modules.get(name, {}).get('bytes'))
                      for name, origin, relative, digest in bound_files(report['binding']))
    seal_files.extend((platform_root/item['path'],Path('runtime/demiflow')/item['path'],item['sha256'],platform['limits']['file_bytes'])
                      for item in platform['platform']['files'])
    # Python files are admitted by their actual storage footprint. Keep the
    # existing allowance for the platform snapshot and declared resources.
    python_bytes = other_bytes = 0
    measured_files = []
    for path, relative, digest, limit in seal_files:
        size = path.stat().st_size
        if limit is None:
            python_bytes += size
        else:
            other_bytes += size
        measured_files.append((path, relative, digest, size if limit is None else min(size, limit)))
    if len(seal_files) > 16384 or other_bytes > SEALED_BYTES:
        raise ForgeError('Runtime and resource seal exceeds its storage or file count budget', code='resource_limit')
    if shutil.disk_usage(root).free < (limits['max_scratch_bytes'] + limits['max_output_bytes']
            + limits['max_evidence_bytes'] + python_bytes + other_bytes + 64 * 1024**2):
        raise ForgeError('Insufficient workspace disk headroom')
    from demiflow.workbench.runs.storage import object_locations
    store = root / '.forge/runs'
    from demiflow.workbench.projects import index
    with ws.control_lock(root):
        operation_budget.check()
        from demiflow.workbench.projects.index import initialize_if_empty
        initialize_if_empty(root)
        store.mkdir(exist_ok=True)
        marker = index.begin(root, 'run', run_id)
        target = directory(root, run_id)
        created = False
        try:
            target.mkdir()
            created = True
            from demiflow.workbench.runs.domain import history_admission
            from demiflow.workbench.host_partition import policy
            from demiflow.workbench.host_partition import location
            history_admission(root, ceiling=policy(location())['history_bytes'],
                              reserve=MANAGEMENT_RESERVE + max(0, python_bytes + other_bytes - SEALED_BYTES))
            (target / 'work').mkdir()
            for source,relative,digest,maximum in measured_files:
                _copy_checked(source,target/relative,digest,maximum)
            declaration = platform_declaration(platform_root, platform['limits'])
            ws.write_control(target / 'inspection.json', report, create=True)
            ws.write_control(target / 'effective-params.json', effective, create=True)
            if change_scope is not None:
                ws.write_control(target / 'change-baseline.json', ws._scope_baseline(root, workspace), create=True)
            manifest = dict(format=FORMAT, run_id=run_id, root=str(root), entry=entry,
                runtime_contract='demiflow_forge_runtime_v1', native_submission=native_flow, prepare_request_id=prepare_request_id, telemetry_enabled=telemetry,
                change_scope=change_scope,
                journal_version=2,
                created_at=time.time(), report_digest=report['digest'], original_parameters=original,
                parameter_path=str(parameter_path.relative_to(root)) if parameter_path else None,
                outputs=output_specs, limits=limits, limits_explicit=explicit_limits, workspace=workspace, capacity_domain=capacity_domain, executable_dirs=executable_dirs,
                observation_instances=observation_instances,
                credential_names=sorted(credentials), evidence=ledgers, request_budgets=list(budgets.values()),
                object_locations=object_locations(report=report),
                platform=dict(digest=platform['digests']['snapshot'], origin=str(platform_root),
                    declaration=declaration, environment=platform['environment'], files=platform['platform']['files'], limits=platform['limits']))
            if dependencies is not None:
                manifest['project_dependencies'] = {
                    'digest': dependencies['digest'],
                    'environment': dependencies['environment'],
                    'requirements': dependencies['requirements'],
                    'modules': {name: {k: row[k] for k in ('sha256', 'bytes')} for name, row in managed_modules.items()},
                }
            if training_deployment:
                manifest['training_deployment'] = training_deployment
            if native_execution is not None:
                native_resources.seal(target, native_execution)
                manifest['native_execution'] = native_execution
            manifest['digest'] = content_digest(manifest)
            operation_budget.check()
            # Expensive freshness checks precede the authorization commit guard.
            if dependencies is not None:
                current_dependencies = project_dependencies(root, authority=authority.bootstrap())
                if current_dependencies is None or current_dependencies['digest'] != dependencies['digest']:
                    raise ForgeError('Project dependency approval changed during preparation', code='conflict')
            if native_flow:
                native_submission.verify_basis(root, manifest, report)
            else:
                verify_run_basis(root, manifest)
                native_submission.verify_shared_sources(report['binding'])
            durable_tree(target)
            with authority.guard(root, 'run_submit'):
                _verify_dependency_commit(root, manifest, authority)
                run = RunJournal(target)
                run.initialize(manifest, attempts=True)
                run = RunJournal(target, attempt=1)
                run.reserve_nodes([(item['action_id'], item['node_id'], item['occurrence']) for item in observation_instances])
                run.reserve_actions(_fact_actions(report,output_specs))
                run.transition({None}, 'prepared', prepared_at=time.time())
                run.record_usage(elapsed_ms=0, output_bytes=0, complete=True)
                durable_tree(target)
        except BaseException as exc:
            if created:
                shutil.rmtree(target)
            if isinstance(exc, Exception):
                publish_run_index(root, marker)
            raise
        publish_run_index(root, marker)
    return show(root, run_id, include_report=True)


def _load(root, run_id, *, detail='full', action_ids=None, attempt=None, deadline=None):
    target = directory(root, run_id)
    current = operation_budget.current_deadline()
    deadline = current if deadline is None else deadline if current is None else min(deadline, current)
    try:
        with read_run(target, deadline=deadline) as reader:
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
    if manifest.get('format') not in {FORMAT, PREVIOUS_FORMAT, LEGACY_EXECUTABLE_FORMAT} or report.get('format') not in {'demiflow_forge_flow_inspection_v3', 'demiflow_forge_native_inspection_v1'}:
        raise ForgeError('Historical execution records are read-only; prepare a newly inspected run')
    if manifest['format'] in {FORMAT, PREVIOUS_FORMAT}:
        if 'change_scope' not in manifest:
            raise ForgeError('Run has no change scope binding')
        scope = manifest['change_scope']
        audit.scope_digest(scope)
        if scope is not None:
            audit.verify_baseline(ws.read_control(target / 'change-baseline.json', audit.BASELINE_BYTES),
                                  scope=scope, root=manifest['root'], project_id=manifest['workspace']['id'])
    source = sealed_root(target, report['binding'])
    for name, digest in report['binding']['sources'].items():
        if source_digest(source / name) != digest:
            raise ForgeError('Sealed candidate source differs')
    managed_modules = manifest.get('project_dependencies', {}).get('modules', {})
    shared = list(bound_files(report['binding']))
    if set(managed_modules) - {name for name, _, _, _ in shared}:
        raise ForgeError('Sealed report omits project dependency modules', code='conflict')
    for name, _, relative, digest in shared:
        proof = managed_modules.get(name)
        if proof and proof['sha256'] != digest:
            raise ForgeError('Sealed project dependency manifest differs: ' + name, code='conflict')
        if source_digest(target / relative, proof['bytes'] if proof else None) != digest:
            raise ForgeError('Sealed shared Python source differs: ' + name, code='conflict')
    for resource in report['binding'].get('resources', {}).values():
        if resource['kind'] in {'config', 'file'} and hashlib.sha256(read_bounded(source / resource['path'], resource.get('max_bytes', 1024**2))).hexdigest() != resource['sha256']:
            raise ForgeError('Sealed resource differs')
    for item in manifest['platform']['files']:
        if hashlib.sha256(read_bounded(target / 'runtime/demiflow' / item['path'], manifest['platform']['limits']['file_bytes'])).hexdigest() != item['sha256']:
            raise ForgeError('Sealed platform source differs')
    if environment:
        p = manifest['platform']
        actual = environment_inventory(p['declaration'], p['limits'], extras=p['environment']['extras'])
        if {k: v for k, v in actual.items() if k != 'platform_distribution'} != {
                k: v for k, v in p['environment'].items() if k != 'platform_distribution'}:
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
        run = RunJournal(target, attempt=attempt)
        from demiflow.workbench.projects import index
        with ws.control_lock(root):
            operation_budget.check()
            with run_domain.preparation_lock() as preparation_lease:
                preflight = operation('run_preflight', root, run_id=run_id, expected=expected,_pass_fds=(preparation_lease,), _authority=authority.bootstrap())
            if preflight['status'] != 'checked':
                raise ForgeError('Run changed before admission', code='conflict')
            run_domain.history_admission(root, reserve=0, admitting=run_id, ceiling=capacity['history_bytes'])
            operation_budget.check()
            # The durable fence precedes reservation and run admission.
            # The earlier directory marker is only derived-cache intent.
            # A prepared retry may reuse an unlocked fence left before reservation.
            with authority.guard(root, 'run_submit'):
                if attempt == 1:
                    _verify_dependency_commit(root, value['manifest'], authority)
                marker = index.begin(root, 'run', run_id)
                interrupted = False
                try:
                    with _launch_unit(target, attempt) as (unit, unit_identity):
                        process = reservation = None
                        spawn_attempted = False
                        try:
                            operation_budget.check()
                            reservation = run_domain.reserve(domain, capacity, root, value['manifest'], attempt)
                            operation_budget.check()
                            _claim_lease(lease, run_id)
                            run.transition({'prepared'}, 'starting', started_at=time.time(), deployment=reservation,
                                           execution_stopped=False, unit_identity=unit_identity, launch_phase='admitted')
                            publish_run_index(root, marker)
                            marker = None
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
                                                     training_deployment=value['manifest'].get('training_deployment'),
                                                     capacity_domain=value['manifest'].get('capacity_domain'),
                                                     executable_dirs=value['manifest'].get('executable_dirs', []),
                                                     project_root=root)
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
                except BaseException as exc:
                    interrupted = not isinstance(exc, Exception)
                    raise
                finally:
                    if marker is not None and not interrupted:
                        publish_run_index(root, marker)
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
    identity = m.get('prepare_request_id')
    if identity and not value['state'].get('preparation_ready'):
        from demiflow.workbench.runs.requests import read
        receipt = read(root, identity)
        if receipt['status'] != 'ready' or receipt.get('digest') != m['digest']:
            raise ForgeError('Preparation receipt is not durably ready', code='conflict')
    phase = 'resume' if value['state'].get('attempt', 1) > 1 else 'preflight'
    _check_basis(root, target, m, phase=phase, authority=authority)
    validate_output_targets(value)
    remaining_limits(m['limits'], value['state'])
    return {'status': 'checked'}


def _verify_dependency_commit(root, manifest, authority):
    """Recheck the approval under the existing admission and registry locks."""
    expected = manifest.get('project_dependencies')
    from demiflow.workbench.projects.registry import project_dependencies
    current = project_dependencies(root, authority=authority.bootstrap(), verify_files=False)
    if (current['digest'] if current else None) != (expected['digest'] if expected else None):
        raise ForgeError('Project dependency approval changed during admission', code='conflict')


def _check_basis(root, target, m, *, phase='preflight', authority=None):
    verify_seal(target, m)
    dependencies = m.get('project_dependencies')
    historical_modules = ()
    if dependencies is not None:
        from demiflow.forge.dependencies import probe_environment
        if phase == 'resume':
            historical_modules = dependencies['modules']
        else:
            from demiflow.workbench.projects.registry import project_dependencies
            live = project_dependencies(root, authority=authority.bootstrap() if authority else None)
            if live is None or live['digest'] != dependencies['digest']:
                raise ForgeError('Project dependency approval changed before start', code='conflict')
        path = dependencies['environment']['interpreter']['path']
        config = dict(requirements=dependencies['requirements'], packages={
            name.partition('.')[0] for name in dependencies['modules']})
        if probe_environment(path, config, expected=dependencies['environment']['interpreter']) != dependencies['environment']:
            raise ForgeError('Sealed project Python environment changed', code='conflict')
    if m.get('native_submission'):
        from demiflow.workbench.runs.native_submission import verify_basis
        verify_basis(root, m, _report(target, m), sealed_packages=historical_modules)
    else:
        verify_run_basis(root, m)
        if dependencies is not None:
            from demiflow.workbench.runs.native_submission import verify_shared_sources
            verify_shared_sources(_report(target, m)['binding'], sealed_packages=historical_modules)


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
    # Original-contract admission with a concrete reason before any business start.
    # History is read-only: this never rewrites source nor creates a replacement
    # successful run; per-action compatibility is enforced by the recovery path.
    binding = RunJournal(directory(root, run_id)).recovery_identity()
    if binding is None:
        raise ForgeError('该运行没有统一恢复契约，不能恢复历史；请新建运行。', code='conflict')
    RunContext.validate_recovery_identity(binding)
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
        RunContext.validate_recovery_identity(RunJournal(target).recovery_identity())
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
        from demiflow.workbench.runs.storage import reconcile_training
        from demiflow.workbench.runs.storage import cleanup_deadline
        reconcile_training(RunJournal(target, attempt=expected_attempt), manifest, expected_attempt=expected_attempt,
            expected_revision=state['revision'], deadline=cleanup_deadline(), project_root=root,
            confirm_stopped=lambda snapshot: execution_stopped(target, snapshot))
        _check_basis(root, target, manifest, phase='resume', authority=authority)
        from demiflow.workbench.runs.storage import cleanup_deadline, clean_stopped
        deadline = cleanup_deadline()
        errors = register_results(root, run_id, recover=True, maintenance_deadline=deadline)
        if errors:
            raise ForgeError('Resume reconciliation failed: ' + '; '.join(errors))
        _, value = _load(root, run_id)
        clean_stopped(RunJournal(target, attempt=expected_attempt), preserve_jobs=True,
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
            interrupted = False
            try:
                report = _report(target, manifest)
                operation_budget.check(deadline)
                with authority.guard(root, 'run_submit'):
                    control.begin_attempt(expected_attempt=expected_attempt,
                        expected_revision=value['state']['revision'],
                        nodes=[(item['action_id'], item['node_id'], item['occurrence'])
                               for item in manifest['observation_instances']],
                        actions=_fact_actions(report, manifest['outputs']))
            except BaseException as exc:
                interrupted = not isinstance(exc, Exception)
                raise
            finally:
                if not interrupted:
                    publish_run_index(root, marker, deadline=deadline)
    return show(root, run_id)


def worker_environment(target, limits=None, credential_names=(), *, native_execution=None, capacity_domain=None, executable_dirs=(), training_deployment=None, project_root=None):
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
    from demiflow.workbench.runs.native_resources import environment, executable_directories
    if training_deployment:
        result.update(DEMIFLOW_TRAINING_DEPLOYMENT=training_deployment['path'],
                      DEMIFLOW_TRAINING_DEPLOYMENT_SHA256=training_deployment['sha256'])
    paths = executable_directories(list(executable_dirs))
    result['PATH'] = os.pathsep.join([os.path.dirname(sys.executable), *paths, '/usr/bin', '/bin'])
    result.update(environment(target, native_execution))
    if capacity_domain is not None:
        from demiflow.workbench.runs.native_resources import capacity_domain as validate_domain
        root = validate_domain(capacity_domain)
        if result.get('DEMIFLOW_RESOURCE_ROOT', root) != root:
            raise ForgeError('Native deployment and capacity domain differ')
        result['DEMIFLOW_RESOURCE_ROOT'] = root
    project_env: dict[str, str] = {}
    if credential_names:
        env_root = Path(project_root).absolute() if project_root is not None else None
        if env_root is None and target is not None:
            abs_target = Path(target).absolute()
            if len(abs_target.parents) >= 3 and abs_target.parent.name == 'runs' and abs_target.parents[1].name == '.forge':
                env_root = abs_target.parents[2]
        if env_root is not None:
            from demiflow.forge.project_env import load_project_env
            project_env = load_project_env(env_root)
    for name in credential_names:
        if not re.fullmatch(r'[A-Z_][A-Z0-9_]{0,127}', name) or name in result or name in {'HOME', 'LD_PRELOAD', 'LD_LIBRARY_PATH'} or name.startswith(('PYTHON', 'DEMIFLOW_', 'CUDA_')):
            raise ForgeError('Credential name conflicts with worker control environment')
        # Forge project-space .env takes precedence over host os.environ; only declared names are forwarded.
        value = project_env.get(name) or os.environ.get(name)
        if not value or len(value.encode()) > 16384:
            raise ForgeError(
                'Declared credential is unavailable or exceeds budget: ' + name
                + '（请在项目根目录 .env 中配置，变量清单见 .env.example）'
            )
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
    if value['manifest'].get('native_submission') or item.get('location_root'):
        base = ws.no_symlinks(Path(item['location_root']))
    else:
        if ref.store_id not in workspace['locations']:
            raise ForgeError('Result location is no longer declared')
        base = ws.location_root(root, workspace['locations'][ref.store_id])
    uri = ws.relative_path(base, ref.relative_uri)
    if str(uri) != item['uri']:
        raise ForgeError('Result location changed from the sealed output')
    alias = f'run:{run_id}:{name}' + (f':{attempt}' if attempt is not None else '')
    return {'format': 'demiflow_forge_table_v1', 'alias': alias,
            'location_id': ref.store_id, 'ref': ref.to_dict()}, ref, uri


def run_artifacts(root, run_id, *, attempt=None):
    """Collect verified committed output tables and published training models for a finished run."""
    target, value = _load(root, run_id, attempt=attempt)
    described = _describe(run_id, value)
    selected_attempt = described['attempt']
    outputs = {}
    for item in described['outputs']:
        if item.get('status') != 'committed' or item.get('result') is None:
            continue
        _, ref, uri = result_table(root, run_id, item['name'], attempt=selected_attempt)
        outputs[item['name']] = {
            'uri': str(uri),
            'version': ref.lance_version,
            'row_count': ref.row_count,
            'schema_hash': ref.schema_hash,
            'dataset_id': ref.dataset_id,
        }
    models_list = []
    with read_run(target, deadline=operation_budget.current_deadline()) as reader:
        models_list = reader.published_models()
    models = {}
    for idx, model in enumerate(models_list):
        models[str(idx)] = model
        if model.get('occurrence'):
            models[model['occurrence']] = model
        if model.get('action_id') and model['action_id'] not in models:
            models[model['action_id']] = model
    if len(models_list) == 1:
        models['default'] = models_list[0]
    return {
        'run_id': run_id,
        'attempt': selected_attempt,
        'status': described['status'],
        'entry': described['entry'],
        'outputs': outputs,
        'models': models,
        'models_list': models_list,
    }


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
            from demiflow.workbench.runs.storage import reconcile_training
            reconcile_training(run, value['manifest'], expected_attempt=expected_attempt, expected_revision=current['revision'],
                                   deadline=deadline, confirm_stopped=stopped, project_root=root)
            clean_stopped(run, preserve_jobs=current['status'] != 'cancelled', expected_revision=current['revision'],
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
    raw = read_bounded(ws.relative_path(sealed_root(target, report['binding']), path), 1024**2)
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ForgeError('Sealed source differs')
    return dict(path=path, text=raw.decode('utf-8'))


def retain_logs(target, attempt, budget):
    """Evict only stopped prior-attempt logs before reserving the next tail."""
    parent = attempt_directory(target, 'logs')
    parent.mkdir(parents=True, exist_ok=True)
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
    attempt_num = value['state'].get('attempt')
    parent = attempt_directory(target, 'logs', attempt_num)
    if not parent.exists():
        legacy = Path(target) / 'logs' / ('attempt-' + str(attempt_num)) if attempt_num is not None else Path(target) / 'logs'
        if legacy.exists():
            parent = legacy
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


def _table_resolver():
    from demiflow.workbench.table_bindings import make_resolver
    return make_resolver(ws.table_binding, result_table)


def verify_run_basis(root, manifest):
    from demiflow.forge import audit
    root, project, _ = ws.load_workspace(root)
    if manifest['format'] == 'demiflow_forge_run_manifest_v3' and project['format'] == ws.FORMAT_V3:
        raise ForgeError('Workspace audit contract changed; prepare a new run', code='conflict')
    audit.require_scope(ws._check_change_scope(root), expected_scope=manifest.get('change_scope'))
    report = ws._report_status(root, manifest['report_digest'], table_resolver=_table_resolver(), platform_check=False)
    if report['status'] != 'checked' or not report['verification']['complete']:
        raise ForgeError('Prepared basis is stale: ' + ', '.join(report.get('changes', [])), code='conflict')
    if manifest['parameter_path'] and ws.read_control(ws.relative_path(root, manifest['parameter_path'])) != manifest['original_parameters']:
        raise ForgeError('Prepared parameters changed', code='conflict')


def run_change_hints(root, index_info):
    """Opaque native storage hints, never execution or data authority."""
    from demiflow.execution.host.inspection import observe_storage
    root = Path(root).absolute()
    active = index_info.get('active_runs', [])
    if index_info.get('reason') == 'index_not_initialized':
        active = _run_ids(root, maximum=128)
    values = {}
    for run_id in active:
        target = directory(root, run_id)
        if not target.exists():
            values[run_id] = content_digest(None)
            continue
        deadline = operation_budget.current_deadline()
        operation_budget.check(deadline)
        with observe_storage(target, deadline=deadline) as observation:
            values[run_id] = observation.change_token
    return dict(runs=values, run_revision=content_digest(index_info))
