"""Bounded author-file inventories and change constraints; no workflow dependencies."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time

from .io import no_symlinks
from .platform import bounded_json, content_digest
from .schema import ForgeError

FORMAT = 'demiflow_forge_change_baseline_v1'
STATUS_FORMAT = 'demiflow_forge_audit_status_v1'
DEFAULT_LIMITS = dict(entries=4096, depth=16, file_bytes=32*1024**2, total_bytes=128*1024**2)
MAX_LIMITS = dict(entries=20000, depth=32, file_bytes=256*1024**2, total_bytes=512*1024**2)
BASELINE_BYTES = 8*1024**2
STATUS_BYTES = 1024**2
SKIP_DIRS = frozenset({'__pycache__', '.pytest_cache', '.ipynb_checkpoints', '.git'})


def digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ForgeError('Invalid audit digest', code='invalid_request')
    return value


def path_name(value):
    if type(value) is not str or len(value.encode('utf-8')) > 4096:
        raise ForgeError('Invalid audit path', code='invalid_request')
    path = PurePosixPath(value)
    if (not path.parts or path.is_absolute() or str(path) != value
            or any(p in {'.', '..', '.forge'} | SKIP_DIRS for p in path.parts)
            or '\\' in value):
        raise ForgeError('Audit path must be an author path within the workspace', code='invalid_request')
    return value


def within(path, root):
    return path == root or path.startswith(root + '/')


def limits_with(value=None):
    if value is not None and (type(value) is not dict or set(value) - set(DEFAULT_LIMITS)):
        raise ForgeError('Invalid audit limits', code='invalid_request')
    result = {**DEFAULT_LIMITS, **(value or {})}
    if any(type(v) is not int or not 1 <= v <= MAX_LIMITS[k] for k, v in result.items()):
        raise ForgeError('Audit limits exceed hard bounds', code='invalid_request')
    return result


def roots(value):
    if type(value) is not list or not 1 <= len(value) <= 64:
        raise ForgeError('Audit needs 1..64 author roots', code='invalid_request')
    names = sorted(path_name(p) for p in value)
    if 'workspace.json' in names:
        raise ForgeError('Workspace control is projected separately from author roots')
    if any(within(b, a) for i, a in enumerate(names) for b in names[i+1:]):
        raise ForgeError('Overlapping audit roots', code='invalid_request')
    return names


def writes(value, watch_roots=None):
    if type(value) is not list or len(value) > 256:
        raise ForgeError('Invalid audit writes', code='invalid_request')
    result = []
    for item in value:
        if (type(item) is not dict or set(item) != {'path', 'kind'}
                or type(item['kind']) is not str or item['kind'] not in {'file', 'tree'}):
            raise ForgeError('Write rules require path and file/tree kind', code='invalid_request')
        name = path_name(item['path'])
        if name == 'workspace.json' and item['kind'] != 'file':
            raise ForgeError('Workspace control is a file rule')
        if watch_roots is not None and name != 'workspace.json' and not any(within(name, p) for p in watch_roots):
            raise ForgeError('Write rule is outside the observed author roots')
        result.append(dict(path=name, kind=item['kind']))
    result.sort(key=lambda x: x['path'])
    if any(within(b['path'], a['path']) for i, a in enumerate(result) for b in result[i+1:]):
        raise ForgeError('Overlapping write rules', code='invalid_request')
    return result


def validate_policy(value):
    if type(value) is not dict or set(value) != {'reports', 'change_scope'}:
        raise ForgeError('Invalid workspace audit policy')
    reports = value['reports']
    if type(reports) is not list or len(reports) > 64:
        raise ForgeError('Audit supports at most 64 pinned reports')
    if len({digest(d) for d in reports}) != len(reports):
        raise ForgeError('Duplicate audit report')
    scope = value['change_scope']
    if scope is not None:
        if type(scope) is not dict or set(scope) != {'baseline', 'writes'}:
            raise ForgeError('Invalid change scope')
        digest(scope['baseline'])
        if scope['writes'] != writes(scope['writes']):
            raise ForgeError('Change scope writes must be canonical')
    return value


def scope_digest(scope):
    validate_policy(dict(reports=[], change_scope=scope))
    return content_digest(scope) if scope is not None else None


def workspace_digest(value):
    return content_digest({k: v for k, v in value.items() if k != 'audit'})


def _state(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def inventory(root, watch_roots, limits=None, *, metadata_only=False):
    root = no_symlinks(Path(root).absolute())
    watch_roots, limits = roots(watch_roots), limits_with(limits)
    entries, states = [], []
    total = encoded = 0
    deadline = time.monotonic() + 55

    def walk(name, depth, *, observed_root=False):
        nonlocal total, encoded
        if time.monotonic() > deadline or len(entries) >= limits['entries'] or depth > limits['depth']:
            raise ForgeError('Author inventory exceeds count/depth/time budget', code='resource_limit')
        path_name(name)
        path = no_symlinks(root / name)
        try:
            info = path.lstat()
        except FileNotFoundError:
            if not observed_root:
                raise ForgeError('Author tree changed during enumeration', code='conflict')
            entries.append(dict(path=name, kind='missing'))
            states.append((path, None))
            return
        state = _state(info)
        states.append((path, state))
        if stat.S_ISDIR(info.st_mode):
            item = dict(path=name, kind='directory')
        elif stat.S_ISREG(info.st_mode):
            total += info.st_size
            if info.st_size > limits['file_bytes'] or total > limits['total_bytes']:
                raise ForgeError('Author inventory byte budget exceeded', code='resource_limit')
            item = dict(path=name, kind='file', size=info.st_size)
            if not metadata_only:
                sha, consumed = hashlib.sha256(), 0
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'rb') as stream:
                    if _state(os.fstat(stream.fileno())) != state:
                        raise ForgeError('Author file changed before reading', code='conflict')
                    while True:
                        chunk = stream.read(min(1024**2, info.st_size - consumed + 1))
                        if not chunk:
                            break
                        consumed += len(chunk)
                        if consumed > info.st_size or time.monotonic() > deadline:
                            raise ForgeError('Author file changed or read budget exceeded', code='conflict')
                        sha.update(chunk)
                    if consumed != info.st_size or _state(os.fstat(stream.fileno())) != state:
                        raise ForgeError('Author file changed during reading', code='conflict')
                item['sha256'] = sha.hexdigest()
        else:
            raise ForgeError('Only regular author files/directories are supported')
        encoded += len(bounded_json(item, 16384)) + 1
        if encoded > BASELINE_BYTES - 65536:
            raise ForgeError('Author inventory control budget exceeded', code='resource_limit')
        entries.append(item)
        if item['kind'] == 'directory':
            with os.scandir(path) as children:
                for child in children:
                    if child.name in SKIP_DIRS and child.is_dir(follow_symlinks=False):
                        continue
                    walk(name + '/' + child.name, depth + 1)

    for name in watch_roots:
        walk(name, 0, observed_root=True)
    for path, expected in states:
        no_symlinks(path)
        try:
            actual = _state(path.lstat())
        except FileNotFoundError:
            actual = None
        if actual != expected:
            raise ForgeError('Author tree changed during verification', code='conflict')
    if metadata_only:
        return content_digest(sorted((str(p.relative_to(root)), s) for p, s in states))
    return sorted(entries, key=lambda item: item['path'])


def metadata_hint(root, watch_roots, limits=None):
    return inventory(root, watch_roots, limits, metadata_only=True)


def validate_baseline(value):
    fields = {'format', 'workspace_id', 'root', 'watch_roots', 'inventory_rules_version',
              'limits', 'entries', 'workspace_policy_sha256', 'created_at', 'digest'}
    if (type(value) is not dict or set(value) != fields or value['format'] != FORMAT
            or type(value['inventory_rules_version']) is not int or value['inventory_rules_version'] != 1):
        raise ForgeError('Unsupported change baseline')
    if type(value['workspace_id']) is not str or not re.fullmatch('w[0-9a-f]{32}', value['workspace_id']):
        raise ForgeError('Invalid baseline workspace identity')
    if type(value['root']) is not str or str(Path(value['root']).absolute()) != value['root']:
        raise ForgeError('Invalid baseline root')
    if type(value['created_at']) not in {int, float} or not 0 < value['created_at'] < 1e12:
        raise ForgeError('Invalid baseline creation time')
    observed, limits = roots(value['watch_roots']), limits_with(value['limits'])
    if observed != value['watch_roots'] or limits != value['limits']:
        raise ForgeError('Noncanonical baseline controls')
    entries = value['entries']
    if type(entries) is not list or len(entries) > limits['entries']:
        raise ForgeError('Invalid baseline inventory count')
    names, total, previous = {}, 0, ''
    for item in entries:
        if (type(item) is not dict or type(item.get('kind')) is not str
                or item['kind'] not in {'file', 'directory', 'missing'}):
            raise ForgeError('Invalid baseline entry')
        kind, name = item['kind'], path_name(item.get('path'))
        if set(item) != ({'path', 'kind', 'size', 'sha256'} if kind == 'file' else {'path', 'kind'}):
            raise ForgeError('Invalid baseline entry fields')
        parent = str(PurePosixPath(name).parent)
        if name <= previous or (name not in observed and names.get(parent) != 'directory'):
            raise ForgeError('Incomplete or unsorted baseline tree')
        owner = next((p for p in observed if within(name, p)), None)
        if owner is None or len(PurePosixPath(name).parts) - len(PurePosixPath(owner).parts) > limits['depth']:
            raise ForgeError('Baseline path is outside its declared roots/depth')
        if kind == 'missing' and name not in observed:
            raise ForgeError('Only a declared root may be missing')
        if kind == 'file':
            digest(item['sha256'])
            if type(item['size']) is not int or not 0 <= item['size'] <= limits['file_bytes']:
                raise ForgeError('Invalid baseline file size')
            total += item['size']
        names[name], previous = kind, name
    if not set(observed) <= names.keys() or total > limits['total_bytes']:
        raise ForgeError('Incomplete or oversized baseline')
    digest(value['workspace_policy_sha256'])
    if digest(value['digest']) != content_digest({k: v for k, v in value.items() if k != 'digest'}):
        raise ForgeError('Change baseline digest mismatch')
    bounded_json(value, BASELINE_BYTES)
    return value


def compare(baseline, current, allowed_writes, *, workspace_policy_sha256):
    validate_baseline(baseline)
    allowed_writes = writes(allowed_writes, baseline['watch_roots'])
    before = {v['path']: v for v in baseline['entries']}
    after = {v['path']: v for v in current}
    changes = []
    for name in sorted(before.keys() | after.keys()):
        a, b = before.get(name), after.get(name)
        if a == b:
            continue
        change = 'added' if a is None or a['kind'] == 'missing' else 'deleted' if b is None or b['kind'] == 'missing' else 'type_changed' if a['kind'] != b['kind'] else 'modified'
        kind = (b if change == 'added' else a)['kind']
        permitted = any((r['kind'] == 'tree' and within(name, r['path'])) or
                        (r['kind'] == 'file' and name == r['path'] and kind == 'file' and change != 'type_changed') for r in allowed_writes)
        changes.append(dict(path=name, change=change, kind=kind, allowed=permitted))
    # A newly created ancestor is allowed only when it actually contains allowed
    # changes, all descendants are allowed, and it is not a type replacement.
    descendant_state = {}
    for item in reversed(changes):
        if item['kind'] == 'directory' and item['change'] in {'added', 'deleted'} and not item['allowed']:
            item['allowed'] = descendant_state.get(item['path'], False)
        for parent in PurePosixPath(item['path']).parents:
            key = str(parent)
            descendant_state[key] = descendant_state.get(key, True) and item['allowed']
    if workspace_policy_sha256 != baseline['workspace_policy_sha256']:
        changes.append(dict(path='workspace.json', change='modified', kind='control',
                            allowed=any(r['path'] == 'workspace.json' for r in allowed_writes)))
    violations = sum(not c['allowed'] for c in changes)
    return dict(status='violated' if violations else 'within_scope', complete=True,
                change_count=len(changes), violation_count=violations,
                changes=sorted(changes, key=lambda c: (c['allowed'], c['path']))[:256],
                truncated=len(changes) > 256)


def require_scope(result, *, expected_scope):
    """Enforce a fresh caller-provided scope observation; no product imports."""
    if scope_digest(result['scope']) != scope_digest(expected_scope):
        raise ForgeError('Prepared change scope differs; prepare again', code='conflict')
    if result['status'] not in {'within_scope', 'unconfigured'}:
        paths = ', '.join(c['path'] for c in result['changes'] if not c['allowed'])
        raise ForgeError('Changes outside the allowed scope: ' + paths[:2048], code='operation_rejected')
    return result


def verify_baseline(value, *, scope, root, project_id):
    """Validate original engineering evidence for either application."""
    baseline = validate_baseline(value)
    if (baseline['digest'] != scope['baseline'] or baseline['root'] != str(root)
            or baseline['workspace_id'] != project_id):
        raise ForgeError('Change baseline identity differs', code='conflict')
    return baseline
