"""Controller-owned development gates; no harness loop or isolation backend.

Public methods accept trusted controller configuration. Neither task placement,
Review authority nor acceptance is a tool exposed to the development harness.
The controller is trusted. Enforced assurance requires its verified external
execution receipts; this module itself is not an OS security boundary.
Existing local records retain their original interpretation.
"""
from __future__ import annotations

from dataclasses import dataclass
import difflib
import fcntl
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import stat
import time

from . import tasks
from .project import ForgeError, Project, REVIEW_RULES, changes, digest, file_hash, file_identity, no_symlinks, safe_id, snapshot_limits, within
from .taskstore import TaskStore
from ..io import write_exclusive, fsync_directory, read_json as bounded_read_json

SCHEMA = 'demiflow-managed-development-v1'
MAX_CONTRACT = 65536
REVIEW_BYTES = 512 * 1024
SOURCE_BYTES = 128 * 1024
# Unchanged readonly data may be opaque; code, configuration and authority
# remain complete text. Explicit context sources are protected regardless of
# extension. These suffixes only tighten that rule, never grant write scope.
REVIEW_TEXT_SUFFIXES = frozenset({'.py', '.pyi', '.ipynb', '.js', '.cjs', '.mjs', '.ts', '.tsx', '.jsx',
    '.sh', '.bash', '.zsh', '.fish', '.ps1', '.rb', '.rs', '.go', '.c', '.h', '.cc', '.cpp', '.hpp',
    '.java', '.kt', '.scala', '.sql', '.html', '.css', '.xml', '.toml', '.ini', '.cfg', '.json',
    '.json5', '.yaml', '.yml', '.md', '.rst', '.txt'})


def _text_source(project, name, expected):
    from .coverage import bounded_bytes
    path = no_symlinks(project.root / name)
    raw, _ = bounded_bytes(path, SOURCE_BYTES)
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ForgeError('Review source changed while reading: ' + name)
    try:
        return raw.decode('utf-8')
    except UnicodeError as error:
        raise ForgeError('Review source is not bounded UTF-8 text: ' + name) from error


def _readonly_review_artifact(project, name, sha, observation, deadline):
    """Metadata for unchanged data, never a claim of semantic data inspection."""
    path = no_symlinks(project.root / name)
    info = path.stat()
    identity = (observation.identities or {}).get(path)
    if (not stat.S_ISREG(info.st_mode) or identity is not None and file_identity(info) != identity):
        raise ForgeError('Readonly Review artifact changed while collecting metadata: ' + name)
    # Older standards have no observation identity map. Preserve their hash
    # meaning with the same bounded streaming source hash before using metadata.
    if identity is None and file_hash(path, limit=snapshot_limits(project.config.get('snapshot_limits'))['file_bytes'],
                                      deadline=deadline) != sha:
        raise ForgeError('Readonly Review artifact changed while collecting metadata: ' + name)
    return dict(sha256=sha, bytes=info.st_size, kind='readonly_artifact',
                content='not_included', extension=path.suffix.lower(),
                basis='unchanged_readonly_baseline',
                limitation='Content is opaque to the reviewer; hash and size are evidence, not semantic data inspection')


def _feedback(state, directory):
    """Deterministic, bounded facts from real gates, for the same harness to fix."""
    result = [dict(rule_id=text.split(' ', 1)[0] if text.startswith('DF-') else 'DF-07',
                   path=None, reason=text[:2048], evidence=[], allowed_next_action='fix_within_scope')
              for text in (state.get('check') or {}).get('violations', [])[:24]]
    for command in (state.get('verification') or {}).get('commands', []):
        if command.get('passed') is True:
            continue
        tail = ''
        if command.get('log_base') == 'task':
            label = command.get('log', '')
            if label and not Path(label).is_absolute() and '..' not in Path(label).parts:
                path = no_symlinks(directory / label)
                if path.is_file() and path.stat().st_size <= 4 * 1024 * 1024:
                    with path.open('rb') as stream:
                        stream.seek(max(0, path.stat().st_size - 3072))
                        tail = stream.read(3072).decode('utf-8', errors='replace')
        result.append(dict(rule_id='DF-07', path=None,
            reason=f"Verifier {command.get('id')}: failure={command.get('failure')}; returncode={command.get('returncode')}\n{tail}",
            evidence=[command['log']] if command.get('log') else [],
            allowed_next_action=('fix_within_scope' if command.get('failure') is None and
                type(command.get('returncode')) is int and command['returncode'] > 0 else 'inspect_uncertain_execution')))
    for rule, claim in (state.get('review') or {}).get('claims', {}).items():
        if claim.get('status') == 'fail':
            result.append(dict(rule_id=rule, path=(claim.get('evidence') or [None])[0],
                               reason=claim['reason'][:2048], evidence=claim['evidence'][:8],
                               allowed_next_action='fix_within_scope'))
    bounded = []
    for issue in result:
        if len(json.dumps([*bounded, issue], ensure_ascii=False).encode()) > 12 * 1024:
            break
        bounded.append(issue)
    return bounded


def _json_copy(value, maximum=MAX_CONTRACT):
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise ForgeError('Managed contract must contain finite JSON values') from error
    if len(raw.encode()) > maximum:
        raise ForgeError('Managed JSON byte budget exceeded')
    return json.loads(raw)


def _bounded(values, *, count=256, byte_limit=32768):
    result, used = [], 0
    for value in values:
        if len(result) >= count or used + len(str(value).encode()) > byte_limit:
            break
        result.append(value)
        used += len(str(value).encode())
    return result


def _index_snapshot(root):
    """Hash the index read-only. Conservative stat-cache changes also invalidate.

    Only path-oriented rev-parse options are used. --shared-index-path is NOT
    read-only: Git refreshes the shared index timestamp even without optional
    locks. Instead all shared-index candidates beside the index are bounded and
    hashed directly, conservatively including unused historical candidates.
    """
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C',
           'GIT_OPTIONAL_LOCKS': '0', 'GIT_CONFIG_NOSYSTEM': '1',
           'GIT_CONFIG_GLOBAL': '/dev/null'}
    try:
        process = subprocess.run(['git', '--no-optional-locks', '-C', str(root),
                                  'rev-parse', '--absolute-git-dir', '--git-path', 'index'],
                                 capture_output=True, env=env, timeout=5)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ForgeError(f'Cannot observe Git staging identity: {error}') from error
    if process.returncode:
        # A missing repository is distinct from an unreadable/broken repository.
        if b'not a git repository' in process.stderr:
            return {'repository': False}
        raise ForgeError('Cannot observe Git staging identity')
    if len(process.stdout) > 16384:
        raise ForgeError('Git identity path budget exceeded')
    lines = process.stdout.decode('utf-8').splitlines()
    if len(lines) != 2:
        raise ForgeError('Unexpected Git staging identity')
    git_dir = no_symlinks(Path(lines[0]).absolute())
    index = no_symlinks(root / lines[1])
    deadline = time.monotonic() + 5
    index_hash = file_hash(index, limit=16 * 1024 * 1024, deadline=deadline) if index.exists() else None
    shared = {}
    total = 0
    with os.scandir(index.parent) as entries:
        for number, entry in enumerate(entries, 1):
            if number > 4096 or time.monotonic() > deadline:
                raise ForgeError('Git index directory enumeration budget exceeded')
            if not entry.name.startswith('sharedindex.'):
                continue
            path = no_symlinks(Path(entry.path))
            total += path.stat().st_size
            if len(shared) >= 128 or total > 64 * 1024 * 1024:
                raise ForgeError('Git shared-index snapshot budget exceeded')
            shared[entry.name] = file_hash(path, limit=16 * 1024 * 1024, deadline=deadline)
    if (file_hash(index, limit=16 * 1024 * 1024, deadline=deadline) if index.exists() else None) != index_hash:
        raise ForgeError('Git index changed during staging observation')
    return {'repository': True, 'git_dir': str(git_dir), 'index': str(index),
            'sha256': index_hash, 'shared_indexes': dict(sorted(shared.items()))}


def _project_identity(root):
    no_symlinks(root)
    info = root.stat()
    return [info.st_dev, info.st_ino]


@dataclass(frozen=True)
class ReviewReceipt:
    """Opaque proof of a trusted adapter result, never parsed from harness JSON."""
    payload: str
    signature: str


class ReviewAuthority:
    """Only a controller's verified independent read-only adapter may own this.

    Construction does not certify a provider. The integration must establish a
    no-tools/read-only protocol before issuing a receipt; absent that adapter,
    leave ManagedTasks.review_authority unset. The key is never serialized.
    """
    def __init__(self, *, _key=None, available=True, execution_identity=None):
        self._key = _key or secrets.token_bytes(32)
        self.available = available
        self.execution_identity = execution_identity

    @classmethod
    def persistent(cls, state_root, *, execution_identity=None):
        """One private deployment key, atomically created under an exclusive lock."""
        root = no_symlinks(Path(state_root).absolute() / 'forge-development')
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.stat()
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ForgeError('Review authority requires a controller-owned 0700 control directory')
        lockpath = no_symlinks(root / '.review-authority.lock')
        fd = os.open(lockpath, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = no_symlinks(root / '.review-authority.key')
            if not path.exists():
                temporary = root / ('.review-key-' + secrets.token_hex(8))
                try:
                    write_exclusive(temporary, secrets.token_hex(32))
                    os.link(temporary, path)
                    fsync_directory(root)
                finally:
                    temporary.unlink(missing_ok=True)
            info = path.stat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or
                    stat.S_IMODE(info.st_mode) != 0o600 or info.st_size != 64):
                raise ForgeError('Review authority key must be a private controller-owned 0600 file')
            try:
                key = bytes.fromhex(path.read_text(encoding='ascii'))
            except (ValueError, UnicodeError) as error:
                raise ForgeError('Invalid persistent review authority') from error
            if len(key) != 32:
                raise ForgeError('Invalid persistent review authority')
        finally:
            os.close(fd)
        return cls(_key=key, available=False, execution_identity=execution_identity)

    def issue(self, task_id, fingerprint, claims, *, provider, model, session_id, mode, input_digest=None):
        safe_id(task_id)
        if mode not in {'no-tools', 'read-only-protocol'}:
            raise ForgeError('Reviewer requires a verified read-only protocol or no tools')
        for value in (fingerprint, provider, model, session_id):
            if not isinstance(value, str) or not value.strip() or len(value) > 256:
                raise ForgeError('Reviewer receipt requires bounded provider/model/session identity')
        value = dict(domain='independent-review-v1', task_id=task_id, fingerprint=fingerprint,
                                  claims_digest=digest(_json_copy(claims)), provider=provider,
                                  model=model, session_id=session_id, mode=mode)
        if input_digest is not None:
            if not isinstance(input_digest, str) or len(input_digest) != 64:
                raise ForgeError('Review input must be bound by digest')
            value['input_digest'] = input_digest
        payload = json.dumps(value, sort_keys=True)
        return ReviewReceipt(payload, hmac.new(self._key, payload.encode(), hashlib.sha256).hexdigest())

    def issue_execution(self, task_id, fingerprint, *, phase, backend, implementation, execution_id):
        """Controller-only; caller must have observed actual confined completion."""
        safe_id(task_id)
        if phase not in {'developer', 'verifier'}:
            raise ForgeError('Unknown execution evidence phase')
        for value in (fingerprint, backend, implementation, execution_id):
            if not isinstance(value, str) or not value or len(value) > 256:
                raise ForgeError('Execution receipt requires bounded actual backend identity')
        if self.execution_identity is not None and self.execution_identity != dict(backend=backend, implementation=implementation):
            raise ForgeError('Execution backend differs from the current trusted implementation')
        payload = json.dumps(dict(domain='managed-execution-v1', task_id=task_id, fingerprint=fingerprint,
            phase=phase, backend=backend, implementation=implementation, execution_id=execution_id), sort_keys=True)
        return ReviewReceipt(payload, hmac.new(self._key, payload.encode(), hashlib.sha256).hexdigest())

    def validate(self, receipt):
        if (type(receipt) is not ReviewReceipt or not isinstance(receipt.payload, str) or
                not isinstance(receipt.signature, str) or len(receipt.payload) > 4096):
            raise ForgeError('Review requires an opaque trusted controller receipt')
        signature = hmac.new(self._key, receipt.payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, receipt.signature):
            raise ForgeError('Reviewer receipt authority does not match')
        return json.loads(receipt.payload)


class ManagedTasks:
    def __init__(self, state_root, project_id, project_root, *, review_authority=None):
        self.project_id = safe_id(project_id)
        self.project_root = no_symlinks(Path(project_root).absolute()).resolve()
        self.store = TaskStore.external(state_root, project_id, self.project_root)
        if review_authority is not None and type(review_authority) is not ReviewAuthority:
            raise ForgeError('Invalid controller Review authority')
        self.review_authority = review_authority

    def _project(self):
        return Project(self.project_root, task_store=self.store)

    def _validate(self, state, task_id):
        metadata = state.get('managed', {})
        bound = metadata.get('bound_contract', {})
        if (state.get('id') != task_id or metadata.get('schema') != SCHEMA or
                metadata.get('project_id') != self.project_id or
                metadata.get('project_root') != str(self.project_root) or
                metadata.get('contract_digest') != digest(bound) or
                bound.get('project_id') != self.project_id or bound.get('task_id') != task_id or
                bound.get('project_root') != str(self.project_root) or
                metadata.get('request') != bound.get('request') or
                any(state.get(key) != bound.get('request', {}).get(key)
                    for key in ('pipeline', 'goal', 'writes', 'non_goals')) or
                bound.get('baseline_fingerprint') != digest({key: state.get(key) for key in
                    ('baseline', 'readonly_baseline', 'baseline_coverage')})):
            raise ForgeError('Managed task identity/contract mismatch; local records are not managed evidence')
        return metadata

    def prepare_task(self, task_id, pipeline, goal, writes, non_goals, *, contract=None):
        safe_id(task_id)
        contract = _json_copy({} if contract is None else contract)
        if type(contract) is not dict:
            raise ForgeError('Managed contract must be an object')
        if type(contract.get('allow_tests', False)) is not bool:
            raise ForgeError('allow_tests must be a controller authorization boolean')
        if type(contract.get('require_context_binding', False)) is not bool:
            raise ForgeError('require_context_binding must be a controller policy boolean')
        if type(contract.get('require_enforcement', False)) is not bool:
            raise ForgeError('require_enforcement must be a controller policy boolean')
        maximum = contract.get('max_verification_seconds', 120)
        if type(maximum) is not int or not 1 <= maximum <= 600:
            raise ForgeError('Managed verification budget must be 1..600 seconds')
        contract.setdefault('allow_tests', False)
        contract.setdefault('require_context_binding', False)
        contract.setdefault('max_verification_seconds', maximum)
        request = _json_copy(dict(pipeline=pipeline, goal=goal, writes=sorted(set(writes)),
                                  non_goals=non_goals, contract=contract))
        project = self._project()
        if self.store.directory(task_id).exists():
            with tasks.locked(project, task_id, write=False) as (directory, state):
                metadata = self._validate(state, task_id)
                if metadata['request'] != request:
                    raise ForgeError('Managed task already exists with a different goal/scope/contract')
                return self._projection(project, state, directory)
        bound = dict(project_id=self.project_id, project_root=str(self.project_root),
                     project_identity=_project_identity(self.project_root),
                     task_id=task_id, request=request, staging=_index_snapshot(self.project_root),
                     assurance='gate_only', run_policy='observe')
        metadata = dict(schema=SCHEMA, project_id=self.project_id,
                        project_root=str(self.project_root), request=request,
                        bound_contract=bound, contract_digest=digest(bound), acceptance=None)
        try:
            tasks.start(project, task_id, pipeline, goal, writes, non_goals,
                        metadata={'managed': metadata})
            with tasks.locked(project, task_id) as (directory, state):
                self._capture_review_baseline(project, state, directory)
        except FileExistsError:
            # Another controller may have won creation; never replace its baseline.
            with tasks.locked(project, task_id, write=False) as (_, state):
                if self._validate(state, task_id)['request'] != request:
                    raise ForgeError('Managed task creation conflicted with another contract')
        return self.status(task_id)

    def _capture_review_baseline(self, project, state, directory):
        """Save bounded original text once. A restart never recaptures a baseline."""
        files, missing, used = {}, [], 0
        deadline = time.monotonic() + 5
        names = [name for name, sha in state['baseline'].items()
                 if isinstance(sha, str) and len(sha) == 64 and
                 any(within(name, scope) for scope in state['writes'])]
        if len(names) > 128:
            missing = ['Original writable sources exceed the 128-file Review baseline budget']
        else:
            for name in names:
                try:
                    if time.monotonic() > deadline:
                        raise ForgeError('Original Review baseline time budget exceeded')
                    text = _text_source(project, name, state['baseline'][name])
                    used += len(text.encode())
                    if used > 256 * 1024:
                        raise ForgeError('Original Review baseline byte budget exceeded')
                    files[name] = dict(sha256=state['baseline'][name], text=text)
                except (ForgeError, OSError) as error:
                    missing.append(str(error)[:1024])
        value = dict(schema='demiflow-review-baseline-v1', files=files, missing=missing)
        tasks.atomic_write_json(directory / 'review-baseline.json', value)
        bound = state['managed']['bound_contract']
        bound['review_baseline'] = dict(artifact='review-baseline.json', digest=digest(value))
        state['managed']['contract_digest'] = digest(bound)

    def review_material(self, task_id, expected_fingerprint):
        """A complete bounded candidate bundle, never chat conclusions or live tools."""
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            metadata = self._validate(state, task_id)
            tree, readonly, _, fingerprint, observation = tasks.current(project, state, with_observation=True)
            current = self._projection(project, state, directory)
            if (fingerprint != expected_fingerprint or current['gates']['check'] != 'passed' or
                    current['gates']['verify'] != 'passed'):
                raise ForgeError('Review requires current passing mechanical and fixture evidence')
            baseline_record = metadata['bound_contract'].get('review_baseline')
            baseline = {'files': {}}
            if baseline_record:
                baseline = bounded_read_json(no_symlinks(directory / baseline_record['artifact']), maximum=REVIEW_BYTES*4)
                if digest(baseline) != baseline_record['digest']:
                    raise ForgeError('Original Review baseline evidence changed')
            changed = changes(state['baseline'], tree)
            known = {name: sha for name, sha in {**tree, **readonly}.items()
                     if isinstance(sha, str) and len(sha) == 64}
            if len(known) > 128:
                raise ForgeError('Complete Review input exceeds the 128-source budget')
            authority_paths = {str((project.root / definition['path']).resolve())
                               for definition in project.source_defs(state['pipeline']).values()}
            authority_paths.update(source['canonical_path'] for source in metadata['bound_contract'].get('context_baseline', []))
            sources, used = {}, 0
            deadline = time.monotonic() + 10
            for name, sha in sorted(known.items()):
                if time.monotonic() > deadline:
                    raise ForgeError('Complete Review input time budget exceeded')
                path = no_symlinks(project.root / name)
                opaque_allowed = (name not in tree and state['readonly_baseline'].get(name) == sha and
                                  str(path.resolve()) not in authority_paths and path.suffix.lower() not in REVIEW_TEXT_SUFFIXES)
                if opaque_allowed:
                    # Probe only a bounded small text representation. Large or
                    # binary immutable data stays an explicitly opaque fact.
                    if path.stat().st_size > SOURCE_BYTES:
                        sources[name] = _readonly_review_artifact(project, name, sha, observation, deadline)
                        continue
                    from .coverage import bounded_bytes
                    raw, _ = bounded_bytes(path, SOURCE_BYTES, deadline=deadline)
                    if hashlib.sha256(raw).hexdigest() != sha:
                        raise ForgeError('Review source changed while reading: ' + name)
                    try:
                        text = raw.decode('utf-8')
                        if any(ord(char) < 32 and char not in '\t\r\n' for char in text):
                            raise UnicodeError('Binary control bytes')
                    except UnicodeError:
                        sources[name] = _readonly_review_artifact(project, name, sha, observation, deadline)
                        continue
                else:
                    text = _text_source(project, name, sha)
                used += len(text.encode())
                if used > 384 * 1024:
                    raise ForgeError('Complete Review input exceeds the source byte budget')
                sources[name] = dict(sha256=sha, text=text)
            differences = []
            for name in changed:
                if name.endswith('/'):
                    continue
                old_sha, new_sha = state['baseline'].get(name), tree.get(name)
                old = baseline['files'].get(name)
                if old_sha is not None and old is None:
                    raise ForgeError('Original source text is unavailable for exact Review diff: ' + name)
                before = (old or {}).get('text', '')
                after = sources.get(name, {}).get('text', '')
                if new_sha is not None and 'text' not in sources.get(name, {}):
                    raise ForgeError('Changed source is unavailable to the reviewer: ' + name)
                diff = ''.join(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True),
                                                    fromfile='before/' + name, tofile='candidate/' + name))
                differences.append(dict(path=name, before=old_sha, after=new_sha, diff=diff))
            rules, rule_bytes = {}, 0
            for key, definition in project.source_defs(state['pipeline']).items():
                if time.monotonic() > deadline:
                    raise ForgeError('Complete Review authority time budget exceeded')
                text = project.source(definition)
                rule_bytes += len(text.encode())
                if rule_bytes > 256 * 1024:
                    raise ForgeError('Complete Review authority byte budget exceeded')
                rules[key] = text
            for source in metadata['bound_contract'].get('context_baseline', []):
                text = _text_source(project, source['canonical_path'], source['sha256'])
                rule_bytes += len(text.encode())
                if rule_bytes > 256 * 1024:
                    raise ForgeError('Complete Review authority byte budget exceeded')
                rules[source['canonical_path']] = text
            verification = dict(fingerprint=state['verification']['fingerprint'], passed=True,
                                verification_source='live_tree_rechecked', sandbox=state['verification'].get('sandbox', False),
                                commands=[{key: command.get(key) for key in ('id', 'argv', 'returncode', 'failure', 'passed', 'log', 'log_sha256')}
                                          for command in state['verification']['commands']])
            for command in verification['commands']:
                logfile = no_symlinks(directory / command['log'])
                size = logfile.stat().st_size
                with logfile.open('rb') as stream:
                    stream.seek(max(0, size - 8192))
                    command['log_tail'] = stream.read(8192).decode('utf-8', errors='replace')
                command['log_bytes'] = size
                command['log_truncated'] = size > 8192
            value = dict(schema='demiflow-independent-review-input-v1', task_id=task_id,
                fingerprint=fingerprint, project_id=self.project_id, project_root=str(self.project_root),
                original_goal=state['goal'], non_goals=state['non_goals'], writes=state['writes'],
                decisions=[n for n in state['notes'] if n['kind'] == 'decision' or n.get('resolved')],
                applicable_sources=rules, review_rules=list(REVIEW_RULES), sources=sources,
                differences=differences, check=state['check'], verification=verification,
                evidence_paths=sorted(known), assurance='gate_only',
                boundary='Immutable source text and explicitly opaque unchanged readonly artifact facts, rechecked before and after collection; no source execution snapshot or semantic inspection of opaque data claimed')
            value = _json_copy(value, REVIEW_BYTES)
            if tasks.current(project, state)[3] != fingerprint:
                raise ForgeError('Candidate changed while constructing Review input')
            input_digest = digest(value)
            path = no_symlinks(directory / 'review-inputs' / (input_digest + '.json'))
            path.parent.mkdir(mode=0o700, exist_ok=True)
            existing = metadata.setdefault('review_inputs', {})
            # The controller permits two requests per user turn. A task lives
            # across turns; retain bounded historical bundles without resetting
            # its original baseline or imposing a two-request lifetime limit.
            if input_digest not in existing and len(existing) >= 256:
                raise ForgeError('Independent Review input history budget exhausted')
            tasks.atomic_write_json(path, value)
            existing[input_digest] = str(path.relative_to(directory))
            return dict(input=value, input_digest=input_digest, source_fingerprint=fingerprint)

    def record_review_unavailable(self, task_id, expected_fingerprint, reason, *, unknown=False):
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            self._validate(state, task_id)
            if tasks.current(project, state)[3] != expected_fingerprint:
                return self._projection(project, state, directory, stale=True)
            state['review'] = dict(fingerprint=expected_fingerprint, time=tasks.now(), passed=False,
                type='controller-review-outcome', status='unknown' if unknown else 'unavailable', reason=str(reason)[:2048])
            return self._projection(project, state, directory)

    def bind_context(self, task_id, sources):
        """Bind actually delivered full-file authority; callable only by controller.

        Registered full-file snapshots already own source changes and permissions.
        Only additional files form an immutable supplementary norm baseline. This
        operation never changes the original task source baseline or read receipts.
        """
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            metadata = self._validate(state, task_id)
            *_, observation = tasks.current(project, state, with_observation=True)
            supplied = tasks.supplemental_context(sources)
            deadline = time.monotonic() + 5
            # Snapshot keys can include external readonly paths; normalize those
            # lexically, since the observation has already rejected symlink files.
            complete = {}
            for name, sha in {**observation.tree, **observation.readonly}.items():
                if time.monotonic() > deadline:
                    raise ForgeError('Supplementary context coverage scan exceeded five seconds')
                if isinstance(sha, str) and len(sha) == 64:
                    complete[os.path.normpath(str(project.root / name))] = sha
            supplemental = []
            for source in supplied:
                label, sha = source['canonical_path'], source['sha256']
                covered = complete.get(label) == sha
                # A hard-link alias is the same physical source, and changes to it
                # are already visible in the registered full snapshot.
                info = Path(label).stat()
                if not covered and info.st_nlink > 1:
                    for known, known_sha in complete.items():
                        if time.monotonic() > deadline:
                            raise ForgeError('Supplementary context coverage scan exceeded five seconds')
                        if known_sha == sha:
                            other = no_symlinks(Path(known)).stat()
                            if (other.st_dev, other.st_ino) == (info.st_dev, info.st_ino):
                                covered = True
                                break
                if not covered:
                    supplemental.append(source)
            observation.stable()
            bound = metadata['bound_contract']
            if 'context_baseline' in bound and bound['context_baseline'] != supplemental:
                metadata['context_binding_problem'] = 'supplementary_source_set_changed'
                # Preserve this refusal even though the normal mutation scope is
                # rolled back by the exception. Old acceptance stays historical.
                tasks.atomic_write_json(directory / 'task.json', state)
                raise ForgeError('Supplementary authority source set cannot change without an explicit contract update')
            if 'context_baseline' not in bound:
                bound['context_baseline'] = supplemental
                metadata['contract_digest'] = digest(bound)
            return self._projection(project, state, directory)

    def context(self, task_id, source_id=None, page=1):
        project = self._project()
        with tasks.locked(project, task_id) as (_, state):
            self._validate(state, task_id)
            return tasks.context(project, state, source_id, page)

    def refresh_context(self, task_id, reason):
        project = self._project()
        with tasks.locked(project, task_id) as (_, state):
            self._validate(state, task_id)
            return tasks.refresh(project, state, reason)

    def note(self, task_id, kind, text, *, event_id=None):
        if kind not in {'decision', 'progress', 'question', 'unverified', 'next'}:
            raise ForgeError('Unknown managed note kind')
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 16384:
            raise ForgeError('Managed note requires 1..16384 bytes')
        if kind == 'decision' and event_id is None:
            raise ForgeError('A user decision requires its stable controller event ID')
        if event_id is not None and (not isinstance(event_id, str) or not event_id or len(event_id) > 256):
            raise ForgeError('Invalid controller event ID')
        project = self._project()
        with tasks.locked(project, task_id) as (_, state):
            self._validate(state, task_id)
            tasks.current(project, state)
            if event_id is not None:
                for index, note in enumerate(state['notes']):
                    if note.get('event_id') == event_id:
                        if note['kind'] != kind or note['text'] != text:
                            raise ForgeError('Controller event ID was reused with different content')
                        return dict(note_index=index, duplicate=True)
            if len(state['notes']) >= 256:
                raise ForgeError('Managed task note budget exhausted')
            note = dict(kind=kind, text=text, time=tasks.now())
            if event_id is not None:
                note['event_id'] = event_id
            state['notes'].append(note)
            return dict(note_index=len(state['notes']) - 1, duplicate=False)

    def _expected(self, project, state, expected):
        return expected is None or tasks.current(project, state)[3] == expected

    def check(self, task_id, *, expected_fingerprint=None):
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            self._validate(state, task_id)
            if not self._expected(project, state, expected_fingerprint):
                return self._projection(project, state, directory, stale=True)
            tasks.check(project, state)
            return self._projection(project, state, directory)

    def verify(self, task_id, *, timeout_seconds=None, expected_fingerprint=None, launch=None):
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            metadata = self._validate(state, task_id)
            contract = metadata['request']['contract']
            if not contract['allow_tests']:
                raise ForgeError('Managed fixture verification has not been authorized')
            if contract.get('require_enforcement') and (launch is None or self.review_authority is None):
                raise ForgeError('Enforced task verification requires the trusted external execution boundary')
            budget = contract['max_verification_seconds']
            if timeout_seconds is not None:
                if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= budget:
                    raise ForgeError('Verification may only tighten its fixed total timeout')
                budget = timeout_seconds
            if not self._expected(project, state, expected_fingerprint):
                return self._projection(project, state, directory, stale=True)
            if (_project_identity(self.project_root) != metadata['bound_contract']['project_identity'] or
                    _index_snapshot(self.project_root) != metadata['bound_contract']['staging']):
                return self._projection(project, state, directory)
            result = tasks.check(project, state)
            if result['passed']:
                tasks.verify(project, state, directory, True, timeout_seconds=budget, launch=launch)
                state['verification']['verification_source'] = 'live_tree_rechecked'
                verification = state['verification']
                executions = [command.get('execution', {}) for command in verification['commands']]
                if (verification['passed'] and executions and self.review_authority and
                    all(e.get('assurance') == 'enforced' and all(isinstance(e.get(k), str) and e[k] for k in
                        ('backend', 'implementation', 'id')) for e in executions) and
                    len({(e['backend'], e['implementation']) for e in executions}) == 1):
                    receipt = self.review_authority.issue_execution(task_id, verification['fingerprint'], phase='verifier',
                        backend=executions[0]['backend'], implementation=executions[0]['implementation'],
                        execution_id=digest([e['id'] for e in executions]))
                    self._record_execution(state, receipt)
            return self._projection(project, state, directory)

    def _record_execution(self, state, receipt):
        if self.review_authority is None:
            raise ForgeError('Execution evidence requires the trusted controller authority')
        proof = self.review_authority.validate(receipt)
        if proof.get('domain') != 'managed-execution-v1' or proof.get('phase') not in {'developer', 'verifier'}:
            raise ForgeError('Execution evidence has the wrong authority domain')
        if proof.get('task_id') != state['id']:
            raise ForgeError('Execution evidence belongs to a different task')
        state['managed'].setdefault('execution_evidence', {})[proof['phase']] = dict(payload=receipt.payload, signature=receipt.signature)

    def record_execution(self, task_id, expected_fingerprint, *, receipt):
        """Trusted launch completion only; never exposed by the harness bridge."""
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            self._validate(state, task_id)
            if self.review_authority is None:
                raise ForgeError('Execution evidence requires the trusted controller authority')
            proof = self.review_authority.validate(receipt)
            if proof.get('fingerprint') != expected_fingerprint or tasks.current(project, state)[3] != expected_fingerprint:
                raise ForgeError('Execution receipt candidate is stale')
            self._record_execution(state, receipt)
            return self._projection(project, state, directory)

    def record_review(self, task_id, expected_fingerprint, claims, *, receipt):
        if self.review_authority is None:
            raise ForgeError('Independent read-only reviewer unavailable')
        proof = self.review_authority.validate(receipt)
        claims = _json_copy(claims)
        if (proof.get('domain') not in {None, 'independent-review-v1'} or
                proof.get('task_id') != task_id or proof.get('fingerprint') != expected_fingerprint or
                proof['claims_digest'] != digest(claims)):
            raise ForgeError('Reviewer receipt does not match this candidate/result')
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            metadata = self._validate(state, task_id)
            developer = metadata['request']['contract'].get('developer_session_id')
            if not developer or proof['session_id'] == developer:
                raise ForgeError('Reviewer must have a distinct controller-bound session')
            tree, readonly, _, fingerprint = tasks.current(project, state)
            if fingerprint != expected_fingerprint:
                raise ForgeError('Reviewer candidate is stale')
            if proof.get('input_digest'):
                self._review_input(metadata, directory, proof['input_digest'])
            tasks.validate_review_claims(project, tree, readonly, claims, allow_failure=True)
            passed = all(claim['status'] != 'fail' for claim in claims.values())
            state['review'] = dict(fingerprint=fingerprint, time=tasks.now(), passed=passed,
                                   type='independent-controller-review', claims=claims,
                                   receipt=dict(payload=receipt.payload, signature=receipt.signature))
            return self._projection(project, state, directory)

    @staticmethod
    def _review_input(metadata, directory, input_digest):
        label = metadata.get('review_inputs', {}).get(input_digest)
        if not label or Path(label).is_absolute() or '..' in Path(label).parts:
            raise ForgeError('Review input does not belong to this task')
        value = bounded_read_json(no_symlinks(directory / label), maximum=REVIEW_BYTES*4)
        if digest(value) != input_digest:
            raise ForgeError('Review input evidence changed')
        return value

    def evaluate_acceptance(self, task_id, *, expected_fingerprint=None, deadline=None):
        if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            raise ForgeError('Acceptance deadline must be an absolute finite time')
        if deadline is not None and time.time() >= deadline:
            raise ForgeError('Development request deadline exhausted before acceptance')
        project = self._project()
        with tasks.locked(project, task_id) as (directory, state):
            self._validate(state, task_id)
            if not self._expected(project, state, expected_fingerprint):
                return self._projection(project, state, directory, stale=True)
            tasks.check(project, state)
            result = self._projection(project, state, directory)
            if result['ready']:
                if deadline is not None and time.time() >= deadline:
                    raise ForgeError('Development request deadline exhausted before acceptance')
                state['managed']['acceptance'] = dict(fingerprint=result['source_fingerprint'],
                                                       time=tasks.now(), assurance=result['assurance'])
                result['accepted_fingerprint'] = result['source_fingerprint']
                result['development_state'] = 'accepted'
            return result

    def status(self, task_id):
        project = self._project()
        with tasks.locked(project, task_id, write=False) as (directory, state):
            self._validate(state, task_id)
            return self._projection(project, state, directory)

    def _projection(self, project, state, directory, *, stale=False):
        metadata = self._validate(state, state['id'])
        acceptance = metadata.get('acceptance') or {}
        result = dict(schema=SCHEMA, task_id=state['id'], project_id=self.project_id,
                      source_fingerprint=None, baseline_fingerprint=metadata['bound_contract']['baseline_fingerprint'],
                      contract_digest=metadata['contract_digest'], assurance='gate_only',
                      run_policy='observe', development_state='incomplete', gates={}, violations=[],
                      accepted_fingerprint=acceptance.get('fingerprint'), ready=False,
                      verification_source='live_tree_rechecked', changed=[], changed_total=0,
                      truncated=False, feedback=[], execution_evidence={})
        try:
            if _project_identity(self.project_root) != metadata['bound_contract']['project_identity']:
                raise ForgeError('Physical project root changed from the original task baseline')
            tree, readonly, _, fingerprint = tasks.current(project, state)
            result['source_fingerprint'] = fingerprint
            changed = changes(state['baseline'], tree)
            result.update(changed=_bounded(changed), changed_total=len(changed))
            result['truncated'] = len(result['changed']) != len(changed)
            gates = {public: tasks.evidence_status(state[key], fingerprint)
                     for public, key in (('check', 'check'), ('verify', 'verification'), ('review', 'review'))}
            violations = list((state.get('check') or {}).get('violations', []))
            verification = state.get('verification')
            if verification:
                logs = tasks.verification_log_problems(project, state, directory)
                if logs:
                    gates['verify'] = 'failed'
                    violations.extend(logs)
                if verification.get('status') == 'running':
                    gates['verify'] = 'unknown'
                if gates['verify'] == 'passed':
                    expected = [c['id'] for c in project.pipeline(state['pipeline'])['verify']]
                    commands = verification.get('commands', [])
                    if ([c.get('id') for c in commands] != expected or
                            any(c.get('passed') is not True or c.get('log_base') != 'task' for c in commands)):
                        gates['verify'] = 'failed'
                        violations.append('Verification evidence does not cover the fixed command definitions')
            review = state.get('review')
            if review and review.get('type') == 'controller-review-outcome':
                gates['review'] = review['status'] if review['fingerprint'] == fingerprint else 'stale'
                violations.append(review.get('reason', 'Independent Review did not complete'))
            elif self.review_authority is None or (not review and not self.review_authority.available):
                gates['review'] = 'unavailable'
            elif review:
                try:
                    proof = self.review_authority.validate(ReviewReceipt(**review.get('receipt', {})))
                    developer = metadata['request']['contract'].get('developer_session_id')
                    if (proof.get('domain') not in {None, 'independent-review-v1'} or
                            review.get('type') != 'independent-controller-review' or not developer or
                            proof['session_id'] == developer or proof['task_id'] != state['id'] or
                            proof['fingerprint'] != review['fingerprint'] or
                            proof['claims_digest'] != digest(review['claims'])):
                        raise ForgeError('Invalid independent Review provenance')
                    if review['fingerprint'] == fingerprint:
                        tasks.validate_review_claims(project, tree, readonly, review['claims'], allow_failure=True)
                    if review['passed'] is not all(claim['status'] != 'fail' for claim in review['claims'].values()):
                        raise ForgeError('Review outcome differs from its signed claims')
                    if proof.get('input_digest'):
                        reviewed = self._review_input(metadata, directory, proof['input_digest'])
                        if reviewed.get('fingerprint') != review['fingerprint']:
                            raise ForgeError('Review input has a different candidate')
                except (ForgeError, KeyError, TypeError, ValueError):
                    gates['review'] = 'failed'
                    violations.append('Review is not a trusted independent result for this candidate')
            if _index_snapshot(self.project_root) != metadata['bound_contract']['staging']:
                gates['check'] = 'failed'
                violations.append('DF-01 Git staging changed from the original task baseline')
            unresolved = [n for n in state['notes'] if n['kind'] == 'question' and not n.get('resolved')]
            if unresolved:
                violations.append('Unresolved task questions remain')
            execution_ok = True
            if metadata['request']['contract'].get('require_enforcement'):
                implementations = set()
                for phase in ('developer', 'verifier'):
                    status = 'missing'
                    record = metadata.get('execution_evidence', {}).get(phase)
                    if record:
                        try:
                            if self.review_authority is None:
                                raise ForgeError('Execution authority unavailable')
                            proof = self.review_authority.validate(ReviewReceipt(**record))
                            if (proof.get('domain') != 'managed-execution-v1' or proof.get('phase') != phase or
                                proof.get('task_id') != state['id'] or
                                not all(isinstance(proof.get(k), str) and proof[k] for k in ('backend','implementation','execution_id'))):
                                raise ForgeError('Invalid execution provenance')
                            status = 'passed' if proof['fingerprint'] == fingerprint else 'stale'
                            identity = dict(backend=proof['backend'], implementation=proof['implementation'])
                            if self.review_authority.execution_identity is not None and identity != self.review_authority.execution_identity:
                                status = 'stale'
                            implementations.add((proof['backend'], proof['implementation']))
                        except (ForgeError, KeyError, TypeError, ValueError):
                            status = 'failed'
                    result['execution_evidence'][phase] = status
                    execution_ok &= status == 'passed'
                execution_ok &= len(implementations) == 1
                if not execution_ok:
                    violations.append('Enforced execution requires current trusted developer and verifier completion evidence')
            if (tasks.current(project, state)[3] != fingerprint or stale or
                    _project_identity(self.project_root) != metadata['bound_contract']['project_identity']):
                gates = {key: 'stale' for key in gates}
                violations.append('Candidate changed; current evidence must be refreshed')
            result['gates'] = gates
            result['feedback'] = _feedback(state, directory)
            result['violations'] = _bounded([str(v)[:2048] for v in violations], count=64, byte_limit=16384)
            result['truncated'] |= len(result['violations']) != len(violations)
            result['ready'] = all(value == 'passed' for value in gates.values()) and not unresolved and execution_ok
            if result['ready'] and metadata['request']['contract'].get('require_enforcement'):
                result['assurance'] = 'enforced'
            if result['ready'] and acceptance.get('fingerprint') == fingerprint:
                result['development_state'] = 'accepted'
            elif ('stale' in gates.values() or 'stale' in result['execution_evidence'].values() or
                  acceptance.get('fingerprint') and acceptance['fingerprint'] != fingerprint):
                result['development_state'] = 'stale'
            elif gates['check'] == 'missing':
                result['development_state'] = 'developing'
            elif gates['check'] == 'passed' and gates['verify'] == 'missing':
                result['development_state'] = 'verification_pending'
            elif gates['check'] == gates['verify'] == 'passed' and gates['review'] != 'passed':
                result['development_state'] = 'review_pending'
        except (ForgeError, OSError, ValueError, KeyError, TypeError) as error:
            result.update(gates={key: 'stale' for key in ('check', 'verify', 'review')},
                          development_state='stale', violations=[str(error)[:2048]], ready=False)
        return result


__all__ = ['ManagedTasks', 'ReviewAuthority', 'ReviewReceipt']
