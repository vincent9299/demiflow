"""Optional development acceptance at the existing run admission boundary.

The protected domain holds policy references only. Conversation Store and Forge
TaskStore remain the owners of turn completion and candidate acceptance.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from demiflow.forge.io import fsync_directory, no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.workbench import host_partition
from .common import digest, read_file, root_identity, strict_json, wire

FORMAT = 'demiflow-development-admission-v1'
MAXIMUM = 256 * 1024
_UNSPECIFIED = object()


def _reject(reason):
    raise ForgeError('Development acceptance required: ' + reason, code='conflict')


def _private(path):
    no_symlinks(path)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        _reject('runtime domain must be a controller-owned 0700 directory')


@contextmanager
def _lock(domain, *, exclusive):
    _private(domain)
    path = no_symlinks(domain / 'development-admission.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError:
            _reject('development admission policy is being changed')
        yield
    finally:
        os.close(fd)


def _read(domain):
    path = no_symlinks(domain / 'development-admission.json')
    if not path.exists(): return dict(format=FORMAT, projects={})
    value = strict_json(read_file(path, MAXIMUM))
    if (type(value) is not dict or set(value) != {'format', 'projects'} or value['format'] != FORMAT or
            type(value['projects']) is not dict or len(value['projects']) > 128):
        _reject('invalid protected policy references')
    for root, binding in value['projects'].items():
        if (not isinstance(root, str) or len(root) > 4096 or not Path(root).is_absolute() or
                type(binding) is not dict or set(binding) != {'workspace','config_path','sha256','policy_hash'} or
                not all(isinstance(binding[k], str) and binding[k] for k in binding) or
                len(binding['workspace']) > 128 or len(binding['config_path']) > 4096 or
                not Path(binding['config_path']).is_absolute() or
                any(len(binding[k]) != 64 or any(c not in '0123456789abcdef' for c in binding[k]) for k in ('sha256','policy_hash'))):
            _reject('invalid protected project binding')
    return value


def install(deployment, config_path):
    """Called after Service seals its configuration, before workers can start.

Omitting a former project or run_policy never revokes require. An explicit
observe for that exact physical root is the only relaxation supported here.
Managed restricted observe grants also bind the safe inspection boundary.
"""
    required = [p for p in deployment.projects.values() if p.get('run_policy') == 'require']
    restricted = deployment.mode == 'restricted' and any(p.get('managed') for p in deployment.projects.values())
    configured = deployment.value.get('runtime_domain')
    if not configured:
        if required or restricted: _reject('managed restricted development needs an explicit runtime domain')
        return
    domain = no_symlinks(Path(configured).absolute())
    path = domain / 'development-admission.json'
    if not required and not restricted and not path.exists(): return
    if domain != domain.resolve(): _reject('runtime domain is not physical')
    for project in deployment.projects.values():
        for other in (Path(project['root']), Path(deployment.state)):
            if domain == other or domain in other.parents or other in domain.parents:
                _reject('runtime domain, project and development state must be disjoint')
    domain.mkdir(mode=0o700, parents=True, exist_ok=True)
    config_path = no_symlinks(Path(config_path).absolute())
    raw = read_file(config_path, 65536)
    value = strict_json(raw)
    normalized = json.loads(wire(deployment.value))
    for project in normalized['projects'].values(): project.pop('identity', None)
    if value != normalized: _reject('configuration snapshot differs from the admitted deployment')
    sha = hashlib.sha256(raw).hexdigest()
    policy_hash = digest(value)
    with _lock(domain, exclusive=True):
        current = _read(domain)
        projects = dict(current['projects'])
        for workspace, project in deployment.projects.items():
            root = root_identity(project['root'])['path']
            policy = project.get('run_policy')
            safe_scope = deployment.mode == 'restricted' and project.get('managed')
            if policy == 'require' and not safe_scope:
                _reject('require needs managed restricted development')
            if root in projects and policy is None:
                # A new omitted/default grant cannot silently relax a prior require.
                _, previous = _deployment(domain, projects[root], Path(root))
                if previous.get('run_policy') == 'require': continue
            if safe_scope:
                projects[root] = dict(workspace=workspace, config_path=str(config_path), sha256=sha, policy_hash=policy_hash)
            elif policy == 'observe':
                projects.pop(root, None)
        if len(projects) > 128: _reject('protected project count exceeds budget')
        updated = dict(format=FORMAT, projects=projects)
        if updated == current: return
        encoded = wire(updated, MAXIMUM)
        temporary = domain / ('.development-admission-' + uuid.uuid4().hex)
        try:
            fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(encoded); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, path)
            fsync_directory(domain)
        finally:
            temporary.unlink(missing_ok=True)


def _deployment(domain, binding, root):
    raw = read_file(no_symlinks(Path(binding['config_path'])), 65536)
    if hashlib.sha256(raw).hexdigest() != binding['sha256']:
        _reject('protected configuration snapshot changed')
    configured = strict_json(raw)
    if digest(configured) != binding['policy_hash']:
        _reject('protected configuration policy hash differs')
    from .policy import Deployment
    deployment = Deployment(configured)
    if Path(deployment.value.get('runtime_domain', '')) != domain:
        _reject('configuration belongs to a different runtime domain')
    project = deployment.project(binding['workspace'], root)
    if not project.get('managed') or deployment.mode != 'restricted':
        _reject('protected configuration does not authorize managed restricted scope')
    return deployment, project


def lookup(root):
    """Return the unique restricted ancestor scope for trusted inspection."""
    root = no_symlinks(Path(root).absolute())
    domain = host_partition.location()
    if not (domain / 'development-admission.json').exists(): return None
    with _lock(domain, exclusive=False):
        matches = [(Path(path), binding) for path, binding in _read(domain)['projects'].items()
                   if root == Path(path) or Path(path) in root.parents]
        if len(matches) > 1: _reject('inspection root has multiple restricted ancestor grants')
        return _deployment(domain, matches[0][1], matches[0][0]) if matches else None


@contextmanager
def guard(root, *, entry=None, expected=_UNSPECIFIED):
    """Hold policy and one Store read snapshot across a short admission commit.

Explicit expected=None means a prepared seal had no certificate; it cannot
acquire one silently after require was enabled. Observe remains unchanged.
"""
    root = no_symlinks(Path(root).absolute())
    domain = host_partition.location()
    if not (domain / 'development-admission.json').exists():
        yield None
        return
    with _lock(domain, exclusive=False):
        matches = [(Path(path), binding) for path, binding in _read(domain)['projects'].items()
                   if root == Path(path) or Path(path) in root.parents]
        if len(matches) > 1: _reject('run root has multiple restricted ancestor grants')
        if not matches:
            yield None
            return
        bound_root, binding = matches[0]
        from .store import Store
        from .development import project_tasks
        deployment, project = _deployment(domain, binding, bound_root)
        if project.get('run_policy', 'observe') == 'observe':
            yield None
            return
        if root != bound_root:
            _reject('nested runtime workspace has no exact required development binding')
        if entry is not None:
            from demiflow.forge.development.project import Project
            registered = Project(bound_root).entries(project['pipeline'])
            if type(entry) is not str or entry not in {value['path']+':'+value['symbol'] for value in registered.values()}:
                _reject('runtime entry is outside the accepted registered pipeline')
        store = Store(deployment.state, deployment.limits)
        with store.connect() as db:
            # Store.connect enforces DELETE journal mode. This read transaction
            # retains its SHARED lock through yield, blocking writer commits.
            db.execute('BEGIN')
            pinned = db.execute('SELECT session FROM project_conversations WHERE workspace=?', (binding['workspace'],)).fetchone()
            if pinned is None: _reject('no primary development conversation')
            row = db.execute('SELECT * FROM sessions WHERE id=? AND workspace=?', (pinned['session'], binding['workspace'])).fetchone()
            if row is None: _reject('primary development conversation is missing')
            session = dict(row)
            if session['policy_hash'] != binding['policy_hash'] or json.loads(session['root']) != root_identity(root):
                _reject('primary conversation policy or physical root changed')
            busy = db.execute("""SELECT 1 FROM turns t JOIN sessions s ON s.id=t.session
                WHERE json_extract(s.root,'$.path')=? AND
                (t.cleanup!='complete' OR t.gate_cleanup!='complete' OR t.gate_phase='repair_pending') LIMIT 1""", (str(root),)).fetchone()
            if busy: _reject('development or compaction still owns the project')
            row = db.execute("SELECT * FROM turns WHERE session=? AND operation='message' ORDER BY created DESC,id DESC LIMIT 1", (session['id'],)).fetchone()
            if row is None: _reject('no accepted user request')
            turn = dict(row)
            state = json.loads(turn.get('development') or '{}')
            if (turn['state'] != 'completed' or turn['stop_at'] is not None or
                    turn['dispatch'] != 'acknowledged' or turn['delivery'] != 'acknowledged' or
                    turn['cleanup'] != 'complete' or turn['gate_cleanup'] != 'complete' or turn['gate_phase'] != 'finished' or
                    state.get('state') != 'accepted' or state.get('busy') is not False or state.get('assurance') != 'enforced'):
                _reject('latest user request has no completed, uncancelled acceptance receipt')
            current = project_tasks(deployment, session, project).runtime_acceptance(session['id'])
            fingerprint = current.get('source_fingerprint')
            if (not current.get('ready') or current.get('development_state') != 'accepted' or
                    current.get('assurance') != 'enforced' or not fingerprint or
                    current.get('accepted_fingerprint') != fingerprint or
                    state.get('source_fingerprint') != fingerprint or state.get('accepted_fingerprint') != fingerprint):
                _reject('current candidate differs from the accepted enforced evidence')
            identity = dict(task_id=session['id'], turn_id=turn['id'], source_fingerprint=fingerprint,
                            policy_hash=binding['policy_hash'], config_sha256=binding['sha256'])
            if expected is not _UNSPECIFIED and expected != identity:
                _reject('prepared acceptance seal is missing or stale; prepare a new run')
            yield identity


def check(root, *, entry=None, expected=_UNSPECIFIED):
    with guard(root, entry=entry, expected=expected) as identity:
        return identity
