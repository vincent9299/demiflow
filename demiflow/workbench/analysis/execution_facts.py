"""Bounded, immutable generation provenance and scoped cleanup facts.

These are facts in the existing controller database, not an execution ledger
or a backend. They do not start jobs or inspect a historical environment.
"""
from __future__ import annotations

from pathlib import PurePosixPath

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.execution_backend import CAPABILITIES
from demiflow.workbench.analysis.execution_backend import RECEIPT

MAXIMUM = 16 * 1024
MODES = {'trusted_local', 'oci'}
PROTECTIONS = {'filesystem_isolation', 'network_isolation', 'hard_memory_quota',
               'hard_disk_quota', 'malicious_descendant_containment', 'independent_force_stop'}


def _path(value, label):
    c.text(value, label, 2048)
    path = PurePosixPath(value)
    if not path.is_absolute() or '..' in path.parts or str(path) != value:
        c.reject(label+' must be a normalized absolute path')
    return value


def capabilities(value):
    c.fields(value, {'format', 'mode', 'backend_revision', 'cleanup_scope',
        'resource_observation', 'source_tracking'} | PROTECTIONS,
        {'limits_digest'}, label='execution capability facts')
    if value['format'] != CAPABILITIES:
        c.reject('Unsupported execution capability edition')
    mode = c.one_of(value['mode'], MODES, 'execution mode')
    c.text(value['backend_revision'], 'backend revision', 128)
    if any(type(value[key]) is not bool for key in PROTECTIONS):
        c.reject('Execution protections must be explicit booleans')
    expected_scope = 'managed_processes' if mode == 'trusted_local' else 'owned_cgroup'
    if value['cleanup_scope'] != expected_scope:
        c.reject('Cleanup capability scope differs from its mode')
    c.one_of(value['resource_observation'], {'sampled', 'cgroup_and_sampled'}, 'resource observation')
    if mode == 'trusted_local' and (any(value[key] for key in PROTECTIONS)
            or value['resource_observation'] != 'sampled'):
        c.reject('Trusted execution cannot claim OS isolation or hard quotas')
    if value['source_tracking'] != 'registered_broker_dependencies':
        c.reject('This edition only records registered broker dependencies')
    if 'limits_digest' in value:
        c.sha(value['limits_digest'])
    c.encoded(value, maximum=4096)
    return value


def origin(value):
    """Validate recorded facts without requiring old paths to still exist."""
    if type(value) is not dict:
        c.reject('Generation execution origin requires explicit facts')
    capabilities(value.get('capabilities'))
    if value.get('mode') == 'trusted_local':
        c.fields(value, {'format', 'mode', 'code_digest', 'python_digest', 'python',
            'environment_digest', 'uid', 'gid', 'cwd', 'original_fsize', 'limits',
            'capabilities'}, {'media_tool'}, label='trusted generation environment')
        if value['format'] != 'demiflow_analysis_local_origin_v1':
            c.reject('Unsupported trusted generation origin edition')
        for key in ('code_digest', 'python_digest', 'environment_digest'):
            c.sha(value[key], key)
        _path(value['python'], 'fixed interpreter')
        limits = c.sequence(value['original_fsize'], 'inherited file limits', 2, minimum=2)
        for limit in limits:
            c.integer(limit, 'inherited file limit', 2**63-1, -1)
        if limits[1] != -1 and (limits[0] == -1 or limits[0] > limits[1]):
            c.reject('Inherited soft file limit exceeds its hard limit')
        keys = {'cell_seconds', 'kernel_seconds', 'native_seconds', 'media_seconds',
                'scratch_bytes', 'sample_rss_bytes'}
        c.fields(value['limits'], keys, label='local execution policy')
        for item in value['limits'].values():
            if type(item) not in {int, float} or not 0 < item <= 2**40:
                c.reject('Local execution policy must have finite positive bounds')
        if value['capabilities'].get('limits_digest') not in {None, c.digest(value['limits'])}:
            c.reject('Execution policy differs from its capability digest')
        if value.get('media_tool') is not None:
            tool = value['media_tool']
            c.fields(tool, {'path', 'sha256', 'version'}, label='recorded media tool')
            _path(tool['path'], 'recorded media tool path')
            c.sha(tool['sha256'], 'recorded media tool digest')
            c.text(tool['version'], 'recorded media tool version', 64)
    elif value.get('mode') == 'oci':
        c.fields(value, {'format', 'mode', 'deployment_id', 'helper_sha256',
            'profile_sha256', 'uid', 'gid', 'cwd', 'capabilities'},
            {'python', 'python_digest', 'environment_digest'}, label='isolated generation environment')
        if value['format'] != 'demiflow_analysis_oci_origin_v1':
            c.reject('Unsupported isolated generation origin edition')
        c.identity(value['deployment_id'])
        for key in ('helper_sha256', 'profile_sha256', 'python_digest', 'environment_digest'):
            if key in value:
                c.sha(value[key], key)
        if 'python' in value:
            _path(value['python'], 'recorded isolated interpreter')
    else:
        c.reject('Generation origin must declare its actual execution mode')
    for key in ('uid', 'gid'):
        c.integer(value[key], key, 2**32-2)
    _path(value['cwd'], 'recorded project directory')
    if value['capabilities']['mode'] != value['mode']:
        c.reject('Generation origin and capabilities use different modes')
    c.encoded(value, maximum=MAXIMUM)
    return value


def receipt(value, *, mode=None, job_id=...):
    c.fields(value, {'format', 'job_id', 'backend_mode', 'scope', 'state',
        'managed_processes_exited', 'known_descendants_exited', 'channels_closed',
        'temporary_files_removed', 'reason'}, {'evidence_digest'}, label='scoped cleanup receipt')
    if value['format'] != RECEIPT:
        c.reject('Unsupported cleanup receipt edition')
    actual_mode = c.one_of(value['backend_mode'], MODES, 'cleanup mode')
    if mode is not None and actual_mode != mode:
        c.reject('Cleanup receipt belongs to a different execution mode', code='conflict')
    if value['job_id'] is not None:
        c.identity(value['job_id'], 'cleanup job')
    if job_id is not ... and value['job_id'] != job_id:
        c.reject('Cleanup receipt belongs to a different job', code='conflict')
    if value['scope'] != ('managed_processes' if actual_mode == 'trusted_local' else 'owned_cgroup'):
        c.reject('Cleanup receipt scope differs from its execution mode')
    c.one_of(value['state'], {'pending', 'unknown', 'complete'}, 'cleanup state')
    flags = [value[name] for name in ('managed_processes_exited', 'known_descendants_exited',
                                     'channels_closed', 'temporary_files_removed')]
    if any(type(flag) is not bool for flag in flags) or value['state'] == 'complete' and not all(flags):
        c.reject('Complete cleanup requires every explicit scoped fact')
    if value['reason'] is not None:
        c.text(value['reason'], 'cleanup reason', 256)
    if 'evidence_digest' in value:
        c.sha(value['evidence_digest'])
    c.encoded(value, maximum=MAXIMUM)
    return value


def cleanup_complete(value, *, mode=None, job_id=...):
    """A legacy boolean remains an isolated-only compatibility fact."""
    if type(value) is bool:
        return value is True and mode in {None, 'oci'}
    wrapper = value if type(value) is dict and 'cleanup_receipt' in value else None
    proof = receipt(wrapper['cleanup_receipt'] if wrapper is not None else value, mode=mode, job_id=job_id)
    complete = proof['state'] == 'complete'
    if wrapper is not None:
        if type(wrapper.get('cleanup_complete')) is not bool or wrapper['cleanup_complete'] != complete:
            c.reject('Cleanup result disagrees with its scoped receipt')
        expected_legacy = complete and proof['backend_mode'] == 'oci'
        if wrapper.get('cleanup_proven') is not expected_legacy:
            c.reject('Legacy cleanup proof cannot claim trusted process containment')
    return complete


def _body(db, table, columns, values):
    # Both table/column lists are internal constants. Do not allocate an
    # oversized BLOB before checking the database's declared byte length.
    where = ' AND '.join(key+'=?' for key in columns)
    row = db.execute('SELECT digest,length(body) FROM '+table+' WHERE '+where, values).fetchone()
    if row is None:
        return None
    c.integer(row[1], 'stored execution fact bytes', MAXIMUM, 1)
    raw = db.execute('SELECT substr(body,1,?) FROM '+table+' WHERE '+where, (MAXIMUM+1, *values)).fetchone()[0]
    value = c.decode(bytes(raw), maximum=MAXIMUM)
    if c.digest(value) != row[0]:
        c.reject('Stored execution facts changed', code='conflict')
    return dict(digest=row[0], body=value)


def generation(db, project, session_id, generation_id):
    value = _body(db, 'analysis_generation_environments', ('project', 'session_id', 'generation'),
                  (project, session_id, generation_id))
    if value is not None:
        origin(value['body'])
    return value


def bind_generation(db, *, project, session_id, generation_id, body, now):
    origin(body)
    previous = generation(db, project, session_id, generation_id)
    digest = c.digest(body)
    if previous is not None:
        if previous['digest'] != digest:
            c.reject('Original generation environment is immutable', code='conflict')
        return previous
    row = db.execute('SELECT generation,state FROM analysis_sessions WHERE project=? AND id=?',
                     (project, session_id)).fetchone()
    if row is None or row[0] != generation_id or row[1] not in {'admitted', 'starting'}:
        c.reject('Environment facts require the original newly admitted generation', code='conflict')
    if (db.execute('SELECT count(*) FROM analysis_generation_environments').fetchone()[0] >= 4096
            or db.execute('SELECT count(*) FROM analysis_generation_environments WHERE project=?', (project,)).fetchone()[0] >= 1024):
        c.reject('Generation environment history quota is full', code='resource_limit')
    db.execute('INSERT INTO analysis_generation_environments VALUES (?,?,?,?,?,?)',
               (project, session_id, generation_id, digest, c.encoded(body, maximum=MAXIMUM), now))
    return dict(digest=digest, body=body)


def cleanup(db, project, owner_kind, owner_id, generation_id):
    value = _body(db, 'analysis_cleanup_receipts', ('project', 'owner_kind', 'owner_id', 'generation'),
                  (project, owner_kind, owner_id, generation_id))
    if value is not None:
        receipt(value['body'])
    return value


def pending_operations(db, *, session_id=None, limit=2):
    """Bounded admission query over the existing native-operation ledger.

    Legacy isolated proof retains its old meaning. A trusted operation frees
    its slot only through its separately validated complete scoped receipt.
    """
    c.integer(limit, 'pending operation limit', 2048, 1)
    parameters = []
    condition = ''
    if session_id is not None:
        c.identity(session_id)
        condition = ' AND o.session_id=?'
        parameters.append(session_id)
    return db.execute("""SELECT o.id FROM analysis_operations o
        WHERE o.cleanup_proven=0 AND NOT EXISTS (
            SELECT 1 FROM analysis_cleanup_receipts f
            WHERE f.project=o.project AND f.owner_kind='native'
                AND f.owner_id=o.id AND f.generation=o.generation
                AND json_extract(CAST(f.body AS TEXT),'$.state')='complete')"""
        + condition + ' LIMIT ?', (*parameters, limit)).fetchall()


def save_cleanup(db, *, project, owner_kind, owner_id, generation_id, body, now):
    c.one_of(owner_kind, {'session', 'native', 'media'}, 'cleanup owner kind')
    for value in (owner_id, generation_id):
        c.identity(value)
    receipt(body)
    previous = cleanup(db, project, owner_kind, owner_id, generation_id)
    digest = c.digest(body)
    if previous is not None and previous['digest'] == digest:
        return previous
    if owner_kind == 'session':
        row = db.execute('SELECT generation,job_id FROM analysis_sessions WHERE project=? AND id=?',
                         (project, owner_id)).fetchone()
        if row is None or row[0] != generation_id:
            c.reject('Cleanup does not belong to the original current session', code='conflict')
        environment = generation(db, project, owner_id, generation_id)
        receipt(body, mode=environment['body']['mode'] if environment else 'oci', job_id=row[1])
    elif owner_kind == 'native':
        row = db.execute('SELECT session_id,generation,job_id FROM analysis_operations WHERE project=? AND id=?',
                         (project, owner_id)).fetchone()
        if row is None or row[1] != generation_id:
            c.reject('Cleanup does not belong to the original native operation', code='conflict')
        environment = generation(db, project, row[0], generation_id)
        receipt(body, mode=environment['body']['mode'] if environment else 'oci', job_id=row[2])
    else:
        row=db.execute('SELECT body FROM analysis_media_captures WHERE project=? AND capture_id=?',
                       (project,owner_id)).fetchone()
        if row is None:c.reject('Media cleanup has no original capture owner',code='not_found')
        if generation_id!=owner_id:c.reject('Media cleanup differs from its original capture',code='conflict')
        environment=c.decode(bytes(row[0])).get('reader_environment')
        if environment is not None:
            origin(environment['body'])
            if c.digest(environment['body'])!=environment['digest']:
                c.reject('Original media reader environment changed',code='conflict')
        receipt(body,mode=environment['body']['mode'] if environment else 'oci',job_id=None)
    if previous is not None:
        old = previous['body']
        if old['state'] == 'complete' or (old['job_id'], old['backend_mode'], old['scope']) != (
                body['job_id'], body['backend_mode'], body['scope']):
            c.reject('Original completed cleanup/owner facts are immutable', code='conflict')
        db.execute('UPDATE analysis_cleanup_receipts SET digest=?,body=?,updated=? WHERE project=? AND owner_kind=? AND owner_id=? AND generation=?',
            (digest, c.encoded(body, maximum=MAXIMUM), now, project, owner_kind, owner_id, generation_id))
    else:
        if (db.execute('SELECT count(*) FROM analysis_cleanup_receipts').fetchone()[0] >= 8192
                or db.execute('SELECT count(*) FROM analysis_cleanup_receipts WHERE project=?', (project,)).fetchone()[0] >= 2048):
            c.reject('Scoped cleanup history quota is full', code='resource_limit')
        db.execute('INSERT INTO analysis_cleanup_receipts VALUES (?,?,?,?,?,?,?,?)',
            (project, owner_kind, owner_id, generation_id, digest, c.encoded(body, maximum=MAXIMUM), now, body['backend_mode']))
    return dict(digest=digest, body=body)
