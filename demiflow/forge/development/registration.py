"""Signed, bounded registration changes; declarations remain product-owned.

The controller holds the workspace publication lock and its turn transaction.
Forge journals exact bytes but never writes the two public declarations.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from .project import ForgeError, Project, binding_components, digest, no_symlinks, relative, safe_id, within

MAX_RAW = 64 * 1024
MAX_JOURNAL = 2 * 1024 * 1024


def sha(raw):
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _read(path):
    no_symlinks(path)
    if not path.is_file() or path.stat().st_size > MAX_RAW:
        raise ForgeError('Registration declaration exceeds its regular-file byte budget')
    return path.read_bytes().decode('utf-8')


def _object(raw):
    if not isinstance(raw, str) or len(raw.encode()) > MAX_RAW:
        raise ForgeError('Registration declaration exceeds its byte budget')
    value = json.loads(raw)
    if type(value) is not dict:
        raise ForgeError('Registration declaration must be a JSON object')
    return value


def _authority(project):
    authority = getattr(project, 'managed_authority', None)
    if authority is None:
        raise ForgeError('Registration requires the protected controller authority')
    return authority


def records(project, state, *, pending=False):
    from .managed import ReviewReceipt
    items = state.get('managed', {}).get('registration_revisions', [])
    if type(items) is not list or len(items) > 64 or len(json.dumps(items).encode()) > MAX_JOURNAL:
        raise ForgeError('Registration journal exceeds its bounded history')
    previous = state['binding']; committed = None; awaiting = False
    for item in items:
        plan = item['plan']
        proof = _authority(project).validate(ReviewReceipt(**item['receipt']))
        body = {k: v for k, v in plan.items() if k != 'revision_id'}
        kind = plan.get('kind', 'registration')
        domain = {'registration': 'managed-registration-v1', 'implementation': 'managed-implementation-v1'}.get(kind)
        if (domain is None or proof != dict(domain=domain, task_id=state['id'], revision_id=digest(body)) or
                plan['revision_id'] != proof['revision_id'] or plan['before_binding'] != previous or awaiting or
                item['status'] not in {'prepared', 'committed', 'aborted'} or
                kind == 'implementation' and item['status'] != 'committed'):
            raise ForgeError('Registration journal provenance or chain changed')
        if item['status'] != 'aborted': previous = plan['after_binding']
        if item['status'] == 'committed': committed = plan['revision_id']
        elif item['status'] == 'prepared': awaiting = True
    if state.get('managed', {}).get('bound_contract', {}).get('registration_head') != committed:
        raise ForgeError('Registration journal differs from the bound revision')
    if awaiting and not pending:
        raise ForgeError('Registration publication is pending; complete its exact journal before development')
    return items


def binding_for(project, state, *, runtime=False):
    items = [i for i in records(project, state) if i['status'] == 'committed']
    declarations = [i for i in items if i['plan'].get('kind', 'registration') == 'registration']
    if declarations:
        original = declarations[-1]['plan']['workspace']
        current = _read(project.root / 'workspace.json')
        if sha(current) != original['after_sha256']:
            if (not runtime or sha(original['after_raw']) != original['after_sha256'] or
                    {k: v for k, v in _object(current).items() if k != 'platform'} !=
                    {k: v for k, v in _object(original['after_raw']).items() if k != 'platform'}):
                raise ForgeError('Registered workspace metadata changed outside its authorized revision')
    return items[-1]['plan']['after_binding'] if items else state['binding']


def metadata_overlays(project, state):
    items = [i for i in records(project, state) if i['status'] == 'committed' and
             i['plan'].get('kind', 'registration') == 'registration']
    if not items: return {}
    first, last = items[0]['plan'], items[-1]['plan']
    return {str(no_symlinks(project.root / 'workspace.json')):
            (first['workspace']['before_sha256'], last['workspace']['after_sha256'])}


def readonly_baseline(project, state):
    result = dict(state['readonly_baseline'])
    for path, (before, after) in metadata_overlays(project, state).items():
        for label, value in list(result.items()):
            if str(project.root / label) == path:
                if value != before:
                    raise ForgeError('Registration readonly exception differs from the original baseline')
                result[label] = after
    return result


def supplemental_sources(project, state):
    sources = copy.deepcopy(state['managed']['bound_contract'].get('context_baseline'))
    if sources is None: return None
    overlays = metadata_overlays(project, state)
    for source in sources:
        if source['canonical_path'] in overlays:
            before, after = overlays[source['canonical_path']]
            if source['sha256'] != before:
                raise ForgeError('Registration context exception differs from the original authority')
            source['sha256'] = after
    return sources


def original_sources(project, state, sources):
    result = copy.deepcopy(sources)
    overlays = metadata_overlays(project, state)
    for source in result:
        if source['canonical_path'] in overlays:
            before, after = overlays[source['canonical_path']]
            if source['sha256'] == after: source['sha256'] = before
    return result


def _public(plan, status):
    return dict(plan, status=status, request_id=plan['request']['request_id'], event_ids=plan['request'].get('event_ids', []))


def plan_for(project, state, revision_id=None, *, request_id=None):
    if revision_id is not None and request_id is not None:
        raise ForgeError('Select a registration by revision or request, not both')
    items = records(project, state, pending=True)
    if request_id is not None:
        selected = next((i for i in items if i['plan']['request']['request_id'] == request_id), None)
        return _public(selected['plan'], selected['status']) if selected else None
    selected = next((i for i in items if i['plan']['revision_id'] == revision_id), None) if revision_id else next(
        (i for i in items if i['status'] == 'prepared'), None)
    if revision_id and selected is None:
        raise ForgeError('Unknown registration revision')
    return _public(selected['plan'], selected['status']) if selected else None


def prepare(project, state, *, request_id, event_ids, expected_fingerprint, flows, workspace_before, workspace_after):
    from . import tasks
    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 256:
        raise ForgeError('Registration needs a bounded trusted request ID')
    if (type(event_ids) is not list or not event_ids or len(event_ids) > 256 or
            any(not isinstance(e, str) for e in event_ids) or len(set(event_ids)) != len(event_ids)):
        raise ForgeError('Registration requires genuine human decision event references')
    decisions = {n.get('event_id') for n in state['notes'] if n['kind'] == 'decision'}
    if any(event not in decisions for event in event_ids):
        raise ForgeError('Registration requires genuine human decision event references')
    if type(flows) is not list or not 1 <= len(flows) <= 8:
        raise ForgeError('Register one to eight new flows per request')
    before_ws, after_ws = _object(workspace_before), _object(workspace_after)
    request = dict(request_id=request_id, event_ids=event_ids, flows=flows,
                   workspace_before=sha(workspace_before), workspace_after=sha(workspace_after))
    items = records(project, state, pending=True)
    for item in items:
        if item['plan']['request']['request_id'] == request_id:
            if item['plan']['request'] != request:
                raise ForgeError('Registration request ID was reused with different content')
            return _public(item['plan'], item['status'])
    if any(i['status'] == 'prepared' for i in items):
        raise ForgeError('Complete the pending registration before another request')
    if tasks.pending_questions(state):
        raise ForgeError('needs_input: answer unresolved questions before registering a flow')
    if tasks.current(project, state)[3] != expected_fingerprint:
        raise ForgeError('Registration candidate changed from the expected fingerprint')
    if project.config['schema'] != 'demiforge-project-v3':
        raise ForgeError('Flow registration requires candidate.10 project-v3')
    before_raw = _read(project.path)
    if sha(_read(project.root / 'workspace.json')) != request['workspace_before']:
        raise ForgeError('Workspace declaration changed before registration')
    proposed = copy.deepcopy(project.config)
    owner = proposed['projects'][state['pipeline']]
    if 'flows' not in owner: raise ForgeError('Selected owner does not support named flows')
    expected_ws = copy.deepcopy(before_ws)
    if type(expected_ws.get('entries')) is not dict:
        raise ForgeError('Workspace requires explicit flow entries')
    for flow in flows:
        if (type(flow) is not dict or set(flow) != {'flow_id', 'path', 'symbol', 'params'} or
                any(not isinstance(v, str) or not v or len(v) > 4096 for v in flow.values())):
            raise ForgeError('Registration requires exactly flow_id, path, symbol and params')
        safe_id(flow['flow_id'])
        entry = flow['path'] + ':' + flow['symbol']
        if flow['flow_id'] in owner['flows'] or entry in expected_ws['entries']:
            raise ForgeError('Registration cannot replace a flow or append different parameters to an existing entry')
        for path in (flow['path'], flow['params']):
            relative(path, modern=True)
            if (not isinstance(path, str) or path == owner['path'] or not within(path, owner['path']) or
                    not any(within(path, w) for w in state['writes']) or
                    not any(within(path, w) for w in proposed['watch'])):
                raise ForgeError('Registration source must be inside the existing owner, writes and watch scope')
            target = no_symlinks(project.root / path)
            if not target.is_file() or target.stat().st_size > 1024 * 1024:
                raise ForgeError('Registration requires bounded existing source files')
            if any(target.resolve().is_relative_to((project.root / ro).resolve()) for ro in proposed['readonly']):
                raise ForgeError('Registration source intersects readonly authority')
        owner['flows'][flow['flow_id']] = dict(path=flow['path'], symbol=flow['symbol'])
        expected_ws['entries'][entry] = [flow['params']]
    if after_ws != expected_ws:
        raise ForgeError('Workspace patch must only add the requested exact entries and parameter paths')
    candidate = object.__new__(Project)
    candidate.root, candidate.control, candidate.path = project.root, project.control, project.path
    candidate.config, candidate.task_store = proposed, project.task_store
    candidate.validate()
    from ..platform import bounded_json
    after_raw = bounded_json(proposed, MAX_RAW).decode('utf-8')
    components = binding_components(project.path, project.config)
    before_binding = digest(components)
    if before_binding != binding_for(project, state):
        raise ForgeError('Authority changed while preparing registration')
    after_binding = digest(dict(components, config=sha(after_raw)))
    def declaration(path, before, after):
        return dict(path=str(path), before_raw=before, before_sha256=sha(before), after_raw=after, after_sha256=sha(after))
    body = dict(kind='registration', request=request, fingerprint=expected_fingerprint, before_binding=before_binding,
        after_binding=after_binding, project=declaration(project.path, before_raw, after_raw),
        workspace=declaration(project.root / 'workspace.json', workspace_before, workspace_after))
    if len(json.dumps(body).encode()) > MAX_JOURNAL // 2:
        raise ForgeError('Registration revision exceeds its byte budget')
    plan = dict(body, revision_id=digest(body))
    receipt = _authority(project).issue_registration(state['id'], plan['revision_id'])
    item = dict(plan=plan, status='prepared', receipt=dict(payload=receipt.payload, signature=receipt.signature))
    if len(items) >= 64 or len(json.dumps([*items, item]).encode()) > MAX_JOURNAL:
        raise ForgeError('Registration journal budget exhausted')
    state['managed'].setdefault('registration_revisions', []).append(item)
    return _public(plan, 'prepared')


def commit(project, state, revision_id):
    items = records(project, state, pending=True)
    item = next((i for i in items if i['plan']['revision_id'] == revision_id), None)
    if item is None: raise ForgeError('Unknown registration revision')
    if item['plan'].get('kind', 'registration') != 'registration':
        raise ForgeError('Implementation adoption is not a public declaration transaction')
    if item['status'] == 'aborted': raise ForgeError('An aborted registration cannot be committed')
    plan = item['plan']
    for key in ('project', 'workspace'):
        declaration = plan[key]
        if sha(_read(Path(declaration['path']))) != declaration['after_sha256']:
            raise ForgeError('Registration declarations are not both the exact prepared after bytes')
    if project.binding() != plan['after_binding']:
        raise ForgeError('Registration authority changed before commit')
    if item['status'] == 'committed': return
    if item is not items[-1]: raise ForgeError('Registration commit is not the current revision')
    item['status'] = 'committed'
    metadata = state['managed']
    metadata['bound_contract']['registration_head'] = revision_id
    metadata['contract_digest'] = digest(metadata['bound_contract'])


def abort(project, state, revision_id):
    item = next((i for i in records(project, state, pending=True)
                 if i['plan']['revision_id'] == revision_id), None)
    if item is None: raise ForgeError('Unknown registration revision')
    if item['plan'].get('kind', 'registration') != 'registration':
        raise ForgeError('Implementation adoption cannot be aborted as a registration')
    if item['status'] == 'aborted': return
    if item['status'] != 'prepared': raise ForgeError('Committed registration cannot be aborted')
    for key in ('project', 'workspace'):
        declaration = item['plan'][key]
        if sha(_read(Path(declaration['path']))) != declaration['before_sha256']:
            raise ForgeError('Only an unpublished registration with both original declarations can be aborted')
    item['status'] = 'aborted'


def adopt_implementation(project, state, *, request_id, before_components, after_components,
                         allowed_files, authorization):
    """Explicit reviewed upgrade only. Never called by the harness or stale fallback."""
    if (not isinstance(request_id, str) or not 1 <= len(request_id) <= 256 or
            not isinstance(authorization, str) or not authorization.strip() or len(authorization.encode()) > 16384):
        raise ForgeError('Implementation adoption requires an explicit bounded authorization and request ID')
    def valid_sha(value):
        return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)
    for components in (before_components, after_components):
        if (type(components) is not dict or set(components) != {'config', 'tool'} or
                not valid_sha(components['config']) or type(components['tool']) is not dict or
                not 1 <= len(components['tool']) <= 256 or
                any(not isinstance(k, str) or len(k) > 512 or not k.startswith('demiflow/') or
                    not valid_sha(v) for k, v in components['tool'].items())):
            raise ForgeError('Implementation adoption requires exact bounded config/tool hash components')
    if (type(allowed_files) is not list or not allowed_files or len(allowed_files) > 256 or
            any(not isinstance(path, str) or path not in after_components['tool'] or '/standards/' in path
                for path in allowed_files) or len(set(allowed_files)) != len(allowed_files)):
        raise ForgeError('Implementation adoption requires the reviewed implementation file allowlist')
    request = dict(request_id=request_id, before_components=before_components, after_components=after_components,
                   allowed_files=sorted(allowed_files), authorization=authorization)
    items = records(project, state)
    for item in items:
        if item['plan']['request']['request_id'] == request_id:
            if item['plan'].get('kind') != 'implementation' or item['plan']['request'] != request:
                raise ForgeError('Implementation adoption request ID was reused with different content')
            return item['plan']['revision_id']
    before, after = before_components['tool'], after_components['tool']
    changed = {path for path in after if before.get(path) != after[path]}
    if (before_components['config'] != after_components['config'] or set(before) - set(after) or
            {k: v for k, v in before.items() if '/standards/' in k} !=
            {k: v for k, v in after.items() if '/standards/' in k} or
            not changed or not changed.issubset(allowed_files)):
        raise ForgeError('Adoption only permits reviewed implementation changes, never config/standard or reduced binding')
    if digest(before_components) != binding_for(project, state):
        raise ForgeError('Reviewed old implementation does not match this exact task binding')
    if binding_components(project.path, project.config) != after_components:
        raise ForgeError('Reviewed new implementation does not match the actual current files')
    body = dict(kind='implementation', request=copy.deepcopy(request), before_binding=digest(before_components),
                after_binding=digest(after_components))
    plan = dict(body, revision_id=digest(body))
    receipt = _authority(project).issue_implementation(state['id'], plan['revision_id'])
    item = dict(plan=plan, status='committed', receipt=dict(payload=receipt.payload, signature=receipt.signature))
    if len(items) >= 64 or len(json.dumps([*items, item]).encode()) > MAX_JOURNAL:
        raise ForgeError('Binding revision journal budget exhausted')
    metadata = state['managed']
    metadata.setdefault('registration_revisions', []).append(item)
    metadata['bound_contract']['registration_head'] = plan['revision_id']
    metadata['contract_digest'] = digest(metadata['bound_contract'])
    return plan['revision_id']
