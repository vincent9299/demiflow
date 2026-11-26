"""One independent current media-read owner, separate from live kernel owner.

This small durable control identity is not a run ledger. Historical execution
and generation stay in the registered artifact witness, never the read owner.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
import time

from demiflow.forge.process import process_identity
from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.authority import Authority
from demiflow.workbench.analysis.authority_rpc import Exchange
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.oci import object_id

NAME = 'media-read-owner.json'
FORMAT = 'demiflow_analysis_media_read_owner_v1'
CASE_FORMAT = 'demiflow_analysis_case_read_owner_v1'
PROBE_FORMAT = 'demiflow_analysis_media_probe_owner_v1'
FIELDS = {'format', 'kind', 'project', 'principal', 'read_request_id', 'artifact', 'controller',
          'web_parent', 'web_uid', 'web_gid', 'deadline'}


def validate(value):
    case=type(value) is dict and value.get('kind')=='case-read'
    probe=type(value) is dict and value.get('kind')=='media-probe'
    c.fields(value, FIELDS-{'artifact'}|{'probe'} if probe else FIELDS-{'artifact'}|{'case'} if case else FIELDS, label='independent read owner')
    if value['format'] != (PROBE_FORMAT if probe else CASE_FORMAT if case else FORMAT) or value['kind'] not in {'media-read','case-read','media-probe'}:
        c.reject('Read control identity cannot be a kernel generation', code='conflict')
    project_identity(value['project'])
    c.identity(value['principal']); c.identity(value['read_request_id'])
    if probe:
        item=value['probe'];c.fields(item,{'publish_id','object_id','seed_digest','source_digest','content_sha256','config_digest'},label='pending media probe binding')
        if type(item['publish_id']) is not str or not re.fullmatch('p[0-9a-f]{32}',item['publish_id']):c.reject('Invalid owned media publication')
        object_id(item['object_id'])
        for key in ('seed_digest','source_digest','content_sha256','config_digest'):c.sha(item[key])
    elif case:
        c.fields(value['case'],{'object','source_digest','config_digest'},label='fixed Case read binding')
        c.sha(value['case']['source_digest']);c.sha(value['case']['config_digest'])
    if not probe:
        artifact = value['case']['object'] if case else value['artifact']
        c.fields(artifact, {'id','generation','digest'} if case else {'id', 'generation', 'digest', 'sha256', 'bytes'}, label='fixed read artifact')
        object_id(artifact['id']); c.integer(artifact['generation'], 'artifact generation', 2**31-1, 1)
        c.sha(artifact['digest'])
        if not case:
            c.sha(artifact['sha256']); c.integer(artifact['bytes'], 'fixed media artifact bytes', 64*1024*1024, 1)
    for name in ('controller', 'web_parent'):
        item = value[name]
        c.fields(item, {'pid', 'start', 'boot'}, label='original read ' + name)
        c.integer(item['pid'], 'original read PID', 2**31-1, 1)
        c.text(item['start'], 'original start', 32); c.text(item['boot'], 'original boot', 64)
    for name in ('web_uid', 'web_gid'):
        c.integer(value[name], name, 2**32-2)
    if type(value['deadline']) not in {float, int} or not 0 <= value['deadline'] < float('inf'):
        c.reject('Read owner deadline must be finite')
    return value


def load(root):
    return validate(c.decode(_read(Path(root)/NAME)))


def publish(root, authority, read_request_id, artifact):
    return _publish(root,authority,read_request_id,'media-read',artifact)


def seconds(owner):return {'media-read':5,'case-read':60,'media-probe':5}[owner['kind']]


def publish_artifact(root,authority,read_request_id,config):
    from demiflow.workbench.analysis.media_artifact_worker import owner_artifact
    artifact=owner_artifact(config)
    if config['artifact']['project']!=authority.project:c.reject('Media belongs to another project',code='conflict')
    return publish(root,authority,read_request_id,artifact)


def publish_probe(root,authority,read_request_id,config):
    from demiflow.workbench.analysis.media_artifact_worker import probe_binding
    target=probe_binding(config)
    if config['reservation']['seed']['project']!=authority.project:c.reject('Media capture belongs to another project',code='conflict')
    return _publish(root,authority,read_request_id,'media-probe',target)


def publish_case(root,authority,read_request_id,config):
    from demiflow.workbench.analysis.case_worker import configuration
    configuration(config)
    if config['witness']['project']!=authority.project:c.reject('Case read belongs to another project',code='conflict')
    return _publish(root,authority,read_request_id,'case-read',dict(object=config['witness']['artifact'],
        source_digest=config['witness']['source']['digest'],config_digest=c.digest(config)))


def _publish(root,authority,read_request_id,kind,target):
    if authority.check(authority.project, authority.principal, 'read') is not True:
        c.reject('Current read permission is absent',code='operation_rejected')
    if kind=='media-probe' and authority.check(authority.project,authority.principal,'analysis_write') is not True:
        c.reject('Current capture permission is absent',code='operation_rejected')
    value = validate(dict(format=PROBE_FORMAT if kind=='media-probe' else CASE_FORMAT if kind=='case-read' else FORMAT, kind=kind, project=authority.project,
        principal=authority.principal, read_request_id=read_request_id, **({'probe':target} if kind=='media-probe' else {'case':target} if kind=='case-read' else {'artifact':target}),
        controller=process_identity(os.getpid()), web_parent=authority.peer.identity,
        web_uid=authority.peer.uid, web_gid=authority.peer.gid, deadline=time.monotonic()+seconds({'kind':kind})))
    path = no_symlinks(Path(root)/NAME)
    if path.exists():
        original = load(root)
        if value.keys()!=original.keys() or any(value[key] != original[key] for key in value.keys()-{'deadline'}):
            c.reject('Original media read owner needs proved cleanup before replacement', code='busy')
        return c.digest(original)  # never refresh its fixed deadline on retry
    atomic_write_json(path, value)
    return c.digest(value)


class Authorization:
    """Nonblocking original-Web identity and current purpose-specific grants."""
    def __init__(self, deployment, owner, *, peer=None):
        validate(owner)
        original = owner['web_parent']
        if peer is None:
            peer = Peer(original['pid'], owner['web_uid'], owner['web_gid'], original)
        if (not peer.live() or peer.identity != original or peer.uid != owner['web_uid']
                or peer.gid != owner['web_gid']):
            c.reject('Read belongs to another original Web process', code='operation_rejected')
        self.authority = Authority.__new__(Authority)
        self.authority.deployment, self.authority.peer = deployment, peer
        self.authority.project, self.authority.principal = owner['project'], None
        self.owner, self.phase = owner, 'whoami'
        self.exchange = Exchange(self.authority, 'whoami', {})

    def advance(self):
        if not self.exchange.advance():
            return False
        value = self.exchange.value
        if self.phase == 'whoami':
            if value != dict(principal=self.owner['principal']):
                c.reject('Current read principal differs', code='operation_rejected')
            self.authority.principal = self.owner['principal']
            self.phase = 'permission'
            self.exchange = Exchange(self.authority, 'permission', dict(permission='read'))
            return False
        if value != dict(allowed=True):
            c.reject('Current capture permission is absent' if self.phase=='capture-permission' else 'Current read permission is absent', code='operation_rejected')
        if self.phase=='permission' and self.owner['kind']=='media-probe':
            self.phase='capture-permission'
            self.exchange=Exchange(self.authority,'permission',dict(permission='analysis_write'))
            return False
        return True

    def close(self):
        self.exchange.close()


class StopRequest(Authorization):
    def __init__(self, deployment, peer, owner, payload):
        c.fields(payload, {'request_id', 'read_request_id'}, label='independent read stop')
        c.identity(payload['request_id']); c.identity(payload['read_request_id'])
        if payload['read_request_id'] != owner['read_request_id']:
            c.reject('Stop belongs to another media read', code='conflict')
        self.payload = payload
        super().__init__(deployment, owner, peer=peer)
