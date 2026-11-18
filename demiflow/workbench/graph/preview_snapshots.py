"""Bounded presentation cache, never a run fact or an execution authority.

Only the read-only compiler populates it. Entries are scoped to a registered
project and the exact preview digest; client paths are never opened here.
"""
from collections import OrderedDict
from copy import deepcopy
import threading
import time

from demiflow.forge.platform import bounded_json
from demiflow.forge.schema import ForgeError

KINDS = frozenset({'implementation', 'prompt', 'configuration', 'callsite'})


def validate_basis(basis):
    if (type(basis) is not dict
            or set(basis) != {'kind', 'entry', 'params', 'digest', 'dag_id'}
            or basis.get('kind') != 'preview'
            or any(type(basis.get(k)) is not str or not basis[k] or len(basis[k]) > 4096
                   for k in ('entry', 'digest', 'dag_id'))
            or (basis.get('params') is not None and
                (type(basis['params']) is not str or not basis['params'] or len(basis['params']) > 4096))
            ):
        raise ForgeError('Invalid plan source address', code='invalid_request')
    return basis


def validate_request(request):
    validate_basis(request.get('basis'))
    if (type(request.get('kind')) is not str or request['kind'] not in KINDS
            or any(type(request.get(k)) is not str or not request[k] or len(request[k]) > 4096
                   for k in ('logical_node_id', 'occurrence_id'))):
        raise ForgeError('Invalid plan node address', code='invalid_request')
    return request


class PreviewSnapshots:
    def __init__(self, *, max_entries=8, max_bytes=16 * 1024**2, ttl=1800, clock=time.monotonic):
        self._entries = OrderedDict()
        self._bytes = 0
        self._max_entries, self._max_bytes, self._ttl = max_entries, max_bytes, ttl
        self._clock, self._lock = clock, threading.Lock()

    def _prune(self):
        for key, (created, _, _, size) in tuple(self._entries.items()):
            if self._clock() - created >= self._ttl:
                self._bytes -= size
                del self._entries[key]

    def put(self, root, preview):
        # Remove the private bundle from the public response. The caller owns
        # this fresh worker result; no source or executable object is retained.
        bundle = preview.pop('_node_details', {})
        pipeline = preview.pop('_pipeline_detail', None)
        size = len(bounded_json({'nodes': bundle, 'pipeline': pipeline}, 4 * 1024**2))
        basis = {k: preview['basis'][k] for k in ('entry', 'params', 'digest', 'dag_id')}
        basis['kind'] = 'preview'
        nodes = {n['occurrence_id']: n['logical_node_id'] for n in preview['dag']['nodes']}
        key = (str(root), basis['digest'])
        with self._lock:
            self._prune()
            if key in self._entries:
                self._bytes -= self._entries.pop(key)[3]
            while self._entries and (len(self._entries) >= self._max_entries or self._bytes + size > self._max_bytes):
                self._bytes -= self._entries.popitem(last=False)[1][3]
            if size > self._max_bytes:
                raise ForgeError('Plan materials exceed presentation budget', code='resource_limit')
            self._entries[key] = (self._clock(), basis, (nodes, bundle, pipeline), size)
            self._bytes += size
        return preview

    def read(self, root, **request):
        validate_request(request)
        with self._lock:
            self._prune()
            cached = self._entries.get((str(root), request['basis']['digest']))
            if cached is None:
                raise ForgeError('preview_expired: 解析快照已过期，请重新解析计划。', code='conflict')
            _, basis, (nodes, bundle, _), _ = cached
            if request['basis'] != basis or nodes.get(request['occurrence_id']) != request['logical_node_id']:
                raise ForgeError('Plan node does not belong to this preview', code='conflict')
            detail = bundle.get(request['occurrence_id'], {}).get(request['kind'])
            if detail is None:
                detail = dict(availability='unavailable', reason='本节点没有可核验的这类材料。',
                              source_basis='compiled_snapshot', path=None, sha256=None,
                              start_line=None, end_line=None, language=None, text=None, truncated=False)
            return deepcopy(dict(detail, format='demiflow.plan_node_detail.v1', **request))

    def read_pipeline(self, root, *, basis):
        validate_basis(basis)
        with self._lock:
            self._prune()
            cached = self._entries.get((str(root), basis['digest']))
            if cached is None:
                raise ForgeError('preview_expired: 解析快照已过期，请重新解析计划。', code='conflict')
            _, expected, (_, _, detail), _ = cached
            if basis != expected:
                raise ForgeError('Pipeline does not belong to this preview', code='conflict')
            if detail is None:
                from .plan_details import unavailable
                detail = unavailable('这份解析没有保存入口源码，请重新解析。')
            return deepcopy(dict(detail, format='demiflow.plan_pipeline_source.v1', basis=basis))
