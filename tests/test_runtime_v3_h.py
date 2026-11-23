"""Lane H leaf unit-contract gate G-H: tests/test_runtime_v3_h.py::test_h001..h079.

Every test calls the real lane-H symbol under test. Absent peer lanes (C frozen
DTO/codec, D seal loader, B worker runtime/executor) and the external Ray carrier
are replaced by spec-bound test-domain doubles installed from
_runtime_v3_h_doubles (marked test_doubles in the evidence report). Real Ray /
two-host integration is recorded not_run separately; these leaf tests use a fake
carrier and never contact a cluster.
"""
import contextlib
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
import threading
import time
import zipfile
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _runtime_v3_h_doubles as D

D.assert_tree()
df = D.install()
D.install_fake_seal()


@pytest.fixture(autouse=True)
def _ray_carrier_double(monkeypatch):
    ray = D.build_fake_ray()
    monkeypatch.setitem(sys.modules, 'ray', ray)
    monkeypatch.setitem(sys.modules, 'ray.exceptions', ray.exceptions)
    monkeypatch.setattr(WH, 'WorkerRuntime', FakeRuntime)
    monkeypatch.setattr(HOSTS, '_RAY_AVAILABLE', True)
    monkeypatch.setattr(HOSTS, 'ray', ray, raising=False)
    monkeypatch.setattr(HOSTS, 'RayActorError', ray.exceptions.RayActorError, raising=False)
    monkeypatch.setattr(HOSTS, 'GetTimeoutError', ray.exceptions.GetTimeoutError, raising=False)
    yield

# dataflow_hosts defines RayHostEndpoint only when the carrier imports, so bind
# the declared double for this import alone and unbind before any other lane runs.
_RAY = D.build_fake_ray()
sys.modules['ray'] = _RAY
sys.modules['ray.exceptions'] = _RAY.exceptions

from demiflow.execution import dataflow_authority as AUTH
from demiflow.execution import dataflow_artifacts as ART
from demiflow.execution.hosts import dataflow_control as CTRL
from demiflow.execution import dataflow_hosts as HOSTS
from demiflow.execution import dataflow_services as SVC
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.objects import ARTIFACT_STAGING_BUDGET, RunArtifactStore
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.worker import dataflow_host as WH
from demiflow.resources.manager import ResourceManager

del sys.modules['ray'], sys.modules['ray.exceptions']

ArtifactRef = df.ArtifactRef
_BOUNDS_DEFAULTS = dict(
    max_nodes=8, max_edges=8, max_partitions=8, max_depth=8,
    max_control_bytes=1 << 20, max_row_bytes=1 << 20, max_batch_bytes=1 << 20,
    max_resident_bytes=1 << 20, max_spill_bytes=1 << 20, max_state_bytes=1 << 20,
    max_pending_events=64, max_open_files=64, max_output_rows=1 << 20, timeout_ms=2000,
)


def Bounds(**overrides):
    return df.Bounds(**{**_BOUNDS_DEFAULTS, **overrides})
ControlReply = df.ControlReply
WorkerEvent = df.WorkerEvent
codec = sys.modules['demiflow.execution.contracts.dataflow_codec']

BIG = Bounds(max_spill_bytes=1 << 30, max_control_bytes=1 << 24, max_pending_events=8, timeout_ms=2000)
POLICY = df.RuntimePolicy(provider='local', durability='durable', default_partitions=1, max_replicas=1,
                          fusion=False, checkpoint_rows=100, checkpoint_interval_ms=1000,
                          fairness_quantum_bytes=1 << 20, control_timeout_ms=1000, store_id='store-1',
                          required_recovery='checkpoint')
LOCAL_HOST = df.HostDescriptor(host_id='local', provider='local', runtime_revision='r',
                               store_domains=('store-1',), logical_resources={}, isolation_capabilities=())


def oid(s):
    return hashlib.sha256(s.encode()).hexdigest()


def aref(d, store_id='store-1', size=1, media='application/json'):
    h = oid(d)
    return ArtifactRef(digest=h, size_bytes=size, store_id=store_id, key=h, media_type=media)


def newtmp():
    return Path(tempfile.mkdtemp(prefix='demiflow-h-'))


def mklease(host_id='host-1', expires=None):
    return df.WorkerLease(allocation_id='alloc-1', boot_id='boot-1', stage_id='stage-1',
                          partition_ids=('p0',), host_id=host_id, endpoint_ref='ep', grant_ref=aref('grant'),
                          expires_at_ms=expires if expires is not None else 10 ** 13)


def mk_event(op, kind='progress', alloc='alloc-1', boot='boot-1', payload=None):
    return WorkerEvent(schema='demiflow.dataflow_event.v3', kind=kind, operation_id=oid(op), run_id='run-1',
                       attempt=1, fence='fence-1', allocation_id=alloc, boot_id=boot, plan_revision=1,
                       scope_id='scope-1', controller_epoch=1, payload_ref=None,
                       payload=payload if payload is not None else {'requests': 1})


def mk_reply(op, status='ok', payload=None):
    return ControlReply(operation_id=oid(op), status=status,
                        payload=payload if payload is not None else {'r': 2}, payload_ref=None, error_code=None)


def frame(dto, name):
    raw = codec.encode_wire(dto, type_name=name, bounds=BIG)
    return len(raw).to_bytes(4, 'big') + raw


class FakePort:
    def __init__(self):
        self.pins = {}
        self.reserves = []

    def reserve(self, op, amounts):
        self.reserves.append((op, dict(amounts)))

    def artifact(self, cmd):
        k = (cmd.ref.digest, cmd.owner) if cmd.ref is not None else None
        if cmd.kind == 'pin':
            self.pins[k] = cmd.operation_id
        elif cmd.kind == 'unpin':
            self.pins.pop(k, None)
        return NS(operation_id=cmd.operation_id, present=True, charged_bytes=cmd.size_bytes)


def make_store(tmp, store_id='store-1'):
    return RunArtifactStore(root=Path(tmp) / 'store', store_id=store_id, journal=FakePort(), bounds=BIG)


@contextlib.contextmanager
def driver_env(name):
    tmp = newtmp()
    with RunContext.start(directory=tmp / 'run', limits={'checkpoint_rows': 2, 'max_requests': 10}) as ctx:
        action = RunAction(ctx, 'action-1', {'kind': 'test'})
        ctx.journal.bind_execution_action(ctx.fence, 'action-1', {'kind': 'test'})
        port = AUTH.DriverArtifactPort(action, bounds=BIG)
        store = RunArtifactStore(root=ctx.directory / '.dataflow-store', store_id='store-1', journal=port, bounds=BIG)
        svc = AUTH.DriverJournalService(action, store=store, bounds=BIG)
        svc.register_worker(mklease(), ())
        try:
            yield NS(tmp=tmp, ctx=ctx, action=action, store=store, port=port, svc=svc)
        finally:
            svc.close(deadline_ms=BIG.timeout_ms)
            ctx.journal.complete_execution_action(ctx.fence, 'action-1', {'done': True})


def dev(ctx, kind, op, payload, fence=None):
    return WorkerEvent(schema='demiflow.dataflow_event.v3', kind=kind, operation_id=oid(op), run_id=ctx.ref.run_id,
                       attempt=ctx.attempt, fence=fence or ctx.fence, allocation_id='alloc-1', boot_id='boot-1',
                       plan_revision=0, scope_id='stage-1', controller_epoch=0, payload_ref=None, payload=payload)


class GrantStore:
    store_id = 'store-1'

    def __init__(self, doc):
        self._raw = json.dumps(doc).encode()

    def read(self, ref, *, max_bytes):
        yield self._raw


class ClientTransport:
    def __init__(self, store, replies=None, error=None):
        self.store = store
        self.requests = []
        self._replies = replies or {}
        self._error = error

    def request(self, event, *, deadline_ms):
        self.requests.append(event)
        if self._error is not None:
            raise self._error
        return ControlReply(operation_id=event.operation_id, status='ok',
                            payload={**self._replies.get(event.kind, {}), **({'operation_id': event.operation_id} if 'operation_id' in self._replies.get(event.kind, {}) else {})}, payload_ref=None, error_code=None)


def worker_client(replies=None, error=None, expires=None):
    doc = {'run_id': 'run-1', 'attempt': 1, 'fence': 'fence-1', 'plan_revision': 0, 'controller_epoch': 0, 'scope_id': 'scope-1'}
    lease = mklease(expires=expires if expires is not None else int(time.time() * 1000) + 100000)
    t = ClientTransport(GrantStore(doc), replies, error)
    return AUTH.WorkerJournalClient(transport=t, lease=lease, bounds=BIG), t, lease


class FakeConnection:
    def __init__(self):
        self.out = []
        self.inbuf = b''
        self.closed = False

    def send_bytes(self, v):
        self.out.append(v)

    def recv_bytes(self, n):
        d = self.inbuf[:n]
        self.inbuf = self.inbuf[n:]
        return d

    def poll(self, t):
        return len(self.inbuf) > 0

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self):
        self.cuts = []
        self.replies = []
        self.staged = []
        self.pulled = []
        self.handled = []
        self.driver_socks = {'alloc-1': NS(settimeout=lambda t: None)}

    def stage(self, aid, spec):
        self.staged.append((aid, spec))

    def request_cut(self, aid, req):
        self.cuts.append((aid, req))

    def deliver_control_reply(self, aid, reply):
        self.replies.append((aid, reply))

    def handle(self, aid, env):
        self.handled.append(env['kind'])
        return {'receipt': {}}

    def pull(self, aid, limit):
        return tuple(self.pulled[:limit])

    def _actor(self, aid):
        return NS(pull_events=NS(remote=lambda limit: self.pull(aid, limit)))


class FakeProvider:
    def __init__(self, cleanup='CONFIRMED'):
        self.cleanup = cleanup
        self.absent_calls = []

    def ensure_absent(self, identity, deadline):
        self.absent_calls.append(identity)
        return {'cleanup': self.cleanup, 'evidence_ref': 'x'}


class FakeState:
    def open_scope(self, *a):
        pass


class FakeManager:
    def __init__(self, phase='TERMINATED'):
        self.phase = phase
        self.state = FakeState()
        self.provider = FakeProvider()
        self.provider._control = FakeConn()

    def apply(self, *a):
        pass

    def reconcile_once(self, *a):
        pass

    def get(self, scope):
        return {'allocations': [{'allocation_id': 'alloc-1', 'phase': self.phase,
                                 'cleanup': 'CONFIRMED' if self.phase == 'TERMINATED' else 'NONE'}]}


class FakeModel:
    def __init__(self, **kw):
        self.handled = []

    def handle(self, message, now_ms=None):
        self.handled.append(message.get('kind'))


class FakeExecutor:
    def __init__(self):
        self._closed = False
        self.cuts = []
        self.receipts = []
        self.cancelled = None

    def request_cut(self, r):
        self.cuts.append(r)

    def accept_receipt(self, commit_id, *, cut_id):
        self.receipts.append((commit_id, cut_id))

    def cancel(self, reason, *, deadline_ms):
        self.cancelled = (reason, deadline_ms)

    def close(self):
        self._closed = True


class FakeRuntime:
    def __init__(self, launch_spec, *, control=None, manifest_loader=None):
        self._control = control
        self._v3_config = dict(launch_spec.get('dataflow_v3') or {})
        self._v3_executor = None
        self.grant = self._v3_config.get('grant')
        self.scope_id = launch_spec['scope_id']
        self.run_id = launch_spec['run_id']
        self.allocation_id = launch_spec['allocation_id']
        self.boot_id = launch_spec['boot_id']
        self.controller_epoch = launch_spec['controller_epoch']
        self.plan_revision = 0
        self.closed = False
        self.ready_calls = []

    def register(self):
        import uuid
        self._control({'kind': 'Register', 'operation_id': uuid.uuid4().hex, 'run_id': self.run_id,
                       'controller_epoch': self.controller_epoch, 'allocation_id': self.allocation_id,
                       'boot_id': self.boot_id, 'scope_id': self.scope_id, 'payload': {}})

    def ready(self, aid):
        self.ready_calls.append(aid)

    def handle(self, env, *, now_ms):
        k = env['kind']
        if k == 'Assign':
            self.plan_revision = env['payload']['assignment']['plan_revision']
            self._v3_executor = FakeExecutor()
        return {'kind': k + 'Ack'}

    def close(self):
        self.closed = True
        if self._v3_executor is not None:
            self._v3_executor.close()





def mklaunch():
    return {'directory': '/tmp', 'run_id': 'run-1', 'scope_id': 'action-1', 'allocation_id': 'alloc-1',
            'boot_id': 'boot-1', 'controller_epoch': 1,
            'dataflow_v3': {'bounds': dict(vars(BIG)), 'limits': {'max_requests': 1}, 'code_pin': oid('pin'),
                            'grant': {'grant_id': 'grant-1', 'scope_id': 'action-1'},
                            'store_root': str(newtmp() / 'store'), 'store_id': 'store-1'}}


def mkassignment():
    lease = NS(allocation_id='alloc-1', boot_id='boot-1', host_id='host-1', partition_ids=('p0', 'p1'))
    return NS(action_id='action-1', attempt=1, plan_revision=2, lease=lease,
              sealed=NS(code_pin_ref=aref('pin')),
              stages=(NS(stage_id='stage-a'), NS(stage_id='stage-b')),
              inputs=(NS(channel_id='in-0'),), outputs=(NS(channel_id='out-0'), NS(channel_id='out-1')),
              restore_refs=(aref('cp'),), bounds=BIG)


def new_worker():
    return WH.HostedDataflowWorker(mklaunch(), spec_hash='spec-1', boot_id='boot-1')


def doubled_endpoint(cls, services, phase='TERMINATED'):
    ep = cls.__new__(cls)
    if not hasattr(services, 'resources'):
        services.resources = FakeManager(phase)
    HOSTS._EndpointBase.__init__(ep, mklease('local'), services=services)
    if cls is HOSTS.RayHostEndpoint:
        ep._pending_pull = None
        ep._pull_limit = 0
        ep._pull_started = 0.0
    ep._connector = FakeConn()
    ep._provider = FakeProvider()
    ep._state = FakeState()
    ep._manager = services.resources
    ep._model = FakeModel()
    ep._sock = NS(settimeout=lambda t: None)
    ep._argv = ('x',)
    return ep


# ===== H001-H010 authority =====

def test_h001():
    with driver_env('h001') as e:
        svc = e.svc
        for forbidden in ('start', 'complete', 'commit', 'start_action', 'complete_action'):
            assert not hasattr(svc, forbidden)
        assert svc._journal is e.ctx.journal
        assert not (e.ctx.directory / '.dataflow-effects').exists()
        assert len(list(e.ctx.directory.rglob('*.sqlite'))) == 1
        with pytest.raises(DataflowError) as ei:
            svc.dispatch(dev(e.ctx, 'commit', 'op-x', {}))
        assert ei.value.code == 'INVALID_CONTRACT'


def test_h002():
    with driver_env('h002') as e:
        svc = e.svc
        with pytest.raises(DataflowError) as ei:
            svc.dispatch(dev(e.ctx, 'drop_table', 'op-1', {}))
        assert ei.value.code == 'INVALID_CONTRACT'
        with pytest.raises(DataflowError) as ei:
            svc.dispatch(dev(e.ctx, 'reserve', 'op-2', {'amounts': {'requests': 1}}, fence='stale'))
        assert ei.value.code == 'FENCED'
        small = Bounds(max_control_bytes=64, max_pending_events=8)
        svc_small = AUTH.DriverJournalService(e.action, store=e.store, bounds=small)
        over = dev(e.ctx, 'reserve', 'op-3', {'blob': 'y' * 200})
        with pytest.raises(DataflowError) as ei:
            svc_small.dispatch(over)
        assert ei.value.code == 'BUDGET_EXCEEDED'
        svc_small.close(deadline_ms=100)
        assert svc._queue.maxsize == BIG.max_pending_events
        for i in range(BIG.max_pending_events * 3):
            svc.submit(dev(e.ctx, 'reserve', 'b-%d' % i, {'amounts': {'requests': 1}}))
        time.sleep(0.2)
        assert len(svc._completed) <= BIG.max_pending_events


def test_h003():
    with driver_env('h003') as e:
        svc = e.svc

        def ec(kind, op, eid='eff-1', dig='d' * 64, ref=None):
            return df.EffectCommand(operation_id=oid(op), kind=kind, effect_id=eid, actor_id='actor-1',
                                    request_digest=dig, payload_ref=ref, budget={'requests': 1})

        assert svc.effect(ec('lookup', 'l'), lease=None).status == 'absent'
        assert svc.effect(ec('reserve', 'r'), lease=None).status == 'reserved'
        assert svc.effect(ec('mark_sent', 's'), lease=None).status == 'sent'
        assert svc.effect(ec('lookup', 'after-send'), lease=None).status == 'sent'
        assert not (e.ctx.directory / '.dataflow-effects').exists()
        ref = e.store.put(iter([b'result']), max_bytes=100, media_type='x')
        assert svc.effect(ec('confirm', 'c', ref=ref), lease=None).status == 'confirmed'
        svc.effect(ec('reserve', 'r2', eid='eff-2'), lease=None)
        svc.effect(ec('mark_sent', 's2', eid='eff-2'), lease=None)
        assert svc.effect(ec('unknown', 'u2', eid='eff-2'), lease=None).status == 'unknown'
        with pytest.raises(DataflowError):
            svc.effect(ec('reserve', 'r3', eid='eff-2'), lease=None)
        with pytest.raises(DataflowError) as ei:
            svc.effect(ec('reserve', 'r4', dig='e' * 64), lease=None)
        assert ei.value.code == 'INVALID_CONTRACT'


def test_h004():
    with driver_env('h004') as e:
        svc = e.svc
        svc.reserve('res-1', {'requests': 5}, lease=None)
        svc.reserve('res-1', {'requests': 5}, lease=None)
        with pytest.raises(Exception):
            svc.reserve('res-1', {'requests': 9}, lease=None)
        with pytest.raises(DataflowError):
            svc.reserve('res-2', {}, lease=None)
        with pytest.raises(DataflowError):
            svc.reserve('res-3', {'requests': -1}, lease=None)


def test_h005():
    client, t, lease = worker_client()
    assert client._pending == {} and client._grant_doc is None
    assert not hasattr(client, '_journal') and not hasattr(client, '_connection')
    assert client._bounds is BIG


def test_h006():
    replies = {'effect': {'operation_id': 'op-e', 'status': 'unknown', 'result_ref': None, 'receipt_ref': None}}
    client, t, lease = worker_client(replies=replies)
    cmd = df.EffectCommand(operation_id=oid('op-e'), kind='lookup', effect_id='eff-1', actor_id='a',
                           request_digest='d' * 64, payload_ref=None, budget={})
    assert client.effect(cmd).status == 'unknown'
    assert t.requests[0].kind == 'effect'
    client2, t2, _ = worker_client(error=DataflowError('UNKNOWN_EFFECT', 'lost'))
    with pytest.raises(DataflowError) as ei:
        client2.effect(cmd)
    assert ei.value.code == 'UNKNOWN_EFFECT'


def test_h007():
    client, t, lease = worker_client(replies={'reserve': {'operation_id': 'op-r'}})
    client.reserve(oid('op-r'), {'requests': 1})
    assert t.requests[0].kind == 'reserve' and t.requests[0].payload == {'amounts': {'requests': 1}}
    client2, t2, _ = worker_client(error=DataflowError('BUDGET_EXCEEDED', 'insufficient'))
    with pytest.raises(DataflowError) as ei:
        client2.reserve(oid('op-r2'), {'requests': 1})
    assert ei.value.code == 'BUDGET_EXCEEDED'


def test_h008():
    client, t, lease = worker_client(replies={'read_action': {'action_id': 'action-1', 'declaration': {'kind': 'test'}}})
    doc = client.read_action('action-1')
    assert doc['action_id'] == 'action-1' and t.requests[0].kind == 'read_action'
    client2, t2, _ = worker_client(error=DataflowError('FENCED', 'cross action'))
    with pytest.raises(DataflowError) as ei:
        client2.read_action('other')
    assert ei.value.code == 'FENCED'
    assert not hasattr(client, 'execute')


def test_h009():
    client, t, lease = worker_client(replies={'read_configuration': {'limits': {'max_requests': 10}, 'run_id': 'run-1'}})
    cfg = client.read_configuration()
    assert cfg['limits'] == {'max_requests': 10}
    assert client.read_configuration() == cfg
    assert t.requests[0].kind == 'read_configuration'


def test_h010():
    client, t, lease = worker_client(replies={'check_grant': {'operation_id': 'x'}})
    client.check_grant()
    expired, t2, _ = worker_client(expires=int(time.time() * 1000) - 1)
    with pytest.raises(DataflowError) as ei:
        expired.check_grant()
    assert ei.value.code == 'FENCED'
    with pytest.raises(DataflowError):
        expired.check_grant()
    part, t3, _ = worker_client(error=DataflowError('UNKNOWN_EFFECT', 'partition'))
    with pytest.raises(DataflowError) as ei:
        part.check_grant()
    assert ei.value.code == 'UNKNOWN_EFFECT'


# ===== H011-H014 artifacts =====

def test_h011():
    tmp = newtmp()
    store = make_store(tmp)
    p1 = store.put(iter([json.dumps({'name': 'mod_a', 'code': 'x'}).encode()]), max_bytes=1 << 20, media_type='application/json')
    p2 = store.put(iter([json.dumps({'name': 'mod_b', 'code': 'y'}).encode()]), max_bytes=1 << 20, media_type='application/json')
    pin = store.put(iter([b'pin']), max_bytes=100, media_type='x')
    sealed = df.SealedExecution(logical_ref=pin, physical_ref=pin, programs=(p1, p2), code_pin_ref=pin,
                                identity_bridge_ref=pin, source_refs=(), constants_ref=pin)
    bundle = ART.stage_runtime_bundle(sealed, store=store, bounds=BIG)
    raw = b''.join(store.read(bundle, max_bytes=1 << 20))
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        man = json.loads(z.read('manifest.json'))
        assert man['modules'] == {'mod_a': p1.digest, 'mod_b': p2.digest}
        assert man['schema'] == ART.BUNDLE_SCHEMA
    p3 = store.put(iter([json.dumps({'name': 'mod_a', 'code': 'DIFF'}).encode()]), max_bytes=1 << 20, media_type='application/json')
    sealed2 = df.SealedExecution(logical_ref=pin, physical_ref=pin, programs=(p1, p3), code_pin_ref=pin,
                                 identity_bridge_ref=pin, source_refs=(), constants_ref=pin)
    with pytest.raises(DataflowError) as ei:
        ART.stage_runtime_bundle(sealed2, store=store, bounds=BIG)
    assert ei.value.code == 'INVALID_CONTRACT'


def test_h012():
    tmp = newtmp()
    store = make_store(tmp)
    p1 = store.put(iter([json.dumps({'name': 'm'}).encode()]), max_bytes=1 << 20, media_type='application/json')
    pin = store.put(iter([b'pin']), max_bytes=100, media_type='x')
    sealed = df.SealedExecution(logical_ref=pin, physical_ref=pin, programs=(p1,), code_pin_ref=pin,
                                identity_bridge_ref=pin, source_refs=(), constants_ref=pin)
    bundle = ART.stage_runtime_bundle(sealed, store=store, bounds=BIG)
    cache = tmp / 'cache'
    out = ART.fetch_runtime_bundle(bundle, cache=cache, store=store, bounds=BIG)
    assert out == cache / bundle.digest and (out / 'manifest.json').exists()
    assert not list(cache.glob('.bundle-*'))
    b = io.BytesIO()
    with zipfile.ZipFile(b, 'w') as z:
        z.writestr('../escape', 'x')
    evil = store.put(iter([b.getvalue()]), max_bytes=1 << 20, media_type='application/zip')
    with pytest.raises(DataflowError) as ei:
        ART.fetch_runtime_bundle(evil, cache=tmp / 'c2', store=store, bounds=BIG)
    assert ei.value.code == 'INVALID_CONTRACT'
    assert not list((tmp / 'c2').glob('.bundle-*'))
    b = io.BytesIO()
    with zipfile.ZipFile(b, 'w') as z:
        zi = zipfile.ZipInfo('link')
        zi.external_attr = (0o120777 << 16)
        z.writestr(zi, 't')
    sym = store.put(iter([b.getvalue()]), max_bytes=1 << 20, media_type='application/zip')
    with pytest.raises(DataflowError):
        ART.fetch_runtime_bundle(sym, cache=tmp / 'c3', store=store, bounds=BIG)


def test_h013():
    tmp = newtmp()
    store = make_store(tmp)
    challenge = store.put(iter([b'challenge-bytes']), max_bytes=100, media_type='x')
    host = df.HostDescriptor(host_id='host-1', provider='local', runtime_revision='r',
                             store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
    proof = ART.verify_host_domain(host, store=store, challenge=challenge, bounds=BIG)
    assert proof.durable and proof.challenge_digest == challenge.digest
    wrong = ArtifactRef(digest='a' * 64, size_bytes=challenge.size_bytes, store_id='store-1',
                        key=challenge.digest, media_type='x')
    with pytest.raises(DataflowError):
        ART.verify_host_domain(host, store=store, challenge=wrong, bounds=BIG)
    with pytest.raises(DataflowError) as ei:
        ART.verify_host_domain(host, store=store, challenge=aref('absent'), bounds=BIG)
    assert ei.value.code == 'UNKNOWN_EFFECT'
    host2 = df.HostDescriptor(host_id='h2', provider='local', runtime_revision='r',
                              store_domains=('other',), logical_resources={}, isolation_capabilities=())
    with pytest.raises(DataflowError):
        ART.verify_host_domain(host2, store=store, challenge=challenge, bounds=BIG)


def test_h014():
    tmp = newtmp()
    store = make_store(tmp)
    ref = store.put(iter([b'durable']), max_bytes=100, media_type='x')
    host = df.HostDescriptor(host_id='h', provider='local', runtime_revision='r',
                             store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
    loc = ART.resolve_data_location(ref, host=host, store=store)
    assert loc.domain == 'store-1' and loc.durable_ref == ref and loc.temporary_locator is None
    rayhost = df.HostDescriptor(host_id='h', provider='ray_core', runtime_revision='r',
                                store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
    loc2 = ART.resolve_data_location(ref, host=rayhost, store=store)
    assert loc2.temporary_locator is None and loc2.durable_ref == ref  # no fabricated Ray object handle
    with pytest.raises(DataflowError) as ei:
        ART.resolve_data_location(aref('gone'), host=host, store=store)
    assert ei.value.code == 'UNKNOWN_EFFECT'


# ===== H015-H025, H028-H029 hosts =====

def test_h015():
    with driver_env('h015') as e:
        rayh = df.HostDescriptor(host_id='ray-1', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST, rayh), resources=FakeManager())
        ep = HOSTS.make_host_endpoint(mklease('local'), services=services)
        assert isinstance(ep, HOSTS.LocalHostEndpoint)
        ep2 = HOSTS.make_host_endpoint(mklease('ray-1'), services=services)
        assert isinstance(ep2, HOSTS.RayHostEndpoint)
        with pytest.raises(DataflowError) as ei:
            HOSTS.make_host_endpoint(mklease('nope'), services=services)
        assert ei.value.code == 'INVALID_CONTRACT'
        with pytest.raises(DataflowError):
            df.HostDescriptor(host_id='sp', provider='spark', runtime_revision='r',
                store_domains=('store-1',), logical_resources={}, isolation_capabilities=())


def test_h016(monkeypatch):
    import demiflow.execution.contracts.worker as W
    import demiflow.execution.worker.bootstrap as B
    with driver_env('h016') as e:
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST,))
        ep = doubled_endpoint(HOSTS.LocalHostEndpoint, services)
        assignment = mkassignment()
        calls = []
        ep._build_launch = lambda a: ({'dataflow_v3': {}}, {'grant_id': 'g', 'scope_id': 'action-1'}, {'sha256': oid('pin')})
        ep._send_k3 = lambda kind, payload: calls.append(kind) or {}
        monkeypatch.setattr(B, 'recv_message', lambda sock: {'kind': 'Register'})
        monkeypatch.setattr(W, 'WorkerProtocolModel', FakeModel)
        monkeypatch.setattr(WH, 'make_worker_control_assignment', lambda *a, **k: {'restore_point': None})
        ep._connector.driver_socks = {'alloc-1': ep._sock}
        with pytest.raises(DataflowError, match='staged'):
            ep.activate(assignment)
        ep.prepare(assignment)
        assert ep._connector.staged
        assert ep._manager is services.resources  # one allocator, staged before spawn
        ep.activate(assignment)
        assert ep._activated and calls == ['VerifyArtifactAndCapabilities', 'Restore', 'Activate', 'Assign']
        ep2 = doubled_endpoint(HOSTS.LocalHostEndpoint, services)
        ep2._build_launch = ep._build_launch
        def fail_stage(*a):
            raise DataflowError('RESOURCE_INFEASIBLE', 'spawn preparation failed')
        ep2._connector.stage = fail_stage
        with pytest.raises(DataflowError):
            ep2.prepare(assignment)
        assert not ep2._activated and not ep2._provider.absent_calls


def test_h017(monkeypatch):
    import socket
    from collections import deque
    import demiflow.execution.worker.bootstrap as B
    ep = HOSTS.LocalHostEndpoint.__new__(HOSTS.LocalHostEndpoint)
    ev = mk_event('e')
    payload = json.loads(codec.encode_wire(ev, type_name='WorkerEvent', bounds=BIG))['payload']
    sender, ep._sock = socket.socketpair()
    ep._control_reader = B.ControlFrameReader(ep._sock, timeout_s=1)
    ep._services = NS(bounds=BIG, policy=POLICY)
    ep._events = deque()
    try:
        B.send_messages(sender, [{'type': 'worker_event', 'event': payload} for _ in range(5)])
        got = ep.poll(max_events=3, deadline_ms=100)
        assert len(got) == 3 and got[0].operation_id == ev.operation_id
        assert len(ep.poll(max_events=3, deadline_ms=100)) == 2
        assert ep.poll(max_events=3, deadline_ms=100) == ()
        sender.close()
        with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
            ep.poll(max_events=3, deadline_ms=100)
    finally:
        sender.close(); ep._sock.close()


def test_h018():
    with driver_env('h018') as e:
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST,))
        ep = doubled_endpoint(HOSTS.LocalHostEndpoint, services)
        cut = NS(cut_id='cut-1', sequence=1, plan_revision=1, route_epoch=0, participants=('s:p0',), final=False)
        with pytest.raises(DataflowError) as ei:
            ep.request_cut(cut)
        assert ei.value.code == 'FENCED'
        ep._activated = True
        ep.request_cut(cut)
        ep.request_cut(cut)
        assert len(ep._connector.cuts) == 1
        conflict = NS(**{**cut.__dict__, 'sequence': 2})
        with pytest.raises(DataflowError) as ei:
            ep.request_cut(conflict)
        assert ei.value.code == 'INVALID_CONTRACT'


def test_h019():
    with driver_env('h019') as e:
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST,))
        ep = doubled_endpoint(HOSTS.LocalHostEndpoint, services, phase='RUNNING')
        ep._scope_id = 'scope-1'
        sent = []
        ep._send_k3 = lambda kind, payload: sent.append((kind, payload)) or {}
        ep.cancel('done', deadline_ms=100)
        assert sent[0][0] == 'Cancel' and sent[0][1]['reason'] == 'done'
        assert not ep.closed()  # cancel receipt alone cannot free physical capacity
        assert ep._provider.absent_calls == []  # cleanup belongs to the driver allocator
        ep._manager.phase = 'TERMINATED'
        assert ep.closed()  # observed CONFIRMED, not an optimistic endpoint flag


def test_h020():
    with driver_env('h020') as e:
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST,))
        ep = doubled_endpoint(HOSTS.LocalHostEndpoint, services, phase='RUNNING')
        ep._scope_id = 'scope-1'
        assert ep.closed() is False
        ep._manager = FakeManager('TERMINATED')
        assert ep.closed() is True


def test_h021(monkeypatch):
    with driver_env('h021') as e:
        rayh = df.HostDescriptor(host_id='local', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(rayh,))
        ep = doubled_endpoint(HOSTS.RayHostEndpoint, services)
        # This is the K3 staging-order unit, with no artifact store/real program.
        # Credential selection is exercised through real Ray native API cases.
        import demiflow.execution.dataflow_environment as envs
        services.store = object()
        selected = []
        monkeypatch.setattr(envs, 'allocation_environment',
            lambda assignment, **kwargs: selected.append(assignment) or {})
        ep._connector.stage_environment = lambda allocation_id, environment: None
        ep._build_launch = lambda a: ({'dataflow_v3': {}}, {'grant_id': 'g', 'scope_id': 'scope-1'}, {'sha256': oid('pin')})
        ep._open_scope = lambda *a: None
        ep._apply = lambda *a: None
        monkeypatch.setattr(WH, 'make_worker_control_assignment', lambda *a, **k: {'restore_point': None})
        assignment = mkassignment()
        ep._send_k3 = lambda kind, payload: ep._connector.handled.append(kind) or {}
        with pytest.raises(DataflowError, match='staged'):
            ep.activate(assignment)
        ep.prepare(assignment)
        observed = {'phase':'RUNNING', 'boot_id':ep._boot_id, 'allocation_id':ep._allocation_id,
                    'effective_resources':{'cpu_millis':1000, 'memory_bytes':1048576}}
        selected_binding = [None]
        services.resources.state.read_allocation_identity = lambda scope_id, allocation_id: selected_binding[0]
        for invalid in (None, {**observed, 'phase':'PENDING'}, {**observed, 'boot_id':'other'}):
            selected_binding[0] = None if invalid is None else {'spec_hash':oid('resource-spec'), 'observation':invalid}
            with pytest.raises(DataflowError, match='running allocation'):
                ep.activate(assignment)
            assert not ep._connector.handled
        selected_binding[0] = {'spec_hash':oid('resource-spec'), 'observation':observed}
        ep.activate(assignment)
        assert ep._allocation_envelope == {'allocation_id':ep._allocation_id,
            'spec_hash':oid('resource-spec'), 'cpu_millis':1000, 'memory_bytes':1048576,
            'resource_directory':str(e.ctx.directory/'worker-cache'/ep._allocation_id)}
        assert ep._activated
        assert ep._connector.handled == ['VerifyArtifactAndCapabilities', 'Restore', 'Activate', 'Assign']
        assert selected == [assignment]


def test_h022():
    with driver_env('h022') as e:
        rayh = df.HostDescriptor(host_id='local', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(rayh,))
        ep = doubled_endpoint(HOSTS.RayHostEndpoint, services)
        ev = mk_event('e')
        ep._connector.pulled = [ev, ev, ev]
        got = ep.poll(max_events=2, deadline_ms=100)
        assert len(got) == 2

        class DeadConn(FakeConn):
            def pull(self, aid, limit):
                raise HOSTS.RayActorError('actor died')

        ep._connector = DeadConn()
        with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
            ep.poll(max_events=2, deadline_ms=100)


def test_h023():
    with driver_env('h023') as e:
        rayh = df.HostDescriptor(host_id='local', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(rayh,))
        ep = doubled_endpoint(HOSTS.RayHostEndpoint, services)
        cut = NS(cut_id='cut-1', sequence=1, plan_revision=1, route_epoch=0, participants=('s:p0',), final=False)
        with pytest.raises(DataflowError) as ei:
            ep.request_cut(cut)
        assert ei.value.code == 'FENCED'
        ep._activated = True
        ep.request_cut(cut)
        ep.request_cut(cut)
        assert len(ep._connector.cuts) == 1


def test_h024():
    with driver_env('h024') as e:
        rayh = df.HostDescriptor(host_id='local', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(rayh,))
        ep = doubled_endpoint(HOSTS.RayHostEndpoint, services)
        ep._scope_id = 'scope-1'
        ep._send_k3 = lambda kind, payload: {}
        ep.cancel('actor died', deadline_ms=100)
        assert not ep._closed_flag and ep._provider.absent_calls == []
        assert ep.closed()  # only allocator-confirmed exit releases capacity


def test_h025():
    with driver_env('h025') as e:
        rayh = df.HostDescriptor(host_id='local', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(rayh,))
        ep = doubled_endpoint(HOSTS.RayHostEndpoint, services, phase='RUNNING')
        ep._scope_id = 'scope-1'
        assert ep.closed() is False
        ep._manager = FakeManager('TERMINATED')
        assert ep.closed() is True


def test_h028():
    with driver_env('h028') as e:
        resources = FakeManager()
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST,), resources=resources)
        ep = HOSTS.LocalHostEndpoint(mklease('local'), services=services)
        ep2 = HOSTS.LocalHostEndpoint(mklease('local'), services=services)
        assert not ep._activated and ep._scope_id is None
        assert ep2._manager is ep._manager is resources
        assert ep2._connector is ep._connector is resources.provider._control
        assert ep2._events is not ep._events and ep2._cuts is not ep._cuts


def test_h029():
    with driver_env('h029') as e:
        rayh = df.HostDescriptor(host_id='local', provider='ray_core', runtime_revision='r',
                                 store_domains=('store-1',), logical_resources={}, isolation_capabilities=())
        resources = FakeManager()
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(rayh,), resources=resources)
        ep = HOSTS.RayHostEndpoint(mklease('local'), services=services)
        ep2 = HOSTS.RayHostEndpoint(mklease('local'), services=services)
        assert ep._activated is False and ep._scope_id is None
        assert ep._manager is ep2._manager is resources
        assert ep._connector is ep2._connector is resources.provider._control
        assert ep._events is not ep2._events  # mailbox state remains endpoint-local


# ===== H026-H027 services =====

def test_h026():
    tmp = newtmp()
    with RunContext.start(directory=tmp / 'a', limits={'checkpoint_rows': 2, 'max_requests': 10}) as ctx1:
        a1 = RunAction(ctx1, 'action-a', {'kind': 'literal_empty', 'tag': 'a'})
        s1 = SVC.make_driver_services(a1, policy=POLICY, bounds=BIG)
        assert isinstance(s1.journal_service, AUTH.DriverJournalService)
        assert s1.store.store_id == 'store-1' and s1.hosts[0].host_id == 'local'
        assert isinstance(s1.resources, ResourceManager)
        assert len(list(ctx1.directory.rglob('*.sqlite'))) == 1
        root1 = s1.journal_service._journal.directory
        js1 = s1.journal_service
        js1.close(deadline_ms=BIG.timeout_ms)
    tmp2 = newtmp()
    with RunContext.start(directory=tmp2 / 'b', limits={'checkpoint_rows': 2, 'max_requests': 10}) as ctx2:
        a2 = RunAction(ctx2, 'action-b', {'kind': 'literal_empty', 'tag': 'b'})
        s2 = SVC.make_driver_services(a2, policy=POLICY, bounds=BIG)
        assert s2.journal_service is not js1
        assert s2.journal_service._journal.directory != root1
        assert s2.journal_service._journal is ctx2.journal
        assert s2.journal_service._closed is False
        assert len(list(ctx2.directory.rglob('*.sqlite'))) == 1
        s2.journal_service.close(deadline_ms=BIG.timeout_ms)


def test_h027(monkeypatch):
    tmp = newtmp()
    with RunContext.start(directory=tmp / 'run', limits={'checkpoint_rows': 2, 'max_requests': 10}) as ctx:
        action = RunAction(ctx, 'action-a', {'kind': 'literal_empty'})
        s1 = SVC.make_driver_services(action, policy=POLICY, bounds=BIG)
        prog_raw = json.dumps({'name': 'mod'}).encode()
        program_ref = s1.store.put(iter([prog_raw]), max_bytes=1 << 20, media_type='application/json')
        constants_ref = s1.store.put(iter([json.dumps({'k': 1}).encode()]), max_bytes=1 << 20, media_type='application/json')
        placeholder = aref('pin')
        sealed_stage = df.SealedExecution(logical_ref=placeholder, physical_ref=placeholder, programs=(program_ref,),
                                          code_pin_ref=placeholder, identity_bridge_ref=placeholder, source_refs=(),
                                          constants_ref=constants_ref)
        bundle_ref = ART.stage_runtime_bundle(sealed_stage, store=s1.store, bounds=BIG)
        grant_doc = {'run_id': ctx.ref.run_id, 'attempt': ctx.attempt, 'fence': ctx.fence,
                     'plan_revision': 2, 'controller_epoch': ctx.attempt, 'scope_id': 'scope-1'}
        grant_ref = s1.store.put(iter([json.dumps(grant_doc).encode()]), max_bytes=1 << 20, media_type='application/json')
        lease = df.WorkerLease(allocation_id='alloc-1', boot_id='boot-1', stage_id='action-a', partition_ids=('p0',),
                               host_id='local', endpoint_ref='ep', grant_ref=grant_ref, expires_at_ms=10 ** 13)
        plan_id = oid('plan')
        sealed_assign = df.SealedExecution(logical_ref=placeholder, physical_ref=placeholder, programs=(program_ref,),
                                           code_pin_ref=bundle_ref, identity_bridge_ref=placeholder, source_refs=(),
                                           constants_ref=constants_ref)
        D.SEAL_STATE.update(sealed=sealed_assign, plan_id=plan_id, plan_revision=2, program_ref=program_ref)
        assignment = df.WorkerAssignment(schema='demiflow.dataflow_assignment.v3', run_id=ctx.ref.run_id,
                                         action_id='action-a', attempt=ctx.attempt, fence=ctx.fence, plan_id=plan_id,
                                         plan_revision=2, lease=lease, sealed=sealed_assign, stages=(), inputs=(),
                                         outputs=(), source_splits=(), restore_refs=(), bounds=BIG)

        class FakeTransport:
            def __init__(self, store):
                self.store = store
                self._closed = False
                self.requests = []

            def check_stop(self):
                if self._closed:
                    raise DataflowError('CANCELLED', 'closed')

            def request(self, event, *, deadline_ms):
                self.requests.append(event.kind)
                return ControlReply(operation_id=event.operation_id, status='ok', payload={}, payload_ref=None, error_code=None)

            def close(self):
                self._closed = True

        cache = newtmp() / 'worker-cache'
        assert not (cache / 'run.sqlite').exists()
        transport = FakeTransport(s1.store)
        def load_programs(given, *, store, bounds, cache):
            assert given is assignment and store.store_id == 'store-1'
            return {oid('occ'): df.LoadedProgram('callable', {'digest': program_ref.digest}, program_ref.digest)}
        monkeypatch.setattr(SVC, '_load_programs', load_programs)
        rs = SVC.make_runtime_services(assignment, transport=transport, cache=cache)
        assert rs.store.store_id == 'store-1' and rs.constants == {'k': 1}
        occ = hashlib.sha256(b'occ').hexdigest()
        assert occ in rs.programs and rs.programs[occ].digest == program_ref.digest
        assert 'check_grant' in transport.requests
        assert not list(cache.rglob('*.sqlite'))
        rs.check_stop()
        transport.close()
        with pytest.raises(DataflowError) as ei:
            rs.check_stop()
        assert ei.value.code == 'CANCELLED'
        s1.journal_service.close(deadline_ms=BIG.timeout_ms)


# ===== H030-H035 store =====

def test_h030():
    tmp = newtmp()
    store = make_store(tmp)
    assert store.store_id == 'store-1' and store.root.is_absolute()
    link = tmp / 'link'
    link.symlink_to(tmp / 'store', target_is_directory=True)
    with pytest.raises(DataflowError) as ei:
        RunArtifactStore(root=link, store_id='s', journal=FakePort(), bounds=BIG)
    assert ei.value.code == 'INVALID_CONTRACT'
    assert not (tmp / 'store' / 'run.sqlite').exists()


def test_h031():
    tmp = newtmp()
    store = make_store(tmp)
    data = b'hello artifact world' * 100
    ref = store.put(iter([data[:500], data[500:]]), max_bytes=1 << 20, media_type='application/octet-stream')
    assert ref.digest == hashlib.sha256(data).hexdigest() and ref.key == ref.digest
    ref2 = store.put(iter([data]), max_bytes=1 << 20, media_type='application/octet-stream')
    assert ref2 == ref
    assert len(list((tmp / 'store' / 'store-1' / 'objects').iterdir())) == 1

    # The declared ceiling may exceed the allowance; actual staged growth is
    # charged before writing. Exercise the real authority, not an unmetered port.
    with driver_env('h031-budget') as e:
        small = Bounds(max_spill_bytes=64)
        port = AUTH.DriverArtifactPort(e.action, bounds=small)
        bounded = RunArtifactStore(root=e.ctx.directory / '.dataflow-store',
                                   store_id='bounded', journal=port, bounds=small)
        with pytest.raises(DataflowError) as ei:
            bounded.put(iter([b'x' * 32, b'y' * 40]), max_bytes=128, media_type='x')
        assert ei.value.code == 'BUDGET_EXCEEDED'
        assert not list((bounded.root / 'bounded' / 'staging').iterdir())


def test_h032():
    tmp = newtmp()
    store = make_store(tmp)
    data = b'payload' * 100
    ref = store.put(iter([data]), max_bytes=1 << 20, media_type='x')
    assert b''.join(store.read(ref, max_bytes=1 << 20)) == data
    with pytest.raises(DataflowError) as ei:
        list(store.read(ref, max_bytes=10))
    assert ei.value.code == 'BUDGET_EXCEEDED'
    tampered = ArtifactRef(digest=ref.digest, size_bytes=ref.size_bytes + 1, store_id='store-1',
                           key=ref.digest, media_type='x')
    with pytest.raises(DataflowError):
        list(store.read(tampered, max_bytes=1 << 20))


def test_h033():
    tmp = newtmp()
    store = make_store(tmp)
    ref = store.put(iter([b'x']), max_bytes=100, media_type='x')
    assert store.inspect(ref) is True
    assert store.inspect(aref('absent')) is False
    with pytest.raises(DataflowError) as ei:
        store.inspect(ArtifactRef(digest=ref.digest, size_bytes=ref.size_bytes, store_id='other',
                                  key=ref.digest, media_type='x'))
    assert ei.value.code == 'INVALID_CONTRACT'


def test_h034():
    tmp = newtmp()
    port = FakePort()
    store = RunArtifactStore(root=tmp / 'store', store_id='store-1', journal=port, bounds=BIG)
    ref = store.put(iter([b'x']), max_bytes=100, media_type='x')
    store.retain(ref, owner='owner-1')
    store.retain(ref, owner='owner-1')
    assert ('owner-1' in [k[1] for k in port.pins])
    assert len([k for k in port.pins if k[1] == 'owner-1']) == 1


def test_h035():
    tmp = newtmp()
    port = FakePort()
    store = RunArtifactStore(root=tmp / 'store', store_id='store-1', journal=port, bounds=BIG)
    ref = store.put(iter([b'x']), max_bytes=100, media_type='x')
    store.retain(ref, owner='owner-1')
    store.retain(ref, owner='owner-2')
    store.release(ref, owner='owner-1')
    store.release(ref, owner='owner-1')
    assert (ref.digest, 'owner-2') in port.pins
    assert (ref.digest, 'owner-1') not in port.pins


# ===== H036-H037, H056-H057, H066-H075 authority =====

def test_h036():
    with driver_env('h036') as e:
        svc = e.svc
        ref = e.store.put(iter([b'x']), max_bytes=100, media_type='x')
        svc.artifact(df.ArtifactCommand(operation_id=oid('p1'), kind='pin', ref=ref, owner='owner-1', size_bytes=ref.size_bytes), lease=None)
        rep = svc.artifact(df.ArtifactCommand(operation_id=oid('i1'), kind='inspect', ref=ref, owner='owner-1', size_bytes=0), lease=None)
        assert rep.present is True
        rep2 = svc.artifact(df.ArtifactCommand(operation_id=oid('i2'), kind='inspect', ref=aref('gone'), owner='owner-1', size_bytes=0), lease=None)
        assert rep2.present is False
        with pytest.raises(DataflowError):
            svc.artifact(df.ArtifactCommand(operation_id=oid('bad'), kind='steal', ref=ref, owner='owner-2', size_bytes=0), lease=None)


def test_h037():
    client, t, lease = worker_client(replies={'artifact': {'operation_id': 'a1', 'present': True, 'charged_bytes': 0}})
    cmd = df.ArtifactCommand(operation_id=oid('a1'), kind='pin', ref=aref('x'), owner='o', size_bytes=1)
    assert client.artifact(cmd).present is True
    assert t.requests[0].kind == 'artifact'
    assert client.artifact(cmd).charged_bytes == 0


def test_h056():
    with driver_env('h056') as e:
        svc = e.svc
        binding = df.QueueBinding(action_id='action-1', source_id=None, mode='raw_fetch', contract_digest='d' * 64,
                                  generation=None, pool=None, source_occurrence=None, fetch_occurrence=oid('f'), final_occurrence=None)
        with pytest.raises(DataflowError) as ei:
            svc.queue(df.QueueCommand(operation_id=oid('q1'), operation='drop_table', binding=binding, record_id=None, delivery_ref=None, row_ref=None), lease=None)
        assert ei.value.code == 'INVALID_CONTRACT'
        with pytest.raises(DataflowError) as ei:
            svc.queue(df.QueueCommand(operation_id=oid('q2'), operation='poll', binding=binding, record_id=None, delivery_ref=None, row_ref=None), lease=None)
        assert ei.value.code == 'INVALID_CONTRACT'
        native = NS(action_id='action-1', poll=lambda cmd, *, lease: df.QueueReply(operation_id=cmd.operation_id, status='pending', delivery_ref=None, row_ref=None, native_fact_ref=None, retry_after_ms=0))
        svc.bind_queue(native)
        rep = svc.queue(df.QueueCommand(operation_id=oid('q3'), operation='poll', binding=binding, record_id=None, delivery_ref=None, row_ref=None), lease=None)
        assert rep.status == 'pending'


def test_h057():
    client, t, lease = worker_client(replies={'queue': {'operation_id': 'q1', 'status': 'confirmed', 'delivery_ref': None, 'row_ref': None, 'native_fact_ref': None, 'retry_after_ms': 0}})
    binding = df.QueueBinding(action_id='action-1', source_id=None, mode='raw_fetch', contract_digest='d' * 64,
                              generation=None, pool=None, source_occurrence=None, fetch_occurrence=oid('f'), final_occurrence=None)
    cmd = df.QueueCommand(operation_id=oid('q1'), operation='finalize', binding=binding, record_id=oid('r1'), delivery_ref=aref('delivery'), row_ref=aref('row'))
    assert client.queue(cmd).status == 'confirmed'
    assert t.requests[0].kind == 'queue'
    client2, t2, _ = worker_client(error=DataflowError('UNKNOWN_EFFECT', 'lost'))
    with pytest.raises(DataflowError) as ei:
        client2.queue(cmd)
    assert ei.value.code == 'UNKNOWN_EFFECT'


def test_h066():
    with driver_env('h066') as e:
        svc = e.svc
        assert svc.submit(dev(e.ctx, 'reserve', 's1', {'amounts': {'requests': 1}})) is True
        assert svc.submit(dev(e.ctx, 'reserve', 's1', {'amounts': {'requests': 1}})) is True
        assert svc._queue.maxsize == BIG.max_pending_events
        svc.close(deadline_ms=100)
        assert svc.submit(dev(e.ctx, 'reserve', 's2', {'amounts': {'requests': 1}})) is False


def test_h067():
    with driver_env('h067') as e:
        svc = e.svc
        svc.submit(dev(e.ctx, 'reserve', 'p1', {'amounts': {'requests': 1}}))
        deadline = time.time() + 2
        replies = ()
        while time.time() < deadline:
            replies = svc.poll_replies(max_replies=8)
            if replies:
                break
            time.sleep(0.01)
        assert len(replies) == 1 and replies[0].reply.status == 'ok'
        with pytest.raises(DataflowError):
            svc.poll_replies(max_replies=0)
        assert svc.poll_replies(max_replies=8) == ()


def test_h068():
    with driver_env('h068') as e:
        svc = e.svc
        svc.submit(dev(e.ctx, 'reserve', 'c1', {'amounts': {'requests': 1}}))
        svc.close(deadline_ms=200)
        assert svc._closed
        svc.close(deadline_ms=200)
        assert svc.submit(dev(e.ctx, 'reserve', 'c2', {'amounts': {'requests': 1}})) is False


def test_h069():
    with driver_env('h069') as e:
        svc = e.svc
        with pytest.raises(DataflowError) as ei:
            cmd = df.NativeJournalCommand(operation_id=oid('n1'), operation='drop_table', action_id='action-1', node_id='node-1',
                                          request_key='k', request_ref=aref('req'), value_ref=None, reservation=None, options={})
        assert ei.value.code == 'INVALID_CONTRACT'
        cmd2 = df.NativeJournalCommand(operation_id=oid('n2'), operation='lookup', action_id='other', node_id='node-1',
                                       request_key='k', request_ref=aref('req'), value_ref=None, reservation=None, options={})
        with pytest.raises(DataflowError) as ei:
            svc.native_request(cmd2, lease=None)
        assert ei.value.code == 'FENCED'


def test_h070():
    client, t, lease = worker_client(replies={'native_request': {'operation_id': 'n1', 'value': {'ok': True}, 'value_ref': None}})
    cmd = df.NativeJournalCommand(operation_id=oid('n1'), operation='lookup', action_id='action-1', node_id='node-1',
                                  request_key='k', request_ref=aref('req'), value_ref=None, reservation=None, options={})
    assert client.native_request(cmd).value == {'ok': True}
    assert t.requests[0].kind == 'native_request'
    client2, t2, _ = worker_client(error=DataflowError('UNKNOWN_EFFECT', 'lost'))
    with pytest.raises(DataflowError) as ei:
        client2.native_request(cmd)
    assert ei.value.code == 'UNKNOWN_EFFECT'


def test_h071():
    with driver_env('h071') as e:
        port = e.port
        for forbidden in ('queue', 'effect', 'complete', 'submit', 'dispatch'):
            assert not hasattr(port, forbidden)
        assert hasattr(port, 'reserve') and hasattr(port, 'artifact')


def test_h072():
    with driver_env('h072') as e:
        port = e.port
        port.reserve('st-1', {ARTIFACT_STAGING_BUDGET: 100})
        with pytest.raises(DataflowError):
            port.reserve('st-2', {'other_budget': 100})
        with pytest.raises(DataflowError):
            port.reserve('st-3', {ARTIFACT_STAGING_BUDGET: -1})


def test_h073():
    with driver_env('h073') as e:
        ref = e.store.put(iter([b'x']), max_bytes=100, media_type='x')
        rep = e.port.artifact(df.ArtifactCommand(operation_id=oid('pin-1'), kind='pin', ref=ref, owner='owner-1', size_bytes=ref.size_bytes))
        assert rep.present is True
        with pytest.raises(DataflowError):
            e.port.artifact(df.ArtifactCommand(operation_id=oid('bad'), kind='drop', ref=ref, owner='o', size_bytes=0))


def test_h074():
    with driver_env('h074') as e:
        ref = e.store.put(iter([b'x']), max_bytes=100, media_type='x')
        cmd = df.ArtifactCommand(operation_id=oid('pin-1'), kind='pin', ref=ref, owner='owner-1', size_bytes=ref.size_bytes)
        assert AUTH.apply_artifact_command(e.action, cmd, bounds=BIG).present is True
        assert AUTH.apply_artifact_command(e.action, cmd, bounds=BIG).present is True
        with pytest.raises(DataflowError):
            AUTH.apply_artifact_command(e.action, df.ArtifactCommand(operation_id=oid('x'), kind='unknown', ref=ref, owner='o', size_bytes=0), bounds=BIG)
        rs = AUTH.apply_artifact_command(e.action, df.ArtifactCommand(operation_id=oid('rs-1'), kind='reserve_staging', ref=None, owner='o', size_bytes=100), bounds=BIG)
        assert rs.charged_bytes == 100


def test_h075():
    with driver_env('h075') as e:
        svc = e.svc
        native = NS(action_id='action-1')
        svc.bind_queue(native)
        svc.bind_queue(native)
        with pytest.raises(DataflowError):
            svc.bind_queue(NS(action_id='other'))
        with pytest.raises(DataflowError):
            svc.bind_queue(NS(action_id='action-1', different=True))
        svc.close(deadline_ms=100)
        with pytest.raises(DataflowError):
            svc.bind_queue(native)


# ===== H038-H044, H049-H053, H060 worker host =====

def test_h038():
    w = new_worker()
    assert w.identity() == {'spec_hash': 'spec-1', 'boot_id': 'boot-1'}
    assert w.pull_events(10) == ()  # K3 lifecycle is not a scheduling-ready event
    evs = w.pull_control(10)
    assert len(evs) == 1 and evs[0]['kind'] == 'Register'
    assert not hasattr(w, '_journal')


def test_h039():
    w = new_worker()
    w.activate(mkassignment())
    assert w._activated and isinstance(w._executor, FakeExecutor)
    assert w._runtime.plan_revision == 2


def test_h040():
    w = new_worker()
    for i in range(5):
        w._runtime._control({'kind': 'Heartbeat', 'operation_id': oid('hb%d' % i), 'run_id': 'run-1',
                             'controller_epoch': 1, 'allocation_id': 'alloc-1', 'boot_id': 'boot-1',
                             'scope_id': 'action-1', 'payload': {}})
    assert len(w.pull_control(3)) == 3
    assert len(w.pull_control(10)) == 3
    assert w.poll(max_events=3, deadline_ms=100) == ()


def test_h041():
    w = new_worker()
    cut = NS(cut_id='cut-1', sequence=1, plan_revision=2, route_epoch=0, participants=('stage-a:p0',), final=False)
    with pytest.raises(DataflowError) as ei:
        w.request_cut(cut)
    assert ei.value.code == 'FENCED'
    w.activate(mkassignment())
    w.request_cut(cut)
    assert w._executor.cuts == [cut]


def test_h042():
    w = new_worker()
    w.activate(mkassignment())
    w.cancel('test', deadline_ms=100)
    assert w._executor.cancelled == ('test', 100)
    assert w._cancel_event.is_set()


def test_h043():
    w = new_worker()
    assert w.closed() is False
    w.activate(mkassignment())
    assert w.closed() is False
    w._executor._closed = True
    assert w.closed() is True


def test_h044():
    ctrl = WH.make_worker_control_assignment(mkassignment(), grant={'grant_id': 'grant-1', 'scope_id': 'action-1'},
                                             manifest_ref={'manifest_id': oid('pin'), 'sha256': oid('pin')},
                                             ready_deadline_ms=60000)
    assert ctrl['schema_version'] == 'demiflow.worker_assignment.v1'
    assert ctrl['stages'] == ['stage-a', 'stage-b']
    from demiflow.execution.recovery_contract import identity
    assert ctrl['partitions'] == [identity(['dataflow-k3-partition-v3', p]) for p in ('p0', 'p1')]
    assert mkassignment().lease.partition_ids == ('p0', 'p1')
    assert ctrl['channels'] == {'inputs': [{'channel_id': 'in-0'}], 'outputs': ['out-0', 'out-1']}
    assert ctrl['grant_ref'] == 'grant-1' and ctrl['controller_epoch'] == 1
    assert ctrl['plan_revision'] == 2 and ctrl['expected_plan_revision'] == 2
    assert ctrl['restore_point'] == {'checkpoint_id': oid('cp')}
    a2 = NS(**{**mkassignment().__dict__, 'restore_refs': ()})
    assert WH.make_worker_control_assignment(a2, grant={'grant_id': 'g', 'scope_id': 'action-1'}, manifest_ref={'manifest_id': 'm', 'sha256': 'm'}, ready_deadline_ms=1)['restore_point'] is None


def test_h049():
    w = new_worker()
    assert w.identity() == {'spec_hash': 'spec-1', 'boot_id': 'boot-1'}
    w2 = WH.HostedDataflowWorker(mklaunch(), spec_hash='spec-2', boot_id='boot-2')
    assert w2.identity() != w.identity()


def test_h050():
    import uuid
    w = new_worker()
    r = w.handle({'kind': 'Heartbeat', 'payload': {'boot_id': 'boot-1', 'observed_at_ms': 1}, 'scope_id': 'action-1',
                  'run_id': 'run-1', 'operation_id': uuid.uuid4().hex, 'controller_epoch': 1, 'authority_ref': 'grant-1',
                  'allocation_id': 'alloc-1', 'boot_id': 'boot-1', 'protocol_version': {'major': 1, 'minor': 0}})
    assert 'receipt' in r
    w.activate(mkassignment())
    r2 = w.handle({'response_to': 'op', 'receipt': {'boot_id': 'boot-1', 'commit_id': 'c', 'cut_id': 'cut'}})
    assert r2 == {'receipt': None}


def test_h051():
    w = new_worker()
    for i in range(6):
        w._runtime._control({'kind': 'Heartbeat', 'operation_id': oid('e%d' % i), 'run_id': 'run-1',
                             'controller_epoch': 1, 'allocation_id': 'alloc-1', 'boot_id': 'boot-1',
                             'scope_id': 'action-1', 'payload': {}})
    assert len(w.pull_control(3)) == 3
    assert w.pull_events(3) == ()
    with pytest.raises(DataflowError):
        w.pull_control(0)


def test_h052():
    w = new_worker()
    w.activate(mkassignment())
    with pytest.raises(DataflowError) as ei:
        w.deliver_receipt({'boot_id': 'other', 'commit_id': 'c', 'cut_id': 'cut-1'})
    assert ei.value.code == 'FENCED'
    w.deliver_receipt({'boot_id': 'boot-1', 'commit_id': 'commit-1', 'cut_id': 'cut-1'})
    assert w._executor.receipts == [('commit-1', 'cut-1')]


def test_h053():
    w = new_worker()
    w.activate(mkassignment())
    w.close()
    w.close()
    assert w._runtime.closed and w.closed() is True
    assert len(w._control_outbox) == 0


def test_h060():
    w = new_worker()
    w.deliver_control_reply(mk_reply('unknown-op'))
    ev = mk_event('op1')
    result = {}

    def req():
        try:
            result['r'] = w._mailbox.request(ev, deadline_ms=2000)
        except DataflowError as exc:
            result['e'] = exc

    th = threading.Thread(target=req)
    th.start()
    time.sleep(0.1)
    w.deliver_control_reply(ControlReply(operation_id=ev.operation_id, status='ok', payload={'r': 1}, payload_ref=None, error_code=None))
    th.join(timeout=2)
    assert result['r'].operation_id == ev.operation_id


# ===== H045-H048, H061-H065 control =====

def test_h045():
    conn = FakeConnection()
    t = CTRL.LocalControlTransport(conn, lease=mklease(), bounds=BIG)
    conn.inbuf = frame(mk_reply('op1'), 'ControlReply')
    r = t.request(mk_event('op1'), deadline_ms=1000)
    assert r.operation_id == oid('op1')
    conn.inbuf = (BIG.max_control_bytes + 1).to_bytes(4, 'big') + b'x'
    with pytest.raises(DataflowError) as ei:
        t.request(mk_event('op2'), deadline_ms=1000)
    assert ei.value.code == 'BUDGET_EXCEEDED'
    conn.inbuf = b''
    with pytest.raises(DataflowError) as ei:
        t.request(mk_event('op3'), deadline_ms=50)
    assert ei.value.code == 'UNKNOWN_EFFECT'


def test_h046():
    conn = FakeConnection()
    t = CTRL.LocalControlTransport(conn, lease=mklease(), bounds=BIG)
    conn.inbuf = frame(mk_reply('OTHER'), 'ControlReply')
    with pytest.raises(DataflowError) as ei:
        t.request(mk_event('op1'), deadline_ms=1000)
    assert ei.value.code == 'INVALID_CONTRACT'
    with pytest.raises(DataflowError) as ei:
        t.request(mk_event('op2', alloc='other'), deadline_ms=1000)
    assert ei.value.code == 'FENCED'
    t.close()
    with pytest.raises(DataflowError):
        t.check_stop()


def test_h047():
    mb = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    rt = CTRL.RayControlTransport(mb, bounds=BIG)
    assert rt._mailbox is mb and rt.store is None
    assert not hasattr(rt, '_journal') and not hasattr(rt, '_connection')


def test_h048():
    mb = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    rt = CTRL.RayControlTransport(mb, bounds=BIG)
    ev = mk_event('op1')

    def driver():
        for _ in range(400):
            evs = mb.drain(max_events=8)
            if evs:
                for e in evs:
                    mb.deliver(ControlReply(operation_id=e.operation_id, status='ok', payload={'r': 1}, payload_ref=None, error_code=None))
                return
            time.sleep(0.005)

    th = threading.Thread(target=driver)
    th.start()
    r = rt.request(ev, deadline_ms=2000)
    th.join()
    assert r.operation_id == ev.operation_id and r.status == 'ok'
    rt.close()
    with pytest.raises(DataflowError):
        rt.check_stop()


def test_h061():
    mb = HOSTS.ControlMailbox(bounds=Bounds(max_pending_events=2), check_stop=lambda: None)
    ths = []

    def req(e):
        try:
            mb.request(e, deadline_ms=3000)
        except DataflowError:
            pass

    for i in range(2):
        ths.append(threading.Thread(target=req, args=(mk_event('b%d' % i),)))
        ths[-1].start()
    time.sleep(0.1)
    with pytest.raises(DataflowError) as ei:
        mb.request(mk_event('b2'), deadline_ms=100)
    assert ei.value.code == 'BUDGET_EXCEEDED'
    mb.close()
    for t in ths:
        t.join(timeout=2)


def test_h062():
    mb = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    with pytest.raises(DataflowError) as ei:
        mb.request(mk_event('timeout'), deadline_ms=50)
    assert ei.value.code == 'UNKNOWN_EFFECT'

    def stopped():
        raise DataflowError('CANCELLED', 'stopped')

    mb2 = HOSTS.ControlMailbox(bounds=BIG, check_stop=stopped)
    with pytest.raises(DataflowError) as ei:
        mb2.request(mk_event('c'), deadline_ms=100)
    assert ei.value.code == 'CANCELLED'


def test_h063():
    mb = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    ths = []

    def req(e):
        try:
            mb.request(e, deadline_ms=3000)
        except DataflowError:
            pass

    for i in range(3):
        ths.append(threading.Thread(target=req, args=(mk_event('m%d' % i),)))
        ths[-1].start()
    time.sleep(0.1)
    batch = mb.drain(max_events=2)
    assert len(batch) == 2
    assert len(mb.drain(max_events=8)) == 1
    for e in batch:
        mb.deliver(mk_reply(e.operation_id))
    mb.deliver(mk_reply('UNKNOWN'))
    mb.close()
    for t in ths:
        t.join(timeout=2)
    with pytest.raises(DataflowError):
        mb.drain(max_events=0)


def test_h064():
    mb = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    result = {}

    def req():
        try:
            result['r'] = mb.request(mk_event('op1'), deadline_ms=2000)
        except DataflowError as exc:
            result['e'] = exc

    th = threading.Thread(target=req)
    th.start()
    time.sleep(0.1)
    mb.deliver(mk_reply('wrong-op'))
    assert 'r' not in result
    mb.deliver(mk_reply('op1'))
    th.join(timeout=2)
    assert result['r'].status == 'ok'
    mb2 = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    waiter = HOSTS._Waiter(oid('opX'), 'digest')
    waiter.done = True
    waiter.reply = mk_reply('opX')
    mb2._pending[oid('opX')] = waiter
    with pytest.raises(DataflowError) as ei:
        mb2.deliver(ControlReply(operation_id=oid('opX'), status='error', payload=None, payload_ref=None, error_code='FENCED'))
    assert ei.value.code == 'INVALID_CONTRACT'


def test_h065():
    mb = HOSTS.ControlMailbox(bounds=BIG, check_stop=lambda: None)
    errors = []

    def req():
        try:
            mb.request(mk_event('op1'), deadline_ms=3000)
        except DataflowError as exc:
            errors.append(exc)

    th = threading.Thread(target=req)
    th.start()
    time.sleep(0.1)
    mb.close()
    th.join(timeout=2)
    assert errors and errors[0].code == 'UNKNOWN_EFFECT'


# ===== H054-H055, H058-H059 connectors / deliver =====

def test_h054():
    conn = object.__new__(HOSTS.DataflowRayConnector)
    host = conn.host_class()
    assert host.remote_options == {'max_restarts': 0, 'max_task_retries': 0, 'max_concurrency': 8}
    assert host._cls is WH.HostedDataflowWorker
    host.options(name='n', get_if_exists=True, num_cpus=1, memory=1, num_gpus=0).remote('launch', 'hash', 'boot')
    assert host.remote_calls == [('launch', 'hash', 'boot')]


def test_h055():
    c = HOSTS.DataflowLocalConnector()
    launch = {'directory': '/tmp', 'dataflow_v3': {'entrypoint': 'dataflow_worker_v3', 'bounds': dict(vars(BIG))}}
    c.stage('alloc-1', launch)
    payload = c.spawn_payload('alloc-1')
    assert 'extra_env' in payload and 'extra_pass_fds' in payload
    c.abort('alloc-1')
    assert 'alloc-1' not in c._pending and 'alloc-1' not in c._staged
    c.stage('alloc-2', {'directory': '/tmp', 'dataflow_v3': {'entrypoint': 'wrong', 'bounds': dict(vars(BIG))}})
    with pytest.raises(DataflowError) as ei:
        c.spawn_payload('alloc-2')
    assert ei.value.code == 'INVALID_CONTRACT'
    c.abort('alloc-2')
    with pytest.raises(DataflowError):
        c.stage('alloc-3', {'directory': '/tmp'})
    c.abort('alloc-3')


def test_h058():
    with driver_env('h058') as e:
        services = NS(action=e.action, policy=POLICY, bounds=BIG, hosts=(LOCAL_HOST,))
        ep = doubled_endpoint(HOSTS.LocalHostEndpoint, services)
        r = mk_reply('r')
        ep.deliver_reply(r)
        assert ep._connector.replies == [('alloc-1', r)]


def test_h059():
    ep = HOSTS.RayHostEndpoint.__new__(HOSTS.RayHostEndpoint)
    ep._allocation_id = 'a'
    ep._connector = FakeConn()
    r = mk_reply('r')
    ep.deliver_reply(r)
    assert ep._connector.replies == [('a', r)]
    assert not hasattr(ep._connector, 'journal')


# ===== H076-H079 run_journal artifact owners =====

def test_h076():
    with driver_env('h076') as e:
        j = e.ctx.journal
        fence = e.ctx.fence
        ref = aref('art-1')
        j.bind_artifact_owner(fence, ref, owner='owner-1', operation_id=oid('op-1'))
        j.bind_artifact_owner(fence, ref, owner='owner-1', operation_id=oid('op-1'))
        owners, _ = j.artifact_owners(ref, limit=10)
        assert owners == ('owner-1',)
        with pytest.raises(Exception):
            j.bind_artifact_owner(fence, aref('art-2'), owner='owner-1', operation_id=oid('op-1'))


def test_h077():
    with driver_env('h077') as e:
        j = e.ctx.journal
        fence = e.ctx.fence
        ref = aref('art-1')
        j.bind_artifact_owner(fence, ref, owner='o1', operation_id=oid('b1'))
        j.bind_artifact_owner(fence, ref, owner='o2', operation_id=oid('b2'))
        j.release_artifact_owner(fence, ref, owner='o1', operation_id=oid('u1'))
        j.release_artifact_owner(fence, ref, owner='o1', operation_id=oid('u1'))
        owners, _ = j.artifact_owners(ref, limit=10)
        assert owners == ('o2',)


def test_h078():
    with driver_env('h078') as e:
        j = e.ctx.journal
        fence = e.ctx.fence
        ref = aref('art-1')
        for i in range(5):
            j.bind_artifact_owner(fence, ref, owner='o%d' % i, operation_id=oid('b%d' % i))
        page1, cur = j.artifact_owners(ref, limit=2)
        assert len(page1) == 2 and cur is not None
        page2, cur2 = j.artifact_owners(ref, limit=2, cursor=cur)
        assert len(page2) == 2 and set(page1).isdisjoint(page2)
        with pytest.raises(Exception):
            j.artifact_owners(ref, limit=0)
        with pytest.raises(Exception):
            j.artifact_owners(ref, limit=99999)


def test_h079():
    db = sqlite3.connect(':memory:')
    RunJournal._initialize_connection(db)
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert 'artifact_owner' in tables
    RunJournal._initialize_connection(db)
    assert 'artifact_owner' in {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    db.close()
