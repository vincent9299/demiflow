"""Real V012/V023 resource experiments; measurements never grant resources."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
_ARROW_POOLS = []  # Keep proxy pools alive until all native buffers are gone.


def _write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str) + '\n')


class ArrowAllocationProbe:
    """Actual business actor observes the real Arrow pool before conversion."""
    def __init__(self, directory):
        import pyarrow as pa
        self.directory = directory
        self.previous = pa.default_memory_pool()
        self.pool = pa.proxy_memory_pool(self.previous)
        _ARROW_POOLS.append(self.pool)
        pa.set_memory_pool(self.pool)

    def __call__(self, batch):
        values = batch.to_pylist()
        assert values == [{'value': None}] * len(batch)
        _write(Path(self.directory) / 'callback.json', {'rows': len(batch),
            'retained_bytes': batch.get_total_buffer_size(), 'nbytes': batch.nbytes,
            'schema': str(batch.schema), 'values': values})
        return batch

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        assert state == {}

    def close(self):
        import pyarrow as pa
        _write(Path(self.directory) / 'allocation.json', {'pid': os.getpid(),
            'peak_bytes': self.pool.max_memory(), 'live_bytes': self.pool.bytes_allocated(),
            'allocation_count': self.pool.num_allocations()})
        pa.set_memory_pool(self.previous)


def arrow_worker_preallocation(directory, *, provider='local', rejected=True):
    """Typed nulls amplify into native fixed-list buffers before any callback."""
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([('value', pa.list_(pa.float64(), 1024) if rejected else pa.float64())])
    count = 16 if rejected else 8  # Separate legal positive; the 16-row red case never shrinks.
    source = pa.Table.from_pylist([{'value': None}] * count, schema=schema)
    limits = dict(memory_bytes=4*1024**2, max_row_bytes=1024,
        max_batch_bytes=4096, max_state_bytes=512*1024, timeout_s=30)
    error = None
    try:
        with RunContext.start(directory / 'run', limits=limits), _executor(provider, workers=4,
                memory_bytes=limits['memory_bytes'], batch_rows=16):
            flow = data.from_arrow(source).map_batches(ArrowAllocationProbe,
                fn_constructor_kwargs={'directory': str(directory)},
                batch_size=16, batch_format='pyarrow')
            # The positive isolates constructor admission from the independently
            # bounded terminal block-index window at this deliberately tiny cap.
            actual = flow.take_all() if rejected else flow.count()
    except Exception as exc:
        error = {'type': type(exc).__name__, 'text': str(exc)}
    _write(directory / 'error.json', error)
    assert (directory / 'allocation.json').exists(), error
    observed = json.loads((directory / 'allocation.json').read_text())
    result = {'error': error, 'actual_arrow': observed, 'limits': limits,
              'input_arrow_bytes': source.get_total_buffer_size(),
              'rows': count, 'required_converted_payload_bytes': count * (1024 if rejected else 1) * 8}
    _write(directory / 'proof.json', result)
    assert observed['peak_bytes'] <= limits['max_batch_bytes'], result
    if rejected:
        assert error is not None and 'conversion admission' in error['text'], result
        assert not (directory / 'callback.json').exists(), 'callback must not run after failed admission'
    else:
        assert error is None and actual == count, result
        callback = json.loads((directory / 'callback.json').read_text())
        assert callback['rows'] == count and callback['schema'] == str(schema)
        assert callback['values'] == [{'value': None}] * count
        result['callback'] = callback
        _write(directory / 'proof.json', result)
    return result


@contextmanager
def _executor(provider, *, workers, memory_bytes, batch_rows=1):
    from demiflow import data
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    if provider == 'local':
        with data.local_execution(workers=workers, memory_bytes=memory_bytes, batch_rows=batch_rows) as session:
            yield session
        return
    executor = RayDatasetExecutor(workers=workers,
        native_resources={'memory_bytes': memory_bytes, 'batch_rows': batch_rows})
    try:
        with _use_executor(executor):
            yield executor
    finally:
        executor.close()


def lance_cast_preallocation(directory, *, provider='local', case='conversion_negative'):
    """Real Dataset writer cannot allocate an oversized native target first."""
    import pyarrow as pa
    from pytest import MonkeyPatch
    from demiflow import data
    from demiflow.execution.lance_sink import LanceSink
    from demiflow.execution.run_context import RunContext
    from demiflow.execution import arrow_batch_sink
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    observed, casts = [], []
    prepare = LanceSink.prepare
    original_cast_bound = arrow_batch_sink._cast_allocation
    def cast_bound(table, schema):
        allocation = original_cast_bound(table, schema)
        casts.append({'retained_input_bytes': table.get_total_buffer_size(),
                      'reserved_output_bytes': allocation, 'input_schema': str(table.schema),
                      'target_schema': str(schema),
                      'simultaneous_payload_bound': table.get_total_buffer_size() + allocation})
        return allocation
    def measured(sink, *args, **kwargs):
        previous = pa.default_memory_pool()
        pool = pa.proxy_memory_pool(previous); _ARROW_POOLS.append(pool)
        pa.set_memory_pool(pool)
        try:
            return prepare(sink, *args, **kwargs)
        finally:
            observed.append({'peak_bytes': pool.max_memory(), 'live_bytes': pool.bytes_allocated(),
                             'allocation_count': pool.num_allocations()})
            pa.set_memory_pool(previous)
    limits = dict(memory_bytes=4*1024**2, max_row_bytes=4096,
        max_batch_bytes=65536, max_state_bytes=65536, timeout_s=30)
    target = directory / 'output.lance'
    error = None
    with MonkeyPatch.context() as patch:
        patch.setattr(LanceSink, 'prepare', measured)
        patch.setattr(arrow_batch_sink, '_cast_allocation', cast_bound)
        try:
            with RunContext.start(directory / 'run', limits=limits), _executor(provider,
                    workers=4, memory_bytes=limits['memory_bytes']):
                if case.startswith('conversion'):
                    source = data.from_items([{'value': None}] * 16)
                    schema = pa.schema([('value', pa.binary(131072 if case.endswith('negative') else 128))])
                elif case == 'cast_negative':
                    source = data.from_arrow(pa.table({'value': pa.nulls(16)}))
                    schema = pa.schema([('value', pa.binary(131072))])
                else:
                    assert case == 'cast_positive'
                    source = data.from_arrow(pa.table({'value': pa.array(range(16), type=pa.uint64())}))
                    schema = pa.schema([('value', pa.float64())])
                source.write_lance(str(target), mode='create', schema=schema)
        except Exception as exc:
            error = {'type': type(exc).__name__, 'text': str(exc)}
    result = {'error': error, 'actual_arrow': observed, 'cast_admission': casts, 'limits': limits, 'case': case,
              'required_single_target_bytes': 131072, 'target_exists': target.exists()}
    _write(directory / 'proof.json', result)
    assert observed and max(v['peak_bytes'] for v in observed) <= limits['max_batch_bytes'], result
    if case.endswith('negative'):
        assert error is not None and not target.exists(), result
        assert ('conversion admission' if case.startswith('conversion') else 'before allocation') in error['text'], result
    else:
        import lance
        assert error is None and target.exists(), result
        actual = lance.dataset(str(target)).to_table().to_pylist()
        assert actual == [{'value': float(i) if case == 'cast_positive' else None} for i in range(16)]
        if case == 'cast_positive':
            assert casts and all(v['retained_input_bytes'] > 0 and v['reserved_output_bytes'] >= v['retained_input_bytes'] for v in casts)
    return result


def equal_width_cast_residency(directory, *, rejected):
    """Precise native commit seam: each equal-width copy fits, their sum does not.

    Dataset cases above cover full public wiring. This additional original
    coordinator/writer case isolates retained Arrow ownership from the earlier,
    conservatively bounded Python-to-Arrow builder.
    """
    import pyarrow as pa
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.commit import CommitCoordinator
    from demiflow.execution.lance_sink import LanceSink
    from demiflow.execution.recovery_contract import SourcePosition
    from demiflow.lance.model import LanceWriteSpec
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    count = 4300 if rejected else 3000
    incoming = pa.table({'value':pa.array(range(count), type=pa.uint64())})
    limits = dict(memory_bytes=4*1024**2, max_row_bytes=65536,
                  max_batch_bytes=65536, batch_rows=4300, max_state_bytes=512*1024)
    target = directory/'output.lance'
    previous = pa.default_memory_pool(); pool = pa.proxy_memory_pool(previous)
    _ARROW_POOLS.append(pool); error = None
    pa.set_memory_pool(pool)
    try:
        with RunContext.start(directory/'run', limits=limits) as run:
            with run.action({'kind':'equal-width-native-commit'}) as action:
                sink = LanceSink(action, LanceWriteSpec(uri=str(target), mode='create',
                                 schema=pa.schema([('value',pa.float64())])))
                CommitCoordinator(action).commit(1, outputs={'lance':(sink,[incoming])},
                    position=SourcePosition('fixed',count))
                action.complete({'rows':count})
    except Exception as exc:
        error = {'type':type(exc).__name__, 'text':str(exc)}
    finally:
        peak = pool.max_memory()
        pa.set_memory_pool(previous)
    result = {'rows':count,'limits':limits,'input_payload_bytes':incoming.get_total_buffer_size(),
              'target_payload_bytes':count*8,'simultaneous_payload_lower_bound':incoming.get_total_buffer_size()+count*8,
              'actual_arrow_peak_bytes':peak,'error':error,'journal':_journal(run),'target_exists':target.exists()}
    _write(directory/'proof.json',result)
    assert result['input_payload_bytes'] <= limits['max_batch_bytes']
    assert result['target_payload_bytes'] <= limits['max_batch_bytes']
    if rejected:
        assert result['simultaneous_payload_lower_bound'] > limits['max_batch_bytes']
        assert error is not None and 'before allocation' in error['text'],result
        assert peak <= 128, 'equal-width target payload must never have been allocated'
        assert not target.exists() and not any(c['status']=='acked' for c in result['journal']['commits'])
    else:
        import lance
        assert error is None,result
        actual = lance.dataset(str(target)).to_table()
        assert actual.schema == pa.schema([('value',pa.float64())])
        assert actual['value'].to_pylist() == [float(i) for i in range(count)]
        assert result['journal']['commits'][-1]['status'] == 'acked'
        result['actual_values_sha256'] = hashlib.sha256(json.dumps(actual.to_pylist()).encode()).hexdigest()
        _write(directory/'proof.json',result)
    return result


def _event(directory, name, **values):
    path = Path(directory) / (name + '.json')
    if not path.exists():
        _write(path, {'at_ns': time.time_ns(), 'pid': os.getpid(), **values})


class CreditObservation:
    """Worker-local read-only observation, installed only by test actors."""
    def __init__(self, directory, name):
        from demiflow.execution.exchange.dataflow_state import ChannelSetState
        self.directory, self.name = directory, name
        self.original = ChannelSetState.reserve
        self.samples = self.peak_bytes = self.peak_rows = 0
        original = self.original
        def observe(state, *args, **kwargs):
            value = original(state, *args, **kwargs)
            with state._lock:
                resident = state._resident_bytes + state._lease_bytes
                rows = sum(lease.rows for lease in state._leases.values())
                assert resident <= state.bounds.max_resident_bytes
                assert len(state._leases) <= state.bounds.max_pending_events
            self.samples += 1
            self.peak_bytes = max(self.peak_bytes, resident)
            self.peak_rows = max(self.peak_rows, rows)
            return value
        self.observe = observe
        ChannelSetState.reserve = observe

    def close(self):
        from demiflow.execution.exchange.dataflow_state import ChannelSetState
        assert ChannelSetState.reserve is self.observe
        ChannelSetState.reserve = self.original
        _write(Path(self.directory) / ('credit-' + self.name + '-' + str(os.getpid()) + '.json'),
               {'pid':os.getpid(),'samples':self.samples,'peak_bytes':self.peak_bytes,'peak_rows':self.peak_rows})


class SlowBranch:
    def __init__(self, directory, name, delay):
        self.directory, self.name, self.delay = directory, name, delay
        self.count = 0
        self.credit = CreditObservation(directory, name)
    def __call__(self, row):
        _event(self.directory, self.name + '-started')
        time.sleep(self.delay)
        self.count += 1
        return {**row, 'branch': self.name}
    def snapshot_state(self):
        return {'count': self.count}
    def restore_state(self, state):
        self.count = state['count']
    def close(self):
        self.credit.close()
        _write(Path(self.directory) / (self.name + '-closed.json'),
            {'at_ns': time.time_ns(), 'pid': os.getpid(), 'count': self.count})


class StateAndOutput:
    def __init__(self, directory):
        self.directory = directory
        self.state = bytes(bytearray(128 * 1024))
        self.count = 0
        _event(directory, 'state-created', state_payload_bytes=len(self.state))
    def __call__(self, row):
        from demiflow.data.runtime_contract import check_size
        from demiflow.resources.capacity import inherited_envelope
        result = {'id': row['id'], 'value': bytes(bytearray(64 * 1024))}
        self.count += 1
        _event(self.directory, 'state-output-coexist',
            state_payload_bytes=len(self.state), input_python_bytes=check_size(row),
            output_python_bytes=check_size(result), distinct_state_output=self.state is not result['value'],
            original_worker_envelope=inherited_envelope())
        return result
    def snapshot_state(self):
        return {'state': self.state, 'count': self.count}
    def restore_state(self, state):
        self.state, self.count = state['state'], state['count']


@contextmanager
def observe_execution(*, workers, memory_bytes):
    """Tap original successful decisions, never replace their results or grants."""
    from pytest import MonkeyPatch
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.resource_state import RunResourceState
    from demiflow.execution.exchange.dataflow_state import ChannelSetState
    from demiflow.execution.run_journal import RunJournal
    from runtime_v3_performance_scenarios import _plain
    report = {'plans': [], 'launches': [], 'charged_peak': {}, 'credit_peak_bytes': 0,
              'credit_peak_rows': 0, 'credit_samples': 0, 'budget_peaks': {}}
    originals = DataflowExecution.prepare, DataflowExecution._prepare_launch, RunResourceState._charged, ChannelSetState.reserve, RunJournal.reserve_budgets_in_transaction
    def prepare(execution):
        value = originals[0](execution)
        report['plans'].append({'physical': _plain(execution._physical),
                               'graph': _plain(execution._graph)})
        assert execution._physical.bounds.max_resident_bytes == memory_bytes
        assert execution._physical.policy.max_active_workers == workers
        return value
    def launch(execution, lease, stage):
        value = originals[1](execution, lease, stage)
        assert len(report['launches']) < 2048
        report['launches'].append({'at_ns': time.time_ns(), 'stage': stage.stage_id,
            'allocation_id': lease.allocation_id, 'host_id': lease.host_id,
            'resident_bytes': stage.resident_bytes, 'profile': _plain(stage.profile)})
        return value
    def charged(state, db, scope):
        value = originals[2](state, db, scope)
        assert value['workers'] <= workers and value['memory_bytes'] <= memory_bytes
        for key, number in value.items():
            report['charged_peak'][key] = max(report['charged_peak'].get(key, 0), number)
        return value
    def reserve(state, *args, **kwargs):
        value = originals[3](state, *args, **kwargs)
        with state._lock:
            byte_count = state._resident_bytes + state._lease_bytes
            row_count = sum(lease.rows for lease in state._leases.values())
            assert byte_count <= state.bounds.max_resident_bytes
            assert len(state._leases) <= state.bounds.max_pending_events
        report['credit_samples'] += 1
        report['credit_peak_bytes'] = max(report['credit_peak_bytes'], byte_count)
        report['credit_peak_rows'] = max(report['credit_peak_rows'], row_count)
        return value
    def budgets(db, reservation, charges):
        values = originals[4](db, reservation, charges)
        for key, total in values.items():
            assert total <= charges[key][1]
            report['budget_peaks'][key] = max(report['budget_peaks'].get(key, 0), total)
        return values
    with MonkeyPatch.context() as patch:
        patch.setattr(DataflowExecution, 'prepare', prepare)
        patch.setattr(DataflowExecution, '_prepare_launch', launch)
        patch.setattr(RunResourceState, '_charged', charged)
        patch.setattr(ChannelSetState, 'reserve', reserve)
        patch.setattr(RunJournal, 'reserve_budgets_in_transaction', staticmethod(budgets))
        yield report


@contextmanager
def partial_capacity(directory, *, provider):
    """A real live admitted owner retains all but one CPU during the Dataset."""
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    if provider == 'ray_core':
        import ray
        @ray.remote(num_cpus=5)
        class Holder:
            def __init__(self): self.data = bytearray(8 * 1024**2)
            def status(self):
                import psutil
                return {'pid': os.getpid(), 'birth': psutil.Process().create_time(),
                        'node': ray.get_runtime_context().get_node_id(), 'bytes': len(self.data)}
        actor = Holder.remote()
        report = {'provider': provider, 'owner': ray.get(actor.status.remote()),
                  'available_cpu': ray.available_resources().get('CPU', 0)}
        assert report['available_cpu'] <= 1
        try:
            yield report
            assert ray.get(actor.status.remote()) == report['owner']
        finally:
            import psutil
            ray.kill(actor, no_restart=True)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    process = psutil.Process(report['owner']['pid'])
                    live = (process.create_time() == report['owner']['birth']
                            and process.status() != psutil.STATUS_ZOMBIE)
                except psutil.NoSuchProcess:
                    live = False
                if not live and ray.available_resources().get('CPU', 0) >= 5:
                    break
                time.sleep(.05)
            assert not live, 'owned capacity holder did not physically exit'
            report['physical_exit'] = True
            report['released_cpu'] = ray.available_resources().get('CPU', 0)
            assert report['released_cpu'] >= 5
            _write(directory / 'holder-proof.json', report)
        return
    from demiflow.execution.dataflow_capacity import local_capacity
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.resources.process_unit import ManagedProcessUnit
    domain, capacity = local_capacity()
    assert str(domain).startswith('/tmp/demiflow-') and capacity['cpu_millis'] > 1000
    pool = LocalCapacityPool(domain / 'capacity', capacity=capacity)
    allocation, fingerprint = 'acceptance-holder', 'c' * 64
    effective = {'cpu_millis': capacity['cpu_millis'] - 1000, 'memory_bytes': 8*1024**2}
    pool.reserve(allocation, fingerprint, effective, 10000)
    unit = ManagedProcessUnit(directory / 'unit')
    ready = directory / 'ready'
    body = 'import time;from pathlib import Path;data=bytearray(8*1024**2);Path(' + repr(str(ready)) + ').write_text(str(len(data)));time.sleep(300)'
    owned = unit.spawn('acceptance', allocation, fingerprint, [sys.executable, '-B', '-c', body],
        log_path=directory / 'holder.log', envelope={**effective, 'allocation_id': allocation,
            'spec_hash': fingerprint, 'resource_directory': str(domain)})
    pool.bind_identity(allocation, owned['identity'])
    report = {'provider': provider, 'owner': owned['identity'], 'original_capacity': capacity,
              'original_reservation': effective, 'remaining_cpu_millis': 1000}
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline: time.sleep(.01)
        assert ready.exists() and owned['process'].poll() is None
        yield report
        assert owned['process'].poll() is None, 'holder must not yield its reserved capacity to make the graph pass'
        report['while_graph_complete'] = pool.snapshot()
        assert any(a['allocation_id'] == allocation and a['state'] == 'BOUND'
                   for a in report['while_graph_complete']['active'])
    finally:
        proof = unit.stop('acceptance', allocation, terminate_deadline_ms=1000, process=owned['process'])
        assert proof['confirmed']
        pool.release(allocation, proof)
        report['released'] = pool.snapshot()
        assert not report['released']['active']
        _write(directory / 'holder-proof.json', report)


def _journal(run):
    with run.journal._connection() as db:
        budgets = {name: {'consumed': used, 'ceiling': ceiling}
                   for name, used, ceiling in db.execute('SELECT name,consumed,ceiling FROM runtime_budgets')}
        allocated = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resource_allocations'").fetchone() is not None
        allocations = ([dict(zip(('allocation_id','cleanup'), row))
                       for row in db.execute('SELECT allocation_id,cleanup FROM resource_allocations')] if allocated else [])
    return {'commits': run.journal.execution_commits('action-1'), 'budgets': budgets,
            'allocation_table_exists':allocated, 'allocations': allocations}


def _source_rows(count, width, directory):
    for i in range(count):
        if i == 0: _event(directory, 'source-start')
        yield {'id': i, 'payload': bytes([i % 251]) * width}
    _event(directory, 'source-eoi')


def fanout_slow_sink(directory, *, provider='local', partial=False):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from contextlib import nullcontext
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    count, width = (64, 4096) if partial else (8, 8192)
    workers = 1 if partial else 4
    limits = dict(memory_bytes=16*1024**2, max_row_bytes=32768, max_batch_bytes=65536,
        max_state_bytes=512*1024, max_scratch_bytes=32*1024**2,
        checkpoint_rows=64, checkpoint_interval_ms=1000, timeout_s=120)
    source = [{'id': i, 'payload': bytes([i % 251]) * width} for i in range(count)]
    held = partial_capacity(directory/'holder', provider=provider) if partial else nullcontext(None)
    started = time.monotonic()
    rows, first = [], None
    error = None; facts = None; observed = {}; holder = None
    try:
        with held as holder, observe_execution(workers=workers, memory_bytes=limits['memory_bytes']) as observed:
            with RunContext.start(directory/'run', limits=limits) as run, _executor(provider,
                    workers=workers, memory_bytes=limits['memory_bytes']):
                shared = data.from_items(source)
                fast = shared.map(SlowBranch, fn_constructor_kwargs={'directory':str(directory),'name':'fast','delay':0})
                slow = shared.map(SlowBranch, fn_constructor_kwargs={'directory':str(directory),'name':'slow','delay':.002})
                flow = fast.union(slow)
                for row in flow.iter_rows():
                    if first is None:
                        first = time.monotonic()-started
                        _event(directory, 'consumer-start')
                    assert row['payload'] == bytes([row['id'] % 251]) * width
                    rows.append((row['branch'],row['id']))
                    time.sleep(.003)
                facts = _journal(run)
    except Exception as exc:
        error = {'type': type(exc).__name__, 'text': str(exc), 'code': getattr(exc, 'code', None)}
        if 'run' in locals(): facts = _journal(run)
    credit = [json.loads(p.read_text()) for p in directory.glob('credit-*.json')]
    report = {'worker_credit_observations':credit,'error':error,'rows':len(rows),'input_rows':count,'row_payload_bytes':width,'limits':limits,
              'first_result_s':first,'elapsed_s':time.monotonic()-started,
              'original_execution':observed,'original_journal':facts,'capacity_holder':holder,
              'markers':{p.name:json.loads(p.read_text()) for p in directory.glob('*-start*.json')}}
    _write(directory/'proof.json',report)
    assert error is None, report
    assert sorted(rows) == sorted((side,i) for side in ('fast','slow') for i in range(count))
    assert facts['commits'] and all(c['status']=='acked' for c in facts['commits'])
    assert observed['charged_peak']['workers'] > 0
    assert credit and sum(v['samples'] for v in credit) > 0
    assert max(v['peak_bytes'] for v in credit) <= limits['memory_bytes']
    if partial:
        assert len(observed['launches']) >= 3
        assert any(e['mode']=='durable_incremental' for p in observed['plans'] for e in p['physical']['exchanges'])
    return report


def _hot_rows():
    for i in range(16384):
        yield {'k':0 if i%10 else 1+(i//10)%127, 'v':i, 'payload':'x'*4096}


def _hot_reduce(acc, row):
    return {'k':row['k'],'count':(acc or {}).get('count',0)+1,'sum':(acc or {}).get('sum',0)+row['v']}


def hot_key_disk(directory, *, provider='local', small_disk=False):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    limits=dict(memory_bytes=64*1024**2,max_row_bytes=65536,max_batch_bytes=262144,
        max_scratch_bytes=8*1024**2 if small_disk else 2*1024**3,max_state_bytes=2*1024**2,
        max_output_rows=1000000,checkpoint_rows=512,checkpoint_interval_ms=1000,timeout_s=120)
    error=None;rows=None;started=time.monotonic()
    with observe_execution(workers=4,memory_bytes=limits['memory_bytes']) as observed:
        try:
            with RunContext.start(directory/'run',limits=limits) as run,_executor(provider,
                    workers=4,memory_bytes=limits['memory_bytes']):
                rows=data.from_iter(_hot_rows).reduce_by_key('k',_hot_reduce).take_all()
        except Exception as exc:
            error={'type':type(exc).__name__,'text':str(exc),'code':getattr(exc,'code',None)}
        facts=_journal(run)
    result={'limits':limits,'error':error,'rows':rows,'elapsed_s':time.monotonic()-started,
            'original_execution':observed,'original_journal':facts,'input_rows':16384,'payload_width':4096}
    _write(directory/'proof.json',result)
    if small_disk:
        assert error is not None and (error['code']=='BUDGET_EXCEEDED' or
            error['text']=='Execution budget exhausted: artifact_staging_bytes'),result
        assert not any(sink.get('manifest', {}).get('final') is True and c['status']=='acked'
                       for c in facts['commits'] for sink in c['prepared']['sinks'].values())
    else:
        assert error is None,result
        expected={}
        for row in _hot_rows():expected[row['k']]=_hot_reduce(expected.get(row['k']),row)
        assert sorted(rows,key=lambda r:r['k'])==sorted(expected.values(),key=lambda r:r['k'])
        assert len(rows)==128 and sum(r['count'] for r in rows)==16384
        assert facts['commits'][-1]['status']=='acked'
    return result


def _oversized_row():
    yield {'payload': bytes(bytearray(32769))}


def single_row_admission(directory, *, provider='local'):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    limits=dict(memory_bytes=4*1024**2,max_row_bytes=32768,max_batch_bytes=65536,
                max_state_bytes=512*1024,timeout_s=30)
    error=None
    try:
        with RunContext.start(directory/'run',limits=limits) as run,_executor(provider,
                workers=4,memory_bytes=limits['memory_bytes']):
            data.from_iter(_oversized_row).map(SlowBranch,
                fn_constructor_kwargs={'directory':str(directory),'name':'forbidden','delay':0}).take_all()
    except Exception as exc:
        error={'type':type(exc).__name__,'text':str(exc),'code':getattr(exc,'code',None)}
    facts=_journal(run)
    result={'limits':limits,'error':error,'actual_payload_bytes':32769,'journal':facts}
    _write(directory/'proof.json',result)
    assert error is not None and error['code']=='BUDGET_EXCEEDED',result
    assert not (directory/'forbidden-started.json').exists()
    assert not facts['commits']
    return result


def state_output_residency(directory, *, provider='local', rejected=False):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    limits=dict(memory_bytes=1024**2 if rejected else 4*1024**2,
        max_row_bytes=128*1024,max_batch_bytes=256*1024,
        max_state_bytes=1024**2 if rejected else 512*1024,timeout_s=45)
    error=None;rows=None
    with observe_execution(workers=4,memory_bytes=limits['memory_bytes']) as observed:
        try:
            with RunContext.start(directory/'run',limits=limits) as run,_executor(provider,
                    workers=4,memory_bytes=limits['memory_bytes']):
                rows=data.from_items([{'id':i} for i in range(16)]).map(StateAndOutput,
                    fn_constructor_kwargs={'directory':str(directory)}).take_all()
        except Exception as exc:
            error={'type':type(exc).__name__,'text':str(exc),'code':getattr(exc,'code',None)}
    result={'limits':limits,'error':error,'observed':observed,'journal':_journal(run)}
    if rejected:
        assert error is not None and error['code']=='RESOURCE_INFEASIBLE',result
        assert not observed['launches'] and not (directory/'state-created.json').exists()
    else:
        assert error is None,result
        assert rows==[{'id':i,'value':bytes(64*1024)} for i in range(16)]
        actual=json.loads((directory/'state-output-coexist.json').read_text())
        assert actual['distinct_state_output'] and actual['state_payload_bytes']==128*1024
        assert actual['output_python_bytes']>64*1024
        graph=observed['plans'][0]['graph'];physical=observed['plans'][0]['physical']
        states=[s for s in physical['stages'] if s['state_bytes']]
        assert states and max(s['state_bytes'] for s in states)>=actual['state_payload_bytes']
        assert all(s['resident_bytes']>=s['state_bytes']+s['output_bytes'] for s in states)
        minimum=actual['state_payload_bytes']+actual['input_python_bytes']+actual['output_python_bytes']
        if provider=='local':
            assert actual['original_worker_envelope']['memory_bytes']>=minimum
        assert max(s['profile']['requests']['memory_bytes'] for s in states)>=minimum
        result.update(rows=len(rows),actual_coexistence=actual,managed_payload_lower_bound=minimum)
    _write(directory/'proof.json',result)
    return result


def _snapshot():
    paths=sorted((ROOT/'demiflow').rglob('*.py'))+[Path(__file__),ROOT/'tests/runtime_v3_support.py']
    assert len(paths)<10000
    return {'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            'inputs':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}


def _child(scenario, provider, directory):
    from runtime_v3_support import _RayDataBlocker, _ExecuteActionRecorder, assert_tree_binding
    root=Path(directory);root.mkdir(parents=True,exist_ok=True)
    assert_tree_binding()
    assert all(os.environ[name].startswith('/tmp/demiflow-') for name in
               ('DEMIFLOW_RESOURCE_ROOT','DEMIFLOW_RESOURCE_HOME','DEMIFLOW_FORGE_DOMAIN'))
    before=_snapshot();ray=None;result={};error=None
    try:
        with _RayDataBlocker() as blocked, _ExecuteActionRecorder() as recorder:
            if provider=='ray_core':
                import ray
                import tempfile
                ray.init(address='local',num_cpus=6,include_dashboard=False,
                    object_store_memory=100*1024**2,_temp_dir=tempfile.mkdtemp(prefix='demiflow-rv-',dir='/tmp'))
            if scenario=='fairness_deadlock':
                result['partial_slots']=fanout_slow_sink(root/'partial-slots',provider=provider,partial=True)
                result['hot_key']=hot_key_disk(root/'hot-key',provider=provider)
                result['small_disk']=hot_key_disk(root/'small-disk',provider=provider,small_disk=True)
            elif scenario=='memory_peak':
                result['single_row']=single_row_admission(root/'single-row',provider=provider)
                result['wide_fanout']=fanout_slow_sink(root/'wide-fanout',provider=provider)
                for rejected in (True,False):
                    result['worker_conversion_'+str(rejected)]=arrow_worker_preallocation(
                        root/('worker-conversion-'+str(rejected)),provider=provider,rejected=rejected)
                    result['state_output_'+str(rejected)]=state_output_residency(
                        root/('state-output-'+str(rejected)),provider=provider,rejected=rejected)
                for case in ('conversion_negative','conversion_positive','cast_negative','cast_positive'):
                    result[case]=lance_cast_preallocation(root/case,provider=provider,case=case)
                for rejected in (True,False):
                    result['equal_width_cast_'+str(rejected)]=equal_width_cast_residency(
                        root/('equal-width-cast-'+str(rejected)),rejected=rejected)
                result['rows']=result['wide_fanout']['rows']
                assert result['rows']==16
            else:
                raise ValueError(scenario)
            result['child_execute_action_calls']=recorder.calls
            result['child_ray_data_import_attempts']=blocked.attempts
            assert recorder.calls and not blocked.attempts
    except BaseException as exc:
        import traceback
        error={'type':type(exc).__name__,'text':str(exc),'traceback':traceback.format_exc()}
    finally:
        if ray is not None:
            ray.shutdown()
        import psutil
        from itertools import islice
        process = psutil.Process()
        deadline = time.monotonic() + 15
        children = process.children(recursive=True)
        while children and time.monotonic() < deadline:
            time.sleep(.05)
            children = process.children(recursive=True)
        allocation_paths = list(islice((Path(os.environ['DEMIFLOW_RESOURCE_ROOT']) / 'capacity' / 'allocations').glob('*.json'), 1025))
        assert len(allocation_paths) <= 1024
        allocations = [json.loads(p.read_text()) for p in allocation_paths]
        active = [v['allocation_id'] for v in allocations if v['state'] != 'RELEASED']
        import sqlite3
        databases = list(islice(root.glob('*/run/run.sqlite'), 33))
        assert len(databases) <= 32
        journal_allocations = {}; unconfirmed = []
        for path in databases:
            with sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True) as db:
                exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='resource_allocations'").fetchone()
                states = dict(db.execute('SELECT cleanup,count(*) FROM resource_allocations GROUP BY cleanup')) if exists else {}
            journal_allocations[str(path.relative_to(root))] = states
            if any(state != 'CONFIRMED' for state in states):
                unconfirmed.append(str(path.relative_to(root)))
        result['cleanup'] = {'children':[{'pid':p.pid,'birth':p.create_time()} for p in children],
                             'original_pool_records':len(allocations),'active_allocations':active,
                             'original_journal_allocations':journal_allocations,'unconfirmed':unconfirmed}
        if (children or active or unconfirmed) and error is None:
            error = {'type':'AssertionError','text':'original resource cleanup is not confirmed'}
        after=_snapshot()
        result.update(source_before=before,source_after=after,error=error,provider=provider,scenario=scenario)
        _write(root/'result.json',result)
    assert before==after,'source changed during an acceptance run'
    assert error is None,error
    return result


def _export_evidence(directory, *, provider, scenario):
    """Preserve bounded observations outside the pytest-owned data directory."""
    base = os.environ.get('DEMIFLOW_RESOURCE_EVIDENCE')
    if base is None:
        return None
    import shutil
    import sqlite3
    from itertools import islice
    destination = Path(base).resolve()
    assert destination.parent == Path('/tmp') and destination.name.startswith('demiflow-')
    destination = destination / provider / scenario
    destination.mkdir(parents=True, exist_ok=False)
    candidates = [directory/'result.json', directory/'process.log']
    candidates += list(islice(directory.glob('*/*.json'), 4097))
    candidates += list(islice((directory/'capacity-domain'/'capacity'/'allocations').glob('*.json'), 1025))
    assert len(candidates) <= 5120
    copied = {}; total = 0
    for source in candidates:
        if not source.exists():
            continue
        assert source.is_file() and not source.is_symlink()
        size = source.stat().st_size
        total += size
        assert size <= 64*1024**2 and total <= 256*1024**2
        target = destination / source.relative_to(directory)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        copied[str(target.relative_to(destination))] = {'bytes':size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest()}
    databases = list(islice(directory.glob('*/run/run.sqlite'), 33))
    assert len(databases) <= 32
    for source in databases:
        target = destination / source.relative_to(directory)
        target.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect('file:' + str(source) + '?mode=ro', uri=True) as original:
            with sqlite3.connect(target) as snapshot:
                pages = original.execute('PRAGMA page_count').fetchone()[0]
                page_size = original.execute('PRAGMA page_size').fetchone()[0]
                assert pages * page_size <= 64*1024**2
                assert total + pages * page_size <= 256*1024**2
                original.backup(snapshot)
        total += target.stat().st_size
        assert total <= 256*1024**2
        copied[str(target.relative_to(destination))] = {'bytes':target.stat().st_size,
            'sha256':hashlib.sha256(target.read_bytes()).hexdigest()}
    _write(destination/'index.json',{'scope':'observations and original journals, not a replayable payload archive',
        'files':copied,'bytes':total})
    return str(destination)


def _run(fx, scenario, fault):
    if fault is not None:
        raise ValueError('Resource scenarios have explicit fixed negative cases, not arbitrary fault labels')
    directory=fx.root/scenario;directory.mkdir(parents=True,exist_ok=True)
    code='from runtime_v3_resource_scenarios import _child;_child('+repr(scenario)+','+repr(fx.provider)+','+repr(str(directory))+')'
    env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(ROOT)+':'+str(ROOT/'tests'),
         'DEMIFLOW_RESOURCE_ROOT':str(directory/'capacity-domain'),
         'DEMIFLOW_RESOURCE_HOME':str(directory/'legacy-domain'),
         'DEMIFLOW_FORGE_DOMAIN':str(directory/'forge-domain')}
    with (directory/'process.log').open('w') as log:
        process=subprocess.Popen([sys.executable,'-B','-c',code],cwd=ROOT,env=env,
            stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            status=process.wait(timeout=480)
        except subprocess.TimeoutExpired:
            import signal
            os.kill(process.pid,signal.SIGTERM)
            try:process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                os.kill(process.pid,signal.SIGKILL);process.wait(timeout=10)
            raise AssertionError('Owned resource acceptance timed out; inspect original unit evidence')
    result=json.loads((directory/'result.json').read_text())
    exported = _export_evidence(directory, provider=fx.provider, scenario=scenario)
    if exported is not None: result['exported_evidence'] = exported
    assert status==0 and result['error'] is None,result
    return result


def fairness_deadlock(fx, fault=None):
    return _run(fx,'fairness_deadlock',fault)


def memory_peak(fx, fault=None):
    return _run(fx,'memory_peak',fault)
