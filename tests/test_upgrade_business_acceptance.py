"""E package: real business acceptance from public entry points (upgrade 20261113).

Proves observable business results through the genuine engine, persistence and
workers. Production classes are reused, never mocked: PromptActor, RunPromptJournal
(its check_recovery is the native recovery gate), StreamLanceWriter (native keyed
commit reconciliation), QueueHistoryDatasource and the workbench runs manager
(prepare/start/show/cancel/resume). The only fake is the external model transport (a
loopback HTTP server) and a local origin object server; no real PromptActor / effect
journal / queue settlement / writer / recovery check is replaced. The standalone
StreamCheckpoint orchestration is retired on current disk, so keyed recovery is proven
through the writer's own commit reconciliation (the public delivery-only save_lance
resume needs its P3 native sink adapter — reported separately as a C integration gap).

Scenarios: workbench normal flow; function-dependency recovery (module helper /
closure / default args); model calls and unknown effects (known_completed / unknown /
confirmed-unsent); keyed save_lance interrupt recovery (append/overwrite contract,
conflict refusal); queue publish/consume/settle/fixed-prefix history; static graph
(multi-consumer, tuple/bytes, slow consumer, small budget, failure recovery).

Resource/perf evidence records result consistency, elapsed time, allocated budget
occupancy and temporary disk file counts under identical inputs/budgets. Only
allocated quotas are observed -- never claimed as process RSS. Small controlled
budgets cover oversized rows, slow consumers, cancel and recovery; real resource
state is checked after success/failure returns without calling an external release
helper first.

Tests write only under pytest tmp_path (run with --basetemp=/tmp/demiflow-e-*).
No Ray, no paid model, no full business data. L2 plan-driven / identity-bridge
acceptance that depends on B/C/D merged implementation is reported separately as
pending integration, never faked green here.
"""
from _forge_project_fixtures import declare_existing_flow
import asyncio
import importlib
import importlib.util
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.queue import QueueChannel, QueueConfig, ProducerSourceSpec, task_message
from demiflow.execution.queue_commit import producer_action
from demiflow.execution.queue_contract import HistoryReadLimit, HistoryReadLimits
from demiflow.execution.queue_sinks import QueuePublishSink, QueueResultSink
from demiflow.execution.recovery_contract import (
    RecoveryContractError, RunRef, SourcePosition, identity,
)
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.operator_llm.journal import UncertainPromptCall
from demiflow.operator_llm.stream_recovery import CALL_RESERVATION, RunPromptJournal
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.process_unit import ManagedProcessUnit
from demiflow.workbench.graph import model as graph_model
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.runs import facts, manager as runs

from test_map_prompt_async import PACK, server  # loopback fake model transport
from test_raw_fetch import BODY, origin_server  # loopback fake origin object server

LIMITS = {"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}
GRAPH_LIMITS = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
                "checkpoint_interval_ms": 60000}
ENTRY = 'flows/questions.py:select'
PARAMS = 'flows/select_a.json'
KEYED_SCHEMA = pa.schema([('id', pa.string()), ('value', pa.string())])

_CHAIN_CAPACITY = {"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3}
_GRAPH_CAPACITY = {"cpu_millis": 16000, "memory_bytes": 16 * 1024 ** 3}


@pytest.fixture(autouse=True)
def private_runtime_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))


@pytest.fixture
def pidfds():
    from demiflow.resources.process_observation import require_pidfds
    if not sys.platform.startswith('linux'):
        pytest.skip('Local managed units require Linux')
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip('pidfd exit evidence unavailable: ' + str(exc))


# ---------------------------------------------------------------------------
# Required test-domain helpers (never enter the platform public API).
# ---------------------------------------------------------------------------

def _json_default(value):
    if isinstance(value, bytes):
        return {'__bytes__': value.hex()}
    if isinstance(value, tuple):
        return list(value)
    raise TypeError('unserializable ' + type(value).__name__)


def _canonical(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False, default=_json_default)


def _assert_value_semantics(expected, actual, path):
    assert type(expected) is type(actual), (path, type(expected), type(actual))
    if isinstance(expected, dict):
        assert set(expected) == set(actual), (path, set(expected), set(actual))
        for key in expected:
            _assert_value_semantics(expected[key], actual[key], path + '.' + str(key))
    elif isinstance(expected, (list, tuple)):
        assert len(expected) == len(actual), (path, len(expected), len(actual))
        for index, (e, a) in enumerate(zip(expected, actual)):
            _assert_value_semantics(e, a, path + '[' + str(index) + ']')
    else:
        assert expected == actual, (path, expected, actual)


def assert_semantics_equal(expected, actual, *, ordering='declared'):
    """Verify field types, nulls, duplicate multiplicity and declared order.

    ordering='declared' compares row sequence exactly; 'unordered' compares the
    multiset (no global sort semantics imposed on unordered operators) while still
    checking per-row type/null/nested fidelity.
    """
    assert ordering in ('declared', 'unordered'), ordering
    assert type(expected) is list and type(actual) is list
    assert len(expected) == len(actual), ('row count', len(expected), len(actual))
    for row in actual:
        assert type(row) is dict, ('row is not a mapping', type(row))
    if ordering == 'declared':
        pairs = zip(expected, actual)
    else:
        assert Counter(_canonical(r) for r in expected) == Counter(_canonical(r) for r in actual), \
            'duplicate multiplicity differs'
        pairs = zip(sorted(expected, key=_canonical), sorted(actual, key=_canonical))
    for index, (e, a) in enumerate(pairs):
        _assert_value_semantics(e, a, 'row[' + str(index) + ']')


def _resource_evidence(run_dir, *, capacity, started=None):
    """Observe real allocated quotas / disk after a return; never claim RSS."""
    run_dir = Path(run_dir)
    resources = run_dir.with_name(run_dir.name + '-resources')
    files = [p for p in run_dir.rglob('*') if p.is_file()] if run_dir.exists() else []
    sqlite_files = sorted(p.name for p in run_dir.rglob('*.sqlite')) if run_dir.exists() else []
    charged = None
    if (resources / 'capacity').exists():
        charged = LocalCapacityPool(resources / 'capacity', capacity=capacity).snapshot()['charged']
    requests_budget = None
    if (run_dir / 'run.sqlite').exists():
        try:
            requests_budget = RunJournal(run_dir).budget_consumed('requests')
        except Exception:
            requests_budget = None
    return dict(elapsed_s=None if started is None else round(time.monotonic() - started, 3),
                file_count=len(files), sqlite_files=sqlite_files,
                capacity_charged=charged, requests_budget=requests_budget)


def _release_orphans(run_dir, *, capacity):
    """Reconcile capacity for allocations whose driver died, using real exit evidence."""
    resources = Path(run_dir).with_name(Path(run_dir).name + '-resources')
    if not (resources / 'capacity').exists():
        return
    pool = LocalCapacityPool(resources / 'capacity', capacity=capacity)
    unit = ManagedProcessUnit(resources / 'units')
    for record in pool.snapshot()['active']:
        allocation_id = record['allocation_id']
        process_json = next((resources / 'units').glob('*/' + allocation_id + '/process.json'))
        scope_id = process_json.parent.parent.name
        evidence = unit.stop(scope_id, allocation_id, terminate_deadline_ms=2000)
        assert evidence['confirmed'] is True
        pool.release(allocation_id, evidence)
        try:
            os.waitpid(evidence['identity']['pid'], os.WNOHANG)
        except (ChildProcessError, OSError):
            pass
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def _chain_child_id(run):
    # The unified v3 action owns the checkpoint and final result commits;
    # the former folded %-chain action no longer exists.
    assert run.journal.source_binding('action-1-dataflow-sealed') is not None
    return 'action-1'


def _crash_hook(root):
    marker = Path(root) / 'crashed-once'
    counter = [0]

    def hook(row):
        counter[0] += 1
        if counter[0] == 5 and not marker.exists():
            marker.write_text(json.dumps({'pid': os.getpid(), 'row': counter[0]}))
            os.kill(os.getpid(), signal.SIGKILL)
        return row

    return hook, marker


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                              stream_options=MapStreamOptions(concurrency=1)),))


def _load_business_modules(tmp_path, monkeypatch, sources, entry):
    bizdir = tmp_path / ('biz_' + uuid.uuid4().hex[:8])
    bizdir.mkdir()
    paths, specs, mods = {}, {}, {}
    for name, src in sources.items():
        path = bizdir / (name + '.py')
        path.write_text(src)
        paths[name] = path
        specs[name] = importlib.util.spec_from_file_location(name, path)
        mods[name] = importlib.util.module_from_spec(specs[name])
        monkeypatch.setitem(sys.modules, name, mods[name])
    monkeypatch.syspath_prepend(str(bizdir))
    monkeypatch.setenv('PYTHONPATH', str(bizdir) + os.pathsep + os.environ.get('PYTHONPATH', ''))
    for name in sources:
        specs[name].loader.exec_module(mods[name])
    return mods[entry], paths, bizdir


def build_owned_fixture(tmp_path, *, scenario, monkeypatch=None, **options):
    """Create deterministic fixed-version inputs, small objects and isolated config.

    Returns resource references and independently expected results; never borrows a
    missing historical object's SHA to fabricate a stand-in.
    """
    tmp_path = Path(tmp_path)
    if scenario == 'workbench':
        from examples.forge_workspace import make_demo
        root = tmp_path / 'workbench'
        make_demo.create(root)
        expected = [
            {'case_id': 'c01', 'question': '  三角形的面积是多少？  ',
             'image_uri': 'media/triangle.png', 'normalized_question': '三角形的面积是多少？',
             'score': 0.95, 'selected': True},
            {'case_id': 'c02', 'question': '增长率是多少？', 'image_uri': None,
             'normalized_question': '增长率是多少？', 'score': 0.84, 'selected': True},
        ]
        return dict(scenario=scenario, root=root, entry=ENTRY, params=PARAMS, expected=expected)

    if scenario == 'chain':
        form = options.get('form', 'module_helper')
        root = tmp_path / 'chain'
        root.mkdir()
        helper = 'e_helper_' + uuid.uuid4().hex[:8]
        sources = {helper: 'def delta():\n    return 1\n'}
        if form == 'module_helper':
            entry = 'e_entry_' + uuid.uuid4().hex[:8]
            sources[entry] = ('import ' + helper + '\n\n\n'
                              'def transform(row):\n'
                              '    return {"x": row["x"], "value": row["x"] + ' + helper + '.delta()}\n')
        else:
            entry = helper
        module, paths, bizdir = _load_business_modules(root, monkeypatch, sources, entry)
        rows_in = [{'x': i} for i in range(8)]
        expected = [{'x': i, 'value': i + 1} for i in range(8)]
        return dict(scenario=scenario, root=root, form=form, helper_name=helper,
                    helper_path=paths[helper], entry_module=module, bizdir=bizdir,
                    rows_in=rows_in, expected=expected)

    if scenario == 'prompt_journal':
        root = tmp_path / 'prompt-journal'
        root.mkdir()
        return dict(scenario=scenario, root=root)

    if scenario == 'prompt':
        root = tmp_path / 'prompt'
        root.mkdir()
        _prompt_setup(root, options.get('rows', 40))
        return dict(scenario=scenario, root=root,
                    rows=[{'id': i, 'item': str(i)} for i in range(options.get('rows', 40))])

    if scenario == 'keyed_lance':
        root = tmp_path / 'keyed'
        root.mkdir()
        rows = options.get('rows', [{'id': 'a', 'value': 'one'}, {'id': 'b', 'value': 'two'}])
        return dict(scenario=scenario, root=root, schema=KEYED_SCHEMA, rows=rows,
                    identity=options.get('identity', {'input': 1}),
                    expected=[dict(r) for r in rows])

    if scenario == 'queue':
        root = tmp_path / 'queue'
        root.mkdir(parents=True)
        config = QueueConfig(backend='sqlite', queue_id='e-queue', path=str(root / 'q.sqlite'))
        return dict(scenario=scenario, root=root, config=config, pool='p')

    if scenario == 'graph':
        root = tmp_path / 'graph'
        root.mkdir()
        return dict(scenario=scenario, root=root, n=options.get('n', 24))

    raise ValueError('unknown scenario ' + repr(scenario))


def run_workbench_scenario(root, *, entry, params):
    """Drive real prepare/start/show; read actual results, logs and sealed facts."""
    root = Path(root)
    started = time.monotonic()
    prepared = runs.prepare(root, entry, params=params)
    run_id = prepared['run_id']
    runs.start(root, run_id, expected=prepared['digest'])
    deadline = time.monotonic() + 120
    value = runs.show(root, run_id)
    while value['status'] not in runs.TERMINAL:
        if time.monotonic() > deadline:
            raise AssertionError('workbench run did not terminate: ' + value['status'])
        time.sleep(0.05)
        value = runs.show(root, run_id)
    target = runs.directory(root, run_id)
    journal = RunJournal(target)
    status = journal.execution_status()
    outputs = {o['name']: o for o in value['outputs']}
    rows = None
    for name, output in outputs.items():
        if output.get('result') is not None:
            sampled = project_ops.sample_table(root, 'run:' + run_id + ':' + name, limit=100)['rows']
            # _rowid is the preview scanner's internal Lance row id, not a saved column.
            rows = [{k: v for k, v in row.items() if k != '_rowid'} for row in sampled]
            break
    sealed = {}
    for action_id in value['actions']:
        sealed[action_id] = facts.read_sealed_plan(target, action_id)
    graph = graph_model.get(root, basis_kind='run', run_id=run_id, attempt=value['attempt'])
    return dict(run_id=run_id, attempt=value['attempt'], status=value['status'],
                execution_status=status, outputs=outputs, rows=rows, sealed=sealed,
                graph=graph, target=target,
                evidence=_resource_evidence(target, capacity=_CHAIN_CAPACITY, started=started))


def run_recovery_scenario(fixture, *, fault):
    """Interrupt a real run with this test's worker, resume the same Run.

    Returns before/after results, commits, budget, resources and request counts.
    """
    scenario = fixture['scenario']
    if scenario == 'chain':
        return _chain_recovery(fixture, fault=fault)
    if scenario in ('prompt', 'prompt_journal'):
        return _prompt_journal_recovery(fixture, fault=fault)
    if scenario == 'keyed_lance':
        return _keyed_recovery(fixture, fault=fault)
    if scenario == 'graph':
        return _graph_recovery(fixture, fault=fault)
    raise ValueError('recovery not defined for scenario ' + repr(scenario))


# ---------------------------------------------------------------------------
# Scenario drivers.
# ---------------------------------------------------------------------------

def _make_transform(form, helper, entry_module):
    if form == 'module_helper':
        return entry_module.transform
    if form == 'closure':
        def transform(row):
            return {'x': row['x'], 'value': row['x'] + helper.delta()}
        return transform
    if form == 'default_args':
        def transform(row, mod=helper):
            return {'x': row['x'], 'value': row['x'] + mod.delta()}
        return transform
    raise ValueError(form)


def _chain_recovery(fixture, *, fault):
    root = fixture['root']
    helper = importlib.import_module(fixture['helper_name'])
    transform = _make_transform(fixture['form'], helper, fixture['entry_module'])
    hook, marker = _crash_hook(root)
    rows_in = fixture['rows_in']

    def pipeline():
        return (data.from_items(rows_in).map_async(transform, concurrency=1)
                .map_async(hook, concurrency=1))

    def execute():
        with data.local_execution(workers=1, worker_mode='process', partitions=1):
            return pipeline().materialize().take_all()

    run_dir = root / 'run'
    started = time.monotonic()
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='worker.*(connection|confirmation)') as loss:
        with RunContext.start(run_dir, limits=LIMITS) as run:
            reference = run.ref
            execute()
    assert loss.value.code == 'UNKNOWN_EFFECT'
    assert marker.exists(), 'first run must crash after acked cuts, not fail at load'
    killed = json.loads(marker.read_text())
    assert killed['pid'] != os.getpid() and killed['row'] == 5
    prefix = RunJournal(run_dir).execution_commits('action-1')
    assert prefix and all(f['status'] == 'acked' for f in prefix), 'crash must retain a genuine ACK prefix'
    def resource_evidence():
        value = _resource_evidence(run_dir, capacity=_CHAIN_CAPACITY, started=started)
        # v3 uses the selected deployment capacity domain, not run-resources.
        from demiflow.execution.dataflow_capacity import local_capacity
        domain, capacity = local_capacity()
        value['capacity_charged'] = LocalCapacityPool(domain / 'capacity', capacity=capacity).snapshot()['charged']
        return value
    before = resource_evidence()
    assert before['capacity_charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


    if fault == 'edit_helper':
        fixture['helper_path'].write_text('def delta():\n    return 9999\n')
        importlib.reload(helper)

    rows, commits, refused = None, None, False
    if fault == 'edit_helper':
        with pytest.raises(DataflowError, match='Loaded program code differs from the sealed pin') as excinfo:
            with RunContext.resume(reference):
                execute()
        assert excinfo.value.code == 'INVALID_CONTRACT'
        assert not isinstance(excinfo.value, TimeoutError)
        refused = True
        assert RunJournal(run_dir).execution_commits('action-1') == prefix
    else:
        with RunContext.resume(reference) as run2:
            rows = execute()
            commits = run2.journal.execution_commits(_chain_child_id(run2))
    after = resource_evidence()
    _release_orphans(run_dir, capacity=_CHAIN_CAPACITY)
    return dict(before=before, after=after, rows=rows, commits=commits,
                refused=refused, reference=reference)


class _SimulatedPromptLoss(Exception):
    """First-attempt transport/process loss after the effect was reserved."""


def _prompt_journal_recovery(fixture, *, fault):
    run_dir = fixture['root'] / 'run'
    request = {'kind': 'effect', 'id': fault, 'native_response_bytes': 1024}
    started = time.monotonic()
    # The first attempt reserves the effect then loses the transport, leaving the
    # run failed (resumable) — never cleanly succeeded, which cannot be resumed.
    with pytest.raises(_SimulatedPromptLoss):
        with RunContext.start(run_dir) as run:
            reference = run.ref
            journal = run._native_prompt_journal = RunPromptJournal(run)
            CALL_RESERVATION.set(('attempt-one', {'requests': (1, 10)}))
            assert journal.reserve(request) is True
            if fault == 'known_completed':
                journal.response(request, {'ok': True})
            elif fault == 'confirmed_unsent':
                error = RuntimeError('cancelled in transport batch queue')
                error.call = {'request_sent': False, 'batch_cancelled_before_dispatch': True}
                journal.failed(request, error, .01)
            journal.close()  # flush the batched native journal before the loss
            raise _SimulatedPromptLoss('transport loss after effect reservation')
    before = _resource_evidence(run_dir, capacity=_CHAIN_CAPACITY, started=started)

    reused, raised = None, False
    with RunContext.resume(reference) as run2:
        journal2 = run2._native_prompt_journal = RunPromptJournal(run2)
        if fault == 'unknown':
            with pytest.raises(UncertainPromptCall, match='automatic regeneration is forbidden'):
                journal2.check_recovery()
            raised = True
        else:
            journal2.check_recovery()
            CALL_RESERVATION.set(('attempt-two', {'requests': (1, 10)}))
            reused = journal2.reserve(request)
        budget = run2.journal.budget_consumed('requests')
        journal2.close()
    after = _resource_evidence(run_dir, capacity=_CHAIN_CAPACITY, started=started)
    return dict(before=before, after=after, reused=reused, refused=raised,
                requests_budget=budget, reference=reference)


def _keyed_recovery(fixture, *, fault):
    """Native keyed writer recovery: an ambiguous commit is reconciled in place.

    On current disk the standalone StreamCheckpoint orchestration is retired and
    the public delivery-only run_stream resume needs a P3 native sink adapter
    (reported separately as a C integration gap). The real keyed-write recovery
    authority that IS available is StreamLanceWriter's own commit reconciliation:
    a transport loss after the native Lance commit resolves the committed version
    instead of blindly resending, and a replay of the same logical keys is
    idempotent under the append contract.
    """
    import demiflow.execution.stream_lance as stream_lance
    from demiflow.execution.stream_lance import StreamLanceWriter
    root = fixture['root']
    schema, rows = fixture['schema'], fixture['rows']
    target = root / 'out.lance'
    started = time.monotonic()
    writer = StreamLanceWriter(target, schema, key='id', mode='append')
    writer.initialize()
    real_write = stream_lance.write_lance
    commits = [0]

    def commit_then_loss(spec, tables, **kwargs):
        commits[0] += 1
        receipt = real_write(spec, tables, **kwargs)  # the native commit really lands
        raise OSError('connection lost after commit')

    stream_lance.write_lance = commit_then_loss
    try:
        forwarded = asyncio.run(writer(rows))  # reconciles the ambiguous commit, no blind resend
    finally:
        stream_lance.write_lance = real_write
    before = dict(commits=commits[0], version=writer.version,
                  committed_rows=lance.dataset(str(target)).count_rows(),
                  forwarded=forwarded,
                  evidence=_resource_evidence(root, capacity=_CHAIN_CAPACITY, started=started))
    assert commits[0] == 1, 'an ambiguous commit must be reconciled, not resent'
    # A fresh writer replays the same logical keys/content: append stays idempotent.
    replay = StreamLanceWriter(target, schema, key='id', mode='append')
    replay.initialize()
    asyncio.run(replay(rows))
    final_rows = lance.dataset(str(target)).to_table().to_pylist()
    after = dict(ref=replay.reference(), final_rows=final_rows, commits=commits[0],
                 evidence=_resource_evidence(root, capacity=_CHAIN_CAPACITY, started=started))
    return dict(before=before, after=after)


def _graph_spec(fixture, *, target_fns, mode='memory_incremental', cut_rows=None,
                credit_rows=8, n=None):
    n = fixture['n'] if n is None else n

    def shared_double(row):
        return {**row, 'x': row['x'] * 2}

    items = tuple({'x': i} for i in range(n))
    readers = list(target_fns)
    targets = [{'plan': _stream_map(target_fns[rid]), 'logical_node_id': 'node-' + rid,
                'occurrence_id': rid + '-p0', 'reader_id': rid} for rid in readers]
    spec = {'source': ItemsSource(items), 'source_id': 'e-graph-source',
            'ancestor': {'plan': _stream_map(shared_double),
                         'logical_node_id': 'shared', 'occurrence_id': 'shared-p0'},
            'targets': targets, 'routing': 'broadcast', 'exchange_mode': mode,
            'credit_budget': {'rows': credit_rows, 'bytes': 8 * 1024 ** 2},
            'queue_depth': 4096}
    if cut_rows is not None:
        spec['cut_rows'] = cut_rows
    return spec, readers, n


def _graph_committed_rows(run, action_id, rid, facts):
    sink = BlockSink(RunAction(run, action_id + '-graph-' + rid, {'kind': 'graph-node'}), rid)
    rows = []
    for fact in facts:
        rows.extend(sink.iter_rows(fact['receipts'][rid]['data']))
    return rows


def _graph_recovery(fixture, *, fault):
    root = fixture['root']
    crash_marker = root / 'target-crashed-once'

    def crashing_target(row):
        if row['x'] == 16:
            try:
                fd = os.open(crash_marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
            except FileExistsError:
                return row
            os.kill(os.getpid(), signal.SIGKILL)
        return row

    def passthrough(row):
        return row

    spec, readers, n = _graph_spec(fixture, target_fns={'c0': crashing_target, 'c1': passthrough},
                                   mode='durable_incremental', cut_rows=8)
    executor = LocalDatasetExecutor(workers=1)
    run_dir = root / 'run'
    started = time.monotonic()
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(run_dir, limits=GRAPH_LIMITS) as run:
            reference = run.ref
            with run.action({'kind': 'materialize', 'source': {'fixture': 'e-graph-fault'}}) as execution:
                executor.materialize_graph(spec)
                execution.complete(dict(kind='graph-materialized'))
    assert crash_marker.exists(), 'the consumer crash window did not fire'
    before = _resource_evidence(run_dir, capacity=_GRAPH_CAPACITY, started=started)

    with RunContext.resume(reference, limits=GRAPH_LIMITS) as run2:
        with run2.action({'kind': 'materialize', 'source': {'fixture': 'e-graph-fault'}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind='graph-materialized', egress=result['egress_rows']))
        facts = run2.journal.execution_commits(action_id + '-graph-result')
        committed = {rid: _graph_committed_rows(run2, action_id, rid, facts) for rid in readers}
    after = _resource_evidence(run_dir, capacity=_GRAPH_CAPACITY, started=started)
    _release_orphans(run_dir, capacity=_GRAPH_CAPACITY)
    return dict(before=before, after=after, result=result, commits=facts,
                committed=committed, readers=readers, n=n, reference=reference)


# ---------------------------------------------------------------------------
# Prompt end-to-end setup (real PromptActor through map_prompt_async + fake transport).
# ---------------------------------------------------------------------------

def _prompt_load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _prompt_setup(root, count):
    from demiflow.data.flow_contract import arrow_fields
    from demiflow.forge.flow_check import FlowChecker
    from demiflow.forge.platform import LIMITS as PLATFORM_LIMITS, inspect_modules
    from demiflow.forge.project_bindings import resolve_flow_resources
    (root / 'flows').mkdir()
    (root / 'operators').mkdir()
    (root / 'flows/prompt.yaml').write_text(PACK.replace(
        'base_url_env: TEST_PROMPT_URL', 'base_url: ' + os.environ['TEST_PROMPT_URL']))
    (root / 'operators/capture.py').write_text(
        'from typing import TypedDict\n'
        'class Input(TypedDict):\n    id: int | None\n    answer: str\n'
        'class Output(TypedDict):\n    id: int | None\n    value: str\n'
        'def finish(row: Input) -> Output:\n'
        "    return {'id': row['id'], 'value': row['answer'] + ' transformed'}\n")
    (root / 'flows/recovery.py').write_text(
        'from demiflow import data\n'
        'from operators.capture import finish\n'
        'def run(*, source, target):\n'
        "    rows = data.read_lance(source, version=1).map_prompt_async('enrich', config='flows/prompt.yaml',\n"
        "        inputs={'payload':'item'}, output='answer', concurrency=8, queue_depth=16,\n"
        "        max_requests=2048, options={'trust_env':False, 'timeout_s':5,\n"
        "            'max_request_bytes':1048576, 'max_response_bytes':1048576}).map(finish).materialize()\n"
        "    return rows.write_lance(target, mode='create', return_receipt=True)\n")
    rows = [{'id': i, 'item': str(i)} for i in range(count)]
    table = pa.Table.from_pylist(rows)
    source, target = str(root / 'input.lance'), str(root / 'out.lance')
    lance.write_dataset(table, source)
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], PLATFORM_LIMITS)
    report = FlowChecker(root, catalog,
                         {'input': {'uri': source, 'version': 1, 'fields': arrow_fields(table.schema)}},
                         [target],
                         resource_resolver=lambda tree, refs, bindings: resolve_flow_resources(root, tree, refs, bindings)
                         ).inspect('flows/recovery.py:run', {'source': source, 'target': target})
    assert report['status'] == 'checked', report['diagnostics']
    (root / 'report.json').write_text(json.dumps(report))


def _prompt_execute(root):
    from demiflow.execution.checked import CheckedExecution
    from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
    root = Path(root)
    os.chdir(root)
    _prompt_load('operators.capture', root / 'operators/capture.py')
    flow = _prompt_load('flows.recovery', root / 'flows/recovery.py')
    report = json.loads((root / 'report.json').read_text())
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
                               source_root=root, resource_root=root / 'runtime',
                               limits={**DEFAULT_LIMITS, 'memory_bytes': 1024 ** 3},
                               record=lambda *args: None, check_stop=lambda: None)
    directory = root / 'runtime'
    prior = RunJournal(directory).recovery_identity()
    scope = (RunContext.resume(RunRef(str(directory), prior['run_id'])) if prior
             else RunContext.start(directory, limits={'memory_bytes': 1024 ** 3}))
    with scope, data.local_execution(workers=1, worker_mode='thread', partitions=1,
                                     memory_bytes=128 * 1024 ** 2) as session:
        session._executor.resource_root = root
        with checked.bind_context():
            flow.run(source=str(root / 'input.lance'), target=str(root / 'out.lance'))


# ---------------------------------------------------------------------------
# Queue seeding through the real native publication protocol.
# ---------------------------------------------------------------------------

def _queue_source():
    return ProducerSourceSpec(kind='manifest', source_id='e-fixture-source',
                              uri='file:///fixture', sha256='1' * 64)


def _queue_publish(parent, channel, producer, index, messages=(), eos=False):
    occurrence = identity(['e-fixture', index, 'eos' if eos else 'tasks'])
    action = producer_action(parent, channel, producer, occurrence, 'e-fixture-contract')
    sink = QueuePublishSink(action, producer, 'e-fixture-contract')
    prepared = dict(format=2, occurrence=occurrence, source_position=index, output_ordinal=0,
                    messages=list(messages), unit_end=True, eos=eos)
    return CommitCoordinator(action).commit(
        1, outputs={'publish': (sink, prepared)},
        position=SourcePosition(producer.source.source_id, index))


def _queue_seed(fixture, url, *, count=3):
    root = fixture['root']
    channel = QueueChannel(fixture['config'])
    producer = channel.open(fixture['pool'], producer_id='e-seed', source=_queue_source())
    with RunContext.start(root / 'seed-run') as run, run.action({'kind': 'producer-fixture'}) as parent:
        messages = [task_message('t' + str(i), {'request': {'uri': url + '/file'}}, pool=fixture['pool'])
                    for i in range(count)]
        _queue_publish(parent, channel, producer, 0, messages)
        _queue_publish(parent, channel, producer, 1, [], eos=True)
        parent.complete({'kind': 'producer-fixture', 'finished': True})
    channel.seal(producer)
    return channel


def _queue_settle(fixture, root, *, url, unified, max_tasks=10000):
    cfg = fixture['config']

    class PureAnnotation:
        def __call__(self, row):
            return {**row, 'accepted': False, 'temporary': 'remove-me'}

    def receipt_status(result):
        return result['status']

    def annotate(status):
        return 'downloaded' if status == 'ok' else 'failed'

    with RunContext.start(root, limits=LIMITS):
        with data.local_execution(workers=1, worker_mode='thread', partitions=1):
            ds = data.read_queue(cfg, pool=fixture['pool'], max_tasks=max_tasks,
                                 max_inflight=1, idle_timeout_s=.2).fetch(
                store=LocalObjectStore(root / 'objects'), concurrency=1,
                policy=FetchPolicy(max_object_bytes=131072, http=HttpSource(allow_private=True)))
            ds = (ds.map(PureAnnotation).drop_columns(['request', 'temporary'])
                  .rename_columns({'accepted': 'business_accepted'})
                  .select_columns(['result', 'business_accepted', '_queue_delivery'])
                  .map(receipt_status, inputs={'result': 'result'}, output='status')
                  .map(annotate, inputs={'status': 'status'}, output='label'))
            assert ds.write_datasink(QueueResultSink(cfg, pool=fixture['pool'])) is None


from demiflow.collect import FetchPolicy, HttpSource  # noqa: E402
from demiflow.objects import LocalObjectStore  # noqa: E402


# ===========================================================================
# Scenario 1: workbench normal flow (unified chain, real independent worker).
# ===========================================================================

def test_workbench_normal_flow_real_worker(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='workbench')
    observed = run_workbench_scenario(fixture['root'], entry=fixture['entry'], params=fixture['params'])
    assert observed['status'] == 'succeeded', observed['execution_status']
    assert observed['rows'] is not None
    assert_semantics_equal(fixture['expected'], observed['rows'], ordering='unordered')
    # Unedited sealed code is not falsely rejected; the run reached a committed output.
    assert any(o['status'] == 'committed' for o in observed['outputs'].values())
    # A real independent worker process executed the run (pid differs from the test).
    worker = observed['execution_status'].get('worker')
    assert worker is None or worker.get('pid') != os.getpid()
    # The display graph is readable from the checked report and bound to this run.
    assert observed['graph'] is not None
    assert observed['graph']['binding']['run_id'] == observed['run_id']
    assert observed['graph']['completeness']['total_nodes'] > 0
    # New runs must display their executed v3 graph and a complete identity
    # bridge; a declared projection or an unavailable result is not acceptance.
    assert observed['sealed'], 'no sealed-plan reader results for the run actions'
    for action_id, sealed in observed['sealed'].items():
        assert sealed.get('action_id') == action_id
        assert sealed.get('available') is True, sealed
        assert sealed['version'] == 'v3'
        dag = sealed['plan_view']['dag']
        assert dag['nodes']
        from demiflow.execution.run_journal import RunJournal
        import json
        journal = RunJournal(observed['target'])
        bridge_ref = journal.source_binding(action_id + '-identity-bridge')
        assert bridge_ref is not None
        bridge = json.loads((observed['target'] / bridge_ref['path']).read_text())
        assert bridge['dag_id'] == sealed['dag_id']
        assert {e['occurrence_id'] for e in bridge['entries']} == {n['occurrence_id'] for n in dag['nodes']}
        report_ids = {n['id'] for n in observed['graph']['nodes']}
        assert {e['report_node_id'] for e in bridge['entries']} <= report_ids
        with journal._connection() as db:
            docs = [json.loads(row[0]) for row in db.execute(
                'SELECT document FROM node_observations WHERE attempt=? AND action_id=?',
                (observed['attempt'], action_id))]
        assert len(docs) >= len(bridge['entries'])
        assert all(d['rows_output'] is not None and d['state'] == 'succeeded' for d in docs)
    # Real resource state after success: capacity released, single control database.
    assert observed['evidence']['capacity_charged'] in (None, {'cpu_millis': 0, 'memory_bytes': 0})


def test_workbench_cancel_releases_resources(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='workbench')
    root = fixture['root']
    # A slow flow so cancellation can land while running. Forge admits only
    # module-level operator compute boundaries wired by the flow (a nested
    # FunctionDef or an unadmitted import like time is rejected), so the delay
    # is a pure-builtin busy loop in a registered operator.
    (root / 'operators/slow_rules.py').write_text(
        'def slow_marker(case_id: str) -> str:\n'
        '    sum(range(20000000))\n'
        '    return case_id\n')
    (root / 'flows/slow.py').write_text(
        'from demiflow import data\n'
        'from operators.slow_rules import slow_marker\n'
        'def run(*, candidates, target):\n'
        '    return (data.read_lance(candidates["uri"], version=candidates["version"])\n'
        '            .map(slow_marker, inputs={"case_id": "case_id"}, output="slow_marker")\n'
        '            .write_lance(target["uri"], mode="create", return_receipt=True))\n')
    (root / 'flows/slow.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v1',
        'arguments': {'candidates': {'kind': 'table', 'alias': 'candidates'},
                      'target': {'kind': 'output', 'location_id': 'local',
                                 'relative_uri': 'slow_out.lance'}}}))
    declare_existing_flow(root, 'flows/slow.py:run', params='flows/slow.json')
    prepared = runs.prepare(root, 'flows/slow.py:run', params='flows/slow.json')
    run_id = prepared['run_id']
    runs.start(root, run_id, expected=prepared['digest'])
    deadline = time.monotonic() + 30
    value = runs.show(root, run_id)
    while value['status'] != 'running' and value['status'] not in runs.TERMINAL:
        if time.monotonic() > deadline:
            break
        time.sleep(0.02)
        value = runs.show(root, run_id)
    if value['status'] == 'running':
        runs.cancel(root, run_id, expected_attempt=value['attempt'])
    deadline = time.monotonic() + 60
    value = runs.show(root, run_id)
    while value['status'] not in runs.TERMINAL:
        assert time.monotonic() < deadline, 'cancel did not terminate the run'
        time.sleep(0.05)
        value = runs.show(root, run_id)
    assert value['status'] in {'cancelled', 'succeeded'}
    target = runs.directory(root, run_id)
    evidence = _resource_evidence(target, capacity=_CHAIN_CAPACITY)
    assert evidence['capacity_charged'] in (None, {'cpu_millis': 0, 'memory_bytes': 0})


# ===========================================================================
# Scenario 2: function-dependency recovery (chain; shared code pin).
# ===========================================================================

def test_chain_unedited_resume_recovers_exactly_once(tmp_path, monkeypatch, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='chain', monkeypatch=monkeypatch,
                                  form='module_helper')
    observed = run_recovery_scenario(fixture, fault='crash_worker')
    assert observed['refused'] is False
    assert_semantics_equal(fixture['expected'], observed['rows'], ordering='declared')
    sequences = [f['sequence'] for f in observed['commits']]
    assert sequences == list(range(1, len(sequences) + 1))
    assert all(f['status'] == 'acked' for f in observed['commits'])
    assert len({f['commit_id'] for f in observed['commits']}) == len(observed['commits'])
    assert observed['after']['capacity_charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


@pytest.mark.parametrize('form', ['module_helper', 'closure', 'default_args'])
def test_chain_edited_business_dependency_resume_refused(tmp_path, monkeypatch, pidfds, form):
    fixture = build_owned_fixture(tmp_path, scenario='chain', monkeypatch=monkeypatch, form=form)
    observed = run_recovery_scenario(fixture, fault='edit_helper')
    assert observed['refused'] is True
    assert observed['rows'] is None, 'a refused resume must not produce mixed results'
    # The sealed prefix stays acked; no new-code (9999) row was ever committed.
    assert observed['after']['capacity_charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


# ===========================================================================
# Scenario 3: model calls and unknown effects (real journal + fake transport).
# ===========================================================================

@pytest.mark.parametrize('fault', ['known_completed', 'unknown', 'confirmed_unsent'])
def test_prompt_journal_recovery_three_states(tmp_path, fault):
    fixture = build_owned_fixture(tmp_path, scenario='prompt_journal')
    observed = run_recovery_scenario(fixture, fault=fault)
    if fault == 'known_completed':
        assert observed['refused'] is False
        assert observed['reused'] is False, 'known_completed must reuse without a new reservation'
        assert observed['requests_budget'] == 1, 'no new request budget on reuse'
    elif fault == 'unknown':
        assert observed['refused'] is True, 'unknown must fail closed, never blind resend'
    else:
        assert observed['refused'] is False
        assert observed['reused'] is True, 'confirmed-unsent may re-dispatch under the original rule'
        assert observed['requests_budget'] == 2, 're-dispatch consumes budget again'


def test_prompt_end_to_end_crash_resume_no_blind_resend(server, tmp_path):
    fixture = build_owned_fixture(tmp_path, scenario='prompt', rows=40)
    root = fixture['root']
    repo = Path(__file__).resolve().parents[1]
    script = (
        'import os, sys\n'
        'from test_upgrade_business_acceptance import _prompt_execute\n'
        'from demiflow.operator_llm.runtime import PromptActor\n'
        'original = PromptActor.__call__\n'
        'calls = [0]\n'
        'async def fail(self, row):\n'
        '    result = await original(self, row)\n'
        '    calls[0] += 1\n'
        '    if calls[0] == 20:\n'
        '        os._exit(37)\n'
        '    return result\n'
        'PromptActor.__call__ = fail\n'
        '_prompt_execute(sys.argv[1])\n')
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(repo), str(repo / 'tests')]),
           'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1'}
    child = subprocess.run([sys.executable, '-B', '-c', script, str(root)], env=env,
                           capture_output=True, text=True, timeout=180)
    assert child.returncode == 37, child.stdout + child.stderr
    assert not (root / 'out.lance').exists()
    old_calls = len(server['requests'])
    with sqlite3.connect(root / 'runtime/run.sqlite') as db:
        uncertain = db.execute('SELECT count(*) FROM calls WHERE response_json IS NULL').fetchone()[0]
    if uncertain:
        with pytest.raises(UncertainPromptCall, match='automatic regeneration is forbidden'):
            _prompt_execute(root)
        assert len(server['requests']) == old_calls, 'unknown effects must not be resent'
        assert not (root / 'out.lance').exists()
        assert RunJournal(root / 'runtime').execution_status()['status'] == 'failed'
    else:
        _prompt_execute(root)
        result = lance.dataset(str(root / 'out.lance')).to_table().to_pylist()
        assert sorted(r['id'] for r in result) == sorted(r['id'] for r in fixture['rows'])
        assert all(r['value'] == 'ok transformed' for r in result)
        assert len(server['requests']) == len(fixture['rows']), 'known_completed must not add requests'
        assert RunJournal(root / 'runtime').execution_status()['status'] == 'succeeded'


# ===========================================================================
# Scenario 4: keyed save_lance (native StreamLanceWriter commit authority).
# ===========================================================================

def test_keyed_native_writer_interrupt_append_recovery(tmp_path):
    fixture = build_owned_fixture(tmp_path, scenario='keyed_lance')
    observed = run_recovery_scenario(fixture, fault='ambiguous_commit')
    assert observed['before']['commits'] == 1
    assert observed['before']['committed_rows'] == len(fixture['rows'])
    assert_semantics_equal(fixture['expected'], observed['after']['final_rows'], ordering='unordered')
    # Idempotent append replay: same logical keys, no duplicate rows.
    assert lance.dataset(**observed['after']['ref']).count_rows() == len(fixture['rows'])
    assert observed['after']['commits'] == 1, 'replay of known keys must not re-commit'


def test_keyed_append_conflicting_content_refused(tmp_path):
    fixture = build_owned_fixture(tmp_path, scenario='keyed_lance',
                                  rows=[{'id': 'a', 'value': 'one'}])
    root = fixture['root']
    uri = root / 'out.lance'

    def save(rows):
        return (data.from_items(rows).save_lance(uri, schema=KEYED_SCHEMA, key='id',
                                                 stage='out', mode='append', max_batch=1).run_stream())

    save([{'id': 'a', 'value': 'one'}])
    with pytest.raises(ValueError, match='conflicting'):
        save([{'id': 'a', 'value': 'changed'}])
    assert lance.dataset(str(uri)).count_rows() == 1


def test_keyed_overwrite_duplicate_key_refused(tmp_path):
    # The managed public save_lance contract now requires append; the native keyed
    # writer still enforces overwrite uniqueness directly.
    from demiflow.execution.stream_lance import StreamLanceWriter
    root = tmp_path / 'keyed'
    root.mkdir()
    writer = StreamLanceWriter(root / 'out.lance', KEYED_SCHEMA, key='id', mode='overwrite')
    writer.initialize()
    with pytest.raises(ValueError, match='unique'):
        asyncio.run(writer([{'id': 'a', 'value': 'one'}, {'id': 'a', 'value': 'two'}]))


def test_keyed_public_save_lance_append_commits(tmp_path):
    fixture = build_owned_fixture(tmp_path, scenario='keyed_lance')
    root = fixture['root']
    with RunContext.start(root / 'run') as run:
        stats = (data.from_items(fixture['rows'])
                 .save_lance(root / 'out.lance', schema=KEYED_SCHEMA, key='id',
                             stage='out', mode='append', max_batch=1).run_stream())
        assert stats.outputs['out']['version'] >= 1
    rows = lance.dataset(str(root / 'out.lance')).to_table().to_pylist()
    assert_semantics_equal(fixture['expected'], rows, ordering='unordered')


def test_keyed_oversized_row_refused_before_commit(tmp_path):
    from demiflow.execution.stream_lance import StreamLanceWriter
    root = tmp_path / 'keyed'
    root.mkdir()
    target = root / 'out.lance'
    writer = StreamLanceWriter(target, KEYED_SCHEMA, key='id', mode='append', max_row_bytes=512)
    writer.initialize()
    with pytest.raises(ValueError):
        asyncio.run(writer([{'id': 'a', 'value': 'x' * 4096}]))
    assert lance.dataset(str(target)).count_rows() == 0


# ===========================================================================
# Scenario 5: queue publish / consume / settle / fixed-prefix history.
# ===========================================================================

def test_queue_settle_and_fixed_prefix_history(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='queue')
    with origin_server() as (url, calls):
        channel = _queue_seed(fixture, url, count=3)
        _queue_settle(fixture, fixture['root'] / 'settle', url=url, unified=True)
        fetches = calls['/file']
    assert fetches == 3, 'each task acquired exactly once'
    assert channel.state(fixture['pool'])['queue_drained']
    assert channel.state(fixture['pool'])['counts'] == {'done_ok': 3}
    # Fixed publication prefix: results history with native ACK + proof + types.
    snapshot = channel.snapshot(fixture['pool'])
    rows = data.read_queue_records(fixture['config'], pool=fixture['pool'],
                                   through=snapshot, results=True).take_all()
    assert len(rows) == 3
    assert [r['queue_record']['ordinal'] for r in rows] == [1, 2, 3]
    for row in rows:
        assert row['verification']['status'] == 'verified'
        assert row['result']['status'] == 'ok'
        assert row['queue_record']['proof']['settled'] is True
        assert type(row['queue_record']['task_id']) is str
    # Task prefix preserves payload types and occurrence identity.
    tasks = data.read_queue_records(fixture['config'], pool=fixture['pool'],
                                    through=channel.snapshot(fixture['pool'], kind='tasks')).take_all()
    assert len(tasks) == 3
    assert all(type(t['payload']) is dict and 'request' in t['payload'] for t in tasks)
    assert all(t['verification']['status'] == 'verified' for t in tasks)


def test_queue_history_unknown_effect_excluded_from_prefix(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='queue')
    with origin_server() as (url, calls):
        channel = _queue_seed(fixture, url, count=3)
        # Settle only one task; the other two remain claimed/unfinished (unknown effect).
        _queue_settle(fixture, fixture['root'] / 'partial', url=url, unified=True, max_tasks=1)
    snapshot = channel.snapshot(fixture['pool'])
    assert snapshot.cursor == 1, 'unsettled tasks must stay outside the results prefix'
    rows = data.read_queue_records(fixture['config'], pool=fixture['pool'],
                                   through=snapshot, results=True).take_all()
    assert len(rows) == 1
    assert channel.state(fixture['pool'])['counts'].get('done_ok') == 1
    assert not channel.state(fixture['pool'])['queue_drained']


def test_queue_history_budget_cursor_resumes(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='queue')
    with origin_server() as (url, calls):
        channel = _queue_seed(fixture, url, count=3)
        _queue_settle(fixture, fixture['root'] / 'settle', url=url, unified=True)
    snapshot = channel.snapshot(fixture['pool'])
    with pytest.raises(HistoryReadLimit) as caught:
        data.read_queue_records(fixture['config'], pool=fixture['pool'], through=snapshot,
                                results=True, limits=HistoryReadLimits(max_rows=2)).take_all()
    resumed = data.read_queue_records(fixture['config'], pool=fixture['pool'], through=snapshot,
                                      results=True, after=caught.value.next_cursor).take_all()
    assert len(resumed) == 1
    assert resumed[0]['queue_record']['ordinal'] == 3


def test_queue_unified_carrier_matches_legacy(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='queue')
    # A second independent queue for the unified carrier, served by the SAME
    # loopback origin so the endpoint identity is identical and the comparison
    # isolates the carrier, not an ephemeral port.
    fixture2 = build_owned_fixture(tmp_path / 'u', scenario='queue')
    with origin_server() as (url, calls):
        channel = _queue_seed(fixture, url, count=2)
        _queue_settle(fixture, fixture['root'] / 'legacy', url=url, unified=False)
        legacy_fetches = calls['/file']
        channel2 = _queue_seed(fixture2, url, count=2)
        _queue_settle(fixture2, fixture2['root'] / 'unified', url=url, unified=True)
        unified_fetches = calls['/file'] - legacy_fetches
    legacy_rows = data.read_queue_records(fixture['config'], pool=fixture['pool'],
                                          through=channel.snapshot(fixture['pool']), results=True).take_all()
    unified_rows = data.read_queue_records(fixture2['config'], pool=fixture2['pool'],
                                           through=channel2.snapshot(fixture2['pool']), results=True).take_all()

    def business(row):
        result = {k: v for k, v in row['result'].items()
                  if k not in {'retrieved_at', 'acquisition_ref', 'storage_domain'}}
        if result.get('raw_ref'):
            result['raw_ref'] = {'sha256': result['raw_ref']['sha256']}
        return dict(result=result, business_accepted=row['business_accepted'], label=row['label'])

    assert [business(r) for r in legacy_rows] == [business(r) for r in unified_rows]
    assert legacy_fetches == unified_fetches == 2, 'each carrier fetches each object once'
    # The unified carrier supervised a genuine independent managed worker process.
    units = fixture2['root'] / 'unified-resources' / 'units'
    pids = {json.loads(p.read_text())['identity']['pid'] for p in units.glob('*/*/process.json')}
    assert pids and os.getpid() not in pids


# ===========================================================================
# Scenario 6: static graph (explicit entry, real independent workers).
# ===========================================================================

def test_graph_broadcast_multi_consumer_tuple_bytes(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='graph', n=12)

    def ancestor(row):
        return {**row, 'x': row['x'] * 2, 't': (row['x'], row['x'] + 1),
                'b': bytes([row['x'] % 256])}

    def passthrough(row):
        return row

    spec, readers, n = _graph_spec(fixture, target_fns={'c0': passthrough, 'c1': passthrough},
                                   credit_rows=4)
    # Override the ancestor to emit tuple/bytes fidelity payloads.
    spec['ancestor']['plan'] = _stream_map(ancestor)
    executor = LocalDatasetExecutor(workers=1)
    with RunContext.start(fixture['root'] / 'run', limits=GRAPH_LIMITS) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'e-graph-types'}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind='graph-materialized', egress=result['egress_rows']))
        facts = run.journal.execution_commits(action_id + '-graph-result')
        committed = {rid: _graph_committed_rows(run, action_id, rid, facts) for rid in readers}
    assert result['egress_rows'] == n, 'shared ancestor must expand each record once'
    assert result['workers'] == 3
    for rid in readers:
        rows = committed[rid]
        assert len(rows) == n
        for row in rows:
            assert type(row['t']) is tuple and len(row['t']) == 2
            assert type(row['b']) is bytes
            assert row['t'] == (row['x'] // 2, row['x'] // 2 + 1)


def test_graph_slow_consumer_small_budget_no_loss(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='graph', n=24)

    def passthrough(row):
        return row

    def slow(row):
        time.sleep(0.005)
        return row

    spec, readers, n = _graph_spec(fixture, target_fns={'fast': passthrough, 'slow': slow},
                                   credit_rows=4)
    executor = LocalDatasetExecutor(workers=1)
    started = time.monotonic()
    with RunContext.start(fixture['root'] / 'run', limits=GRAPH_LIMITS) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'e-graph-slow'}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind='graph-materialized', egress=result['egress_rows']))
    assert result['egress_rows'] == n
    for rid in readers:
        assert result['target_rows'][rid] == n, (rid, result['target_rows'][rid])
    arrival = result['final_arrival']
    assert arrival['fast'] <= arrival['slow'], 'slow consumer must not block the fast target'
    evidence = _resource_evidence(fixture['root'] / 'run', capacity=_GRAPH_CAPACITY, started=started)
    assert evidence['capacity_charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_graph_consumer_crash_recovers_exactly_once(tmp_path, pidfds):
    fixture = build_owned_fixture(tmp_path, scenario='graph', n=24)
    observed = run_recovery_scenario(fixture, fault='crash_target')
    assert [f['sequence'] for f in observed['commits']] == [1, 2, 3]
    assert all(f['status'] == 'acked' for f in observed['commits'])
    assert len({f['commit_id'] for f in observed['commits']}) == len(observed['commits'])
    for rid in observed['readers']:
        rows = observed['committed'][rid]
        assert [r['x'] for r in rows] == [i * 2 for i in range(observed['n'])], rid
    assert observed['result']['egress_rows'] == observed['n']
    assert observed['after']['capacity_charged'] == {'cpu_millis': 0, 'memory_bytes': 0}