"""Owned process-loss evidence for migrated historical SIGKILL fixtures."""
import json
import os
from pathlib import Path
import signal


def kill_owned_worker(marker, driver_pid):
    """Keep the real fatal signal, but never permit an in-driver thread kill."""
    import psutil
    pid = os.getpid()
    assert pid != driver_pid, 'SIGKILL fixture requires an independent owned worker'
    with Path(marker).open('x') as stream:
        json.dump({'pid': pid, 'driver_pid': driver_pid,
                   'created': psutil.Process().create_time(), 'signal': 'SIGKILL'}, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.kill(pid, signal.SIGKILL)
    raise AssertionError('SIGKILL unexpectedly returned')


def process_records(directory):
    directory = Path(directory)
    units = directory.with_name(directory.name+'-driver-resources')/'units'
    paths = list(units.glob('*/*/process.json'))
    assert len(paths) <= 512, 'fixture exceeded its process evidence bound'
    return {path.parent.name: (path, json.loads(path.read_text())) for path in paths}


def assert_worker_killed(marker, directory):
    import psutil
    fact = json.loads(Path(marker).read_text())
    assert fact['driver_pid'] == os.getpid() and fact['pid'] != os.getpid()
    assert fact['signal'] == 'SIGKILL'
    try:
        process = psutil.Process(fact['pid'])
        assert process.create_time() != fact['created'] or process.status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        pass
    records = process_records(directory)
    assert any(v['identity']['pid'] == fact['pid'] for _, v in records.values())
    return fact


def assert_worker_loss(error):
    """Permit the typed v3 transport failure, never an arbitrary flow error."""
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    if isinstance(error, DataflowError):
        assert error.code == 'UNKNOWN_EFFECT'


def materialize_action_id(run):
    with run.journal._connection() as db:
        rows = db.execute('SELECT action_id,declaration FROM runtime_actions').fetchall()
    actions = [key for key, raw in rows if json.loads(raw)['kind'] == 'materialize']
    assert len(actions) == 1, 'one real materialize action must own the cut/receipt history'
    return actions[0]


def action_process_records(run, action_id):
    """Select physical hosts using the action's authenticated original plan."""
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    from demiflow.execution.contracts.dataflow_codec import runtime_scope_id
    physical = load_sealed_plan(run.directory, action_id, version='v3')['physical']
    scopes = {runtime_scope_id(physical.plan_id, attempt)
              for attempt in range(1, run.attempt + 1)}
    return {key: (path, value) for key, (path, value) in process_records(run.directory).items()
            if path.parent.parent.name in scopes}


def assert_released(directory):
    """Observe the real allocator and birth/lock evidence; do no cleanup here."""
    from demiflow.execution.run_journal import RunJournal
    from demiflow.execution.hosts.capacity import local_capacity
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.resources.process_unit import ManagedProcessUnit
    records = process_records(directory)
    assert records, 'no actual process allocation evidence'
    for path, record in records.values():
        assert record['identity']['pid'] != os.getpid()
        unit = ManagedProcessUnit(path.parent.parent.parent)
        observed = unit.inspect(path.parent.parent.name, path.parent.name)
        assert observed['phase'] == 'TERMINATED' and observed['observation'] == 'FRESH', observed
    journal = RunJournal(directory)
    with journal._connection() as db:
        allocations = db.execute('SELECT allocation_id,cleanup FROM resource_allocations').fetchall()
        scopes = db.execute('SELECT state FROM resource_scopes').fetchall()
    assert allocations and all(cleanup == 'CONFIRMED' for _, cleanup in allocations)
    assert scopes and all(state == 'CLOSED' for state, in scopes)
    domain, capacity = local_capacity()
    assert str(domain).startswith('/tmp/demiflow-')
    snapshot = LocalCapacityPool(domain/'capacity', capacity=capacity).snapshot()
    assert not snapshot['active'] and snapshot['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}
    return snapshot['charged']
