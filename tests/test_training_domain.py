"""Fit-domain failure boundaries over the native run journal and project store."""
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import time

import pytest

from demiflow.execution.contracts.training import (
    FitBinding, FitRequest, FitResult, FixedDatasetRef, InputCopyReceipt, JobLookup, JobRef,
    ProjectObject, TrainingSnapshotRef,
)
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identity
from demiflow.execution.pipeline.run_context import RunAction, RunContext
from demiflow.execution.training.deployment import TrainingDeployment, bind_action_deployment
from demiflow.execution.journal.training_index import FitConflict, TrainingIndex
from demiflow.execution.training.runtime import FitRuntime, read_parent


def setup_request(tmp_path, *, paid=False, managed=False):
    project = tmp_path / 'project'
    project.mkdir(exist_ok=True)
    binding = FitBinding(str(project), str(project / 'models'), 'test-service/account',
                         'EngineManaged' if managed else 'ProviderManaged', 1 << 20, 100, 8,
                         max_resumes=1, max_charge=4 if paid else 0,
                         project_charge_limit=5 if paid else 0, currency='USD' if paid else None,
                         idempotent_submit=True, implementation_digest=None if managed else identity('provider trainer'))
    digest = identity('training input')
    data = FixedDatasetRef('lance/table', 1, digest, digest, digest, 12)
    semantics = dict(trainer_digest=digest, base_model_digest=digest, environment_digest=digest,
                     model_format='native.test', parameters={'seed': 123}, topology={'world_size': 1},
                     budget={'optimizer_updates': 3}, save_steps=[1,3])
    return FitRequest('project', {'train': data}, semantics), binding


def prepared_runtime(run, name, request, binding, provider):
    declaration = dict(kind='count')
    run.journal.bind_execution_action(run.fence, name, declaration)
    run.journal.bind_source(run.fence, name+'-dataflow-sealed', dict(
        schema='demiflow.dataflow_seal.v4', run_id=run.ref.run_id, action_id=name, plan_id=identity('plan'), dag_id=identity('dag'), plan_revision=0, fit_occurrences=[identity('fit1')]))
    action = RunAction(run, name, declaration)
    bind_action_deployment(action, request.project_id, binding)
    return FitRuntime(action, identity('fit1'), request, binding, provider)


class Provider:
    # Test transport exercising lost replies, original-scope lookups and result download.
    def __init__(self):
        self.jobs = {}
        self.sends = []
        self.lookups = []
        self.lose_reply = False
        self.started = True
        self.answer = 'found'
        self.status = 'succeeded'
        self.stopped = 0

    def submit(self, request, binding, key, *, deadline=None):
        self.sends.append(key)
        if self.started:
            self.jobs.setdefault(key, (request, binding))
        if self.lose_reply:
            raise TimeoutError()
        return JobRef(binding.provider_scope, key)

    def lookup(self, key, *, deadline=None):
        self.lookups.append(key)
        if self.answer == 'unknown':
            return JobLookup('unknown')
        proof = dict(scope='test-service/account', request_key=key, authoritative=True,
                     idempotency_expires_at=time.time() + (60 if self.answer != 'expired' else -1))
        if self.answer == 'wrong':
            proof['request_key'] = 'another'
        if key in self.jobs:
            return JobLookup('found', JobRef(proof['scope'], key), proof)
        return JobLookup('confirmed_absent', evidence=proof)

    def inspect(self, job, *, deadline=None):
        return self.status

    def cancel(self, job, *, deadline=None):
        self.stopped += 1

    def supports_charge_cap(self, binding, *, deadline=None):
        return True

    def actual_charge(self, job, *, deadline=None):
        return 1

    def completed_snapshots(self, job):
        return (self._snapshot(job, 1),)

    def _snapshot(self, job, step):
        request, binding = self.jobs[job.job_id]
        root = Path(binding.model_store_uri) / 'staging' / request.instance_id / job.job_id
        root.mkdir(parents=True, exist_ok=True)
        root = root / ('step-' + str(step))
        root.mkdir(exist_ok=True)
        def put(name):
            data = (name + str(step)).encode()
            path = root / name
            path.write_bytes(data)
            return ProjectObject(str(path),hashlib.sha256(data).hexdigest(),len(data))
        return TrainingSnapshotRef(step, 'native.test', {'weights': put('weights')}, {'trainer': put('trainer')})

    def result(self, job, *, deadline=None):
        request, _ = self.jobs[job.job_id]
        steps = [3] if request.restart_snapshot is not None else [1, 3]
        return FitResult(request.instance_id, request.fit_key, tuple(self._snapshot(job, i) for i in steps), 3)


def finish(runtime):
    for _ in range(1000):
        result = runtime.advance()
        if result:
            return result
        time.sleep(.002)
    raise AssertionError('test provider never completed')


def test_original_receipt_cross_action_and_parent(tmp_path):
    request, binding = setup_request(tmp_path)
    provider = Provider()
    with RunContext.start(tmp_path/'producer') as run:
        first = prepared_runtime(run, 'write', request, binding, provider)
        result = finish(first)
        selected = first.selected_model(result, 1, identity('predictor'))
        assert 'resume_files' not in selected['snapshot']
        first.hold_active()
        from demiflow.execution.commit import CommitCoordinator
        from demiflow.execution.outputs.lance_sink import LanceSink
        from demiflow.execution.contracts.recovery_contract import SourcePosition
        from demiflow.lance.model import LanceWriteSpec
        from demiflow.execution.training.runtime import confirm_table_consumption
        from demiflow.execution.journal.run_reader import read_run
        import pyarrow as pa
        uri = str(tmp_path / 'scores.lance')
        provenance = dict(uri=uri, selection=[{'step':1},{'step':3}])
        first.retain_table('write', provenance)
        sink = LanceSink(first.action, LanceWriteSpec(uri=uri, mode='create'))
        fact = CommitCoordinator(first.action).commit(1, outputs={'lance':(sink,[pa.table({'score':[1]})])},
                                                      position=SourcePosition('holdout',1))
        # Recovery with no live FitRuntime or training provider.
        confirm_table_consumption(first.action, first.occurrence, binding, fact['receipts']['lance'])
        first.retain_table('write', provenance)
        with read_run(run.directory) as view:
            retained = view.training_outputs(run.attempt, 'write')
            assert retained['models'][0]['instance'] == request.instance_id
            assert retained['models'][0]['version'] == 1
            trace = view.training_provenance(run.attempt, 'write', first.occurrence)
            assert trace['table']['selected_steps'] == [1,3]
            assert trace['provenance']['training_inputs']['train']['version'] == 1
            assert len(trace['provenance']['selection'][0]['weight_digest']) == 64
        first.release_active()
        assert read_parent(request.project_id, binding, selected['manifest_digest'], 1).resume_files
        first.action.complete({})
    with RunContext.start(tmp_path/'consumer') as run:
        follower = prepared_runtime(run, 'count', request, binding, provider)
        assert finish(follower) == result and len(provider.sends) == 1
        follower.action.complete({})


def test_missing_receipt_not_a_cache_hit(tmp_path, monkeypatch):
    from demiflow.execution import commit
    request,binding = setup_request(tmp_path)
    provider = Provider()
    failed = []
    def crash(point, commit_id):
        if point == 'after_sink_commit_before_receipt' and not failed:
            failed.append(commit_id)
            raise RuntimeError('interrupted')
    monkeypatch.setattr(commit, 'fault_point', crash)
    ref = None
    with pytest.raises(RuntimeError, match='interrupted'):
        with RunContext.start(tmp_path/'producer') as run:
            ref = run.ref
            first = prepared_runtime(run, 'write', request,binding,provider)
            finish(first)
    with RunContext.start(tmp_path/'consumer') as run:
        follower = prepared_runtime(run,'count',request,binding,provider)
        assert follower.advance() is None and len(provider.sends) == 1
        follower.cancel()
        follower.action.complete({})
    with RunContext.resume(ref) as run:
        recovered = prepared_runtime(run,'write',request,binding,provider)
        result = finish(recovered)
        assert result.final_step == 3 and len(provider.sends) == 1
        assert run.journal.execution_commits('write') == []
        recovered.action.complete({})


def test_lost_ack_scoped_lookup_and_budget(tmp_path):
    req,binding = setup_request(tmp_path,paid=True)
    provider = Provider()
    provider.lose_reply = True
    with RunContext.start(tmp_path/'run') as run:
        original = prepared_runtime(run,'fit',req,binding,provider)
        assert 'charge_limit/USD' not in _project_settings(binding)
        assert original.advance() is None
        assert _project_settings(binding)['charge_limit/USD'] == binding.project_charge_limit
        provider.answer = 'unknown'
        assert original.advance() is None
        provider.answer = 'wrong'
        with pytest.raises(FitConflict,match='different'):
            original.advance()
        competing = prepared_runtime(run,'compete',replace(req,retrain_id='second'),binding,provider)
        with pytest.raises(FitConflict,match='budget'):
            competing.advance()
        provider.answer = 'found'
        assert finish(original) and len(provider.jobs) == len(provider.sends) == 1
        provider.lose_reply = False
        assert finish(competing)
        original.action.complete({})
        competing.action.complete({})


def test_redelivery_requires_live_authoritative_absence(tmp_path):
    req,binding = setup_request(tmp_path)
    provider = Provider()
    provider.lose_reply = True
    provider.started = False
    with RunContext.start(tmp_path/'run') as run:
        runtime = prepared_runtime(run,'fit',req,binding,provider)
        assert runtime.advance() is None
        provider.answer = 'expired'
        assert runtime.advance() is None and len(provider.sends) == 1
        provider.answer = 'confirmed_absent'
        provider.lose_reply = False
        provider.started = True
        assert finish(runtime) and len(provider.sends) == 2 and len(set(provider.sends)) == 1
        runtime.action.complete({})


def test_resumed_launch_merges_verified_prior_steps(tmp_path):
    req,binding = setup_request(tmp_path, managed=True)
    provider = Provider()
    with RunContext.start(tmp_path/'run') as run:
        runtime = prepared_runtime(run,'fit',req,binding,provider)
        assert runtime.advance() is None
        provider.status = 'interrupted'
        assert runtime.advance() is None
        provider.status = 'succeeded'
        result = finish(runtime)
        assert [s.step for s in result.snapshots] == [1,3]
        assert len(provider.sends) == len(set(provider.sends)) == 2
        assert list(provider.jobs.values())[-1][0].restart_snapshot.step == 1
        runtime.action.complete({})


def test_corruption_and_wrong_occurrence_fail_closed(tmp_path):
    req,binding = setup_request(tmp_path)
    provider = Provider()
    with RunContext.start(tmp_path/'run') as run:
        first = prepared_runtime(run,'fit',req,binding,provider)
        result = finish(first)
        other = replace(req, retrain_id='new')
        with pytest.raises(RecoveryContractError,match='sealed'):
            FitRuntime(first.action,'not-a-job', other,binding,provider)
        assert first.index.read(other.instance_id) is None
        path = Path(result.snapshots[0].files['weights'].uri)
        path.chmod(0o600)
        path.write_bytes(b'corrupt')
        follower = prepared_runtime(run,'count',req,binding,provider)
        with pytest.raises(RecoveryContractError,match='differs'):
            follower.advance()
        assert len(provider.sends) == 1
        first.action.complete({})
        follower.action.complete({})


def _claim_one(args):
    req, binding, n = args
    index = TrainingIndex(FitBinding.from_dict(binding))
    return index.claim_or_get(FitRequest.from_dict(req), run_id=str(n), run_dir='/test/'+str(n),
                              action='action',occurrence='fit',attempt=1)['run_id']


def test_project_elects_one_owner_across_processes(tmp_path):
    req,binding = setup_request(tmp_path)
    TrainingIndex(binding, quotas='apply')
    with ProcessPoolExecutor(3) as pool:
        answers = list(pool.map(_claim_one, [(req.to_dict(),binding.to_dict(),i) for i in range(10)]))
    assert len(set(answers)) == 1


def test_provider_input_copy_recovers_lost_upload_ack(tmp_path):
    req,binding = setup_request(tmp_path)
    binding = replace(binding, input_destinations={'train':'provider://inputs/a'})
    provider = Provider()
    uploaded = []
    def stage(source, destination, key, *, deadline):
        uploaded.append(InputCopyReceipt(binding.provider_scope, key, destination, replace(
            source, uri='provider://copy/fixed', version=7)))
        raise TimeoutError()
    provider.stage_input = stage
    provider.lookup_input = lambda key, *, deadline: None
    with RunContext.start(tmp_path/'run') as run:
        runtime = prepared_runtime(run,'fit',req,binding,provider)
        assert runtime.advance() is None
        assert runtime.advance() is None and not provider.sends and len(uploaded) == 1
        provider.lookup_input = lambda key, *, deadline: uploaded[0]
        assert finish(runtime) and len(uploaded) == 1
        actual, _ = next(iter(provider.jobs.values()))
        assert actual.inputs['train'].uri == 'provider://copy/fixed'
        assert actual.fit_key == req.fit_key
        runtime.action.complete({})


def test_paid_provider_without_hard_cap_cannot_send(tmp_path):
    req,binding = setup_request(tmp_path,paid=True)
    provider = Provider()
    provider.supports_charge_cap = lambda binding, *, deadline: False
    with RunContext.start(tmp_path/'run') as run:
        runtime = prepared_runtime(run,'fit',req,binding,provider)
        with pytest.raises(FitConflict, match='hard per-job charge cap'):
            runtime.advance()
        assert not provider.sends
        runtime.action.complete({})


def test_cancel_requires_original_provider_terminal(tmp_path):
    request,binding = setup_request(tmp_path, paid=True)
    provider = Provider()
    provider.lose_reply = True
    provider.status = 'running'
    with RunContext.start(tmp_path/'run') as run:
        runtime = prepared_runtime(run, 'train', request,binding,provider)
        runtime.advance()
        provider.answer = 'unknown'
        assert not runtime.cancel()
        assert runtime.index.read(request.instance_id)['state'] == 'cancelling'
        provider.answer = 'found'
        assert not runtime.cancel() and not runtime.cancel()
        assert provider.stopped == 1 and len(provider.sends) == 1
        provider.status = 'cancelled'
        assert runtime.cancel() and runtime.index.read(request.instance_id)['state'] == 'cancelled'
        runtime.action.complete({})


def test_fit_observation_is_attempt_scoped_and_read_only(tmp_path):
    from demiflow.execution.journal.run_reader import read_run
    request,binding = setup_request(tmp_path)
    provider = Provider()
    with pytest.raises(RuntimeError):
        with RunContext.start(tmp_path/'run') as run:
            ref = run.ref
            runtime = prepared_runtime(run,'train',request,binding,provider)
            runtime.advance()
            raise RuntimeError('crashed after submission')
    with RunContext.resume(ref) as run:
        second = prepared_runtime(run,'train',request,binding,provider)
        finish(second)
        second.action.complete({})
    journal_path = tmp_path/'run'/'run.sqlite'
    before=journal_path.read_bytes()
    with read_run(journal_path.parent) as reader:
        rows = [next(reader.observation_rows(n, action_id='train', limit=1)) for n in (1,2)]
        assert [r['state'] for r in rows] == ['running','succeeded']
        assert 'producer_commit' in {e['resource'] for e in rows[1]['evidence']}
    assert journal_path.read_bytes() == before and len(provider.sends) == 1


def test_stopped_control_reconciles_model_without_replaying_rows(tmp_path):
    from demiflow.execution.journal.run_journal import RunJournal
    from demiflow.execution.training.deployment import TrainingDeployment
    from demiflow.execution.pipeline.training_maintenance import reconcile_stopped_fits
    request,binding = setup_request(tmp_path)
    provider = Provider()
    provider.lose_reply = True
    directory = tmp_path/'run'
    with pytest.raises(RuntimeError):
        with RunContext.start(directory) as run:
            runtime = prepared_runtime(run, 'append', request, binding, provider)
            for _ in range(20):
                runtime.advance()
                if len(provider.jobs) == 1:
                    break
                time.sleep(.01)
            raise RuntimeError('driver stopped before first row cut')
    journal = RunJournal(directory, attempt=1)
    state = journal.execution_status()
    outcome = reconcile_stopped_fits(journal, expected_attempt=1, expected_revision=state['revision'],
        deadline=time.monotonic()+5, confirm_stopped=lambda _:True, project_root=tmp_path,
        deployment=TrainingDeployment(request.project_id,binding,provider))
    assert outcome == {request.instance_id:'published'}
    assert journal.execution_commits('append') == [] and len(provider.sends) == 1
    assert journal.execution_status()['attempt'] == 1
    assert TrainingIndex(binding).read(request.instance_id)['state'] == 'published'


def test_unresponsive_submit_does_not_block_driver_or_trigger_second_submission(tmp_path):
    from threading import Event
    request,binding = setup_request(tmp_path)
    binding = replace(binding, provider_timeout_s=.04)
    provider = Provider()
    original = provider.submit
    response = Event()
    accepted = Event()
    def withheld(request,binding,key,*,deadline):
        job = original(request,binding,key,deadline=deadline)
        accepted.set()
        response.wait(3)
        return job
    provider.submit = withheld
    try:
        with RunContext.start(tmp_path/'run') as run:
            runtime = prepared_runtime(run,'train',request,binding,provider)
            start = time.monotonic()
            assert runtime.advance() is None
            assert time.monotonic()-start < .3 and accepted.wait(.3)
            time.sleep(.06)
            assert runtime.advance() is None
            follower = prepared_runtime(run,'other',request,binding,provider)
            assert follower.advance() is None and follower.cancel() is True
            assert not runtime.cancel()
            assert len(provider.sends) == 1
            response.set()
            provider.status = 'cancelled'
            for _ in range(100):
                if runtime.cancel():
                    break
                time.sleep(.003)
            assert runtime.index.read(request.instance_id)['state'] == 'cancelled'
            assert len(provider.sends) == 1
            runtime.action.complete({})
            follower.action.complete({})
    finally:
        response.set()


def _interrupted_import(binding, ready, stop):
    import io
    from demiflow.execution.training.store import ModelStore
    payload = b'bbbb'
    class AtReservation(io.BytesIO):
        def read(self, size):
            ready.set()
            stop.wait(10)
            os._exit(77)
    store = ModelStore(TrainingIndex(FitBinding.from_dict(binding)))
    store._put(AtReservation(payload),hashlib.sha256(payload).hexdigest(),len(payload))


def test_dead_import_returns_quota_but_a_live_import_and_published_inode_are_retained(tmp_path):
    import multiprocessing as mp
    import io
    from demiflow.execution.training.store import ModelStore
    _, binding = setup_request(tmp_path)
    binding = replace(binding,max_model_bytes=4,max_files=1)
    store = ModelStore(TrainingIndex(binding, quotas='apply'))
    ctx = mp.get_context('spawn')
    ready, stop = ctx.Event(),ctx.Event()
    process = ctx.Process(target=_interrupted_import,args=(binding.to_dict(),ready,stop))
    process.start()
    data = b'aaaa'
    digest = hashlib.sha256(data).hexdigest()
    try:
        assert ready.wait(5)
        with pytest.raises(FitConflict,match='quota'):
            store._put(io.BytesIO(data),digest,4)
        stop.set()
        process.join(5)
        assert process.exitcode == 77
        kept = store._put(io.BytesIO(data),digest,4)
        assert Path(kept.uri).read_bytes() == data
        with pytest.raises(FitConflict,match='quota'):
            store._put(io.BytesIO(b'cccc'),hashlib.sha256(b'cccc').hexdigest(),4)
        store.verify(kept)
    finally:
        stop.set()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join(2)


def test_cancelling_never_inspects_unwithdrawn_launch(tmp_path):
    req,binding = setup_request(tmp_path, managed=True)
    class ActivatingProvider(Provider):
        def inspect(self, job):
            if not self.stopped:
                raise AssertionError('would have activated a ready trainer after cancellation')
            return 'cancelled'
    provider = ActivatingProvider()
    with RunContext.start(tmp_path/'run') as run:
        fit = prepared_runtime(run,'fit',req,binding,provider)
        assert fit.advance() is None
        assert fit.cancel()
        row = fit.index.read(req.instance_id)
        assert provider.stopped == 1 and row['state'] == 'cancelled'
        with run.journal.fit_guard(run.fence,fit.action.action_id,fit.occurrence,admitting=True):
            assert not fit.index.launch_is_active(req.instance_id,row['request_key'],run_id=run.ref.run_id,
                action=fit.action.action_id,occurrence=fit.occurrence,attempt=run.attempt)
        fit.action.complete({})


def test_stopped_run_prevents_new_dispatch_but_can_observe_existing_submit(tmp_path):
    from demiflow.errors import ExecutionConflict
    req,binding = setup_request(tmp_path)
    provider = Provider()
    with pytest.raises(RuntimeError, match='explicit stop'):
        with RunContext.start(tmp_path/'run') as run:
            fit = prepared_runtime(run,'fit',req,binding,provider)
            row = fit._claim()
            fit._control(row,'reserve')
            run.journal.transition({'running'},'stopping')
            with pytest.raises(ExecutionConflict, match='cannot admit'):
                fit._control(row,'send')
            assert fit._control(row)['state'] == 'reserved'
            assert fit.cancel() and not provider.sends
            raise RuntimeError('explicit stop')


def _concurrent_dataset_driver(root, run_name):
    from demiflow import data
    from demiflow.execution.training.deployment import TrainingDeployment
    from test_training_dataset_action import recipe
    _, binding = setup_request(root)
    class SharedTransport(Provider):
        def submit(self, request, binding, key, *, deadline):
            with open(root/'service-submissions','ab',buffering=0) as stream:
                stream.write((key+'\n').encode())
                os.fsync(stream.fileno())
            return super().submit(request,binding,key,deadline=deadline)
        def inspect(self, job, *, deadline):
            return 'succeeded' if (root/'release-service').exists() else 'running'
    with RunContext.start(root/run_name, training=TrainingDeployment('project',binding,SharedTransport())):
        prediction = data.read_lance(str(root/'inputs.lance'),version=1).fit(
            recipe=recipe()).checkpoints.select(steps=[1,3]).predict(on=data.from_items([{'x':1}]))
        assert prediction.count() == 2


def test_two_live_dataset_drivers_submit_one_actual_job(tmp_path):
    from contextlib import ExitStack
    import subprocess
    import sys
    import lance
    import pyarrow as pa
    from demiflow.execution.journal.run_reader import read_run, RunReadError
    lance.write_dataset(pa.table({'x':[1,2]}),str(tmp_path/'inputs.lance'))
    names = ('first','second')
    command = [sys.executable,'-B','-c','from pathlib import Path; import sys; '
               'from test_training_domain import _concurrent_dataset_driver; '
               '_concurrent_dataset_driver(Path(sys.argv[1]),sys.argv[2])',str(tmp_path)]
    with ExitStack() as stack:
        children = [stack.enter_context(subprocess.Popen(command+[name], env=dict(os.environ,
            PYTHONPATH=os.pathsep.join(p for p in sys.path if p), DEMIFLOW_RESOURCE_ROOT=str(tmp_path/(name+'-resources')))))
                    for name in names]
        try:
            deadline = time.monotonic()+65
            while True:
                assert all(p.poll() is None for p in children) and time.monotonic()<deadline
                try:
                    routes = []
                    for name in names:
                        with read_run(tmp_path/name) as reader:
                            routes.extend(reader.fit_routes())
                except RunReadError:
                    routes = []
                if len(routes) == 2 and (tmp_path/'service-submissions').exists():
                    break
                time.sleep(.02)
            assert routes[0]['instance'] == routes[1]['instance']
            (tmp_path/'release-service').touch()
            assert all(p.wait(80) == 0 for p in children)
        finally:
            (tmp_path/'release-service').touch()
            for p in children:
                if p.poll() is None:
                    p.kill()
    keys = (tmp_path/'service-submissions').read_text().splitlines()
    assert len(keys) == 1
    _,binding = setup_request(tmp_path)
    row = TrainingIndex(binding).read(routes[0]['instance'])
    assert row['state'] == 'published' and row['request_key'] == keys[0]
    states = []
    for name in names:
        with read_run(tmp_path/name) as reader:
            matches = list(reader.observation_rows(1,action_id=row['action'],node_id=row['occurrence'],limit=1))
            assert len(matches)==1
            states.append(matches[0]['state'])
    assert set(states) == {'succeeded','reused'}


def test_stopped_candidate_before_election_does_not_acquire_or_dispatch(tmp_path,monkeypatch):
    from demiflow.execution.journal.run_journal import RunJournal
    from demiflow.execution.training.deployment import TrainingDeployment
    from demiflow.execution.pipeline.training_maintenance import reconcile_stopped_fits
    from demiflow.execution.journal.run_reader import read_run
    request,binding=setup_request(tmp_path)
    provider=Provider()
    directory=tmp_path/'candidate'
    with pytest.raises(RuntimeError, match='before election'):
        with RunContext.start(directory) as run:
            fit=prepared_runtime(run,'fit',request,binding,provider)
            def crash(*args,**kwargs):
                raise RuntimeError('before election')
            monkeypatch.setattr(fit.index,'claim_or_get',crash)
            fit.advance()
    journal=RunJournal(directory,attempt=1)
    with read_run(directory) as reader:
        assert reader.fit_routes()[0]['instance']==request.instance_id
    result=reconcile_stopped_fits(journal,expected_attempt=1,expected_revision=journal.execution_status()['revision'],
        deadline=time.monotonic()+5,confirm_stopped=lambda _:True,project_root=tmp_path,
        deployment=TrainingDeployment('project',binding,provider))
    assert result == {} and not provider.sends and TrainingIndex(binding).read(request.instance_id) is None


def test_model_import_contention_honors_consumers_deadline(tmp_path):
    import fcntl
    import io
    from demiflow.execution.training.store import ModelStore
    _, binding = setup_request(tmp_path)
    start = time.monotonic()

    def check_stop():
        if time.monotonic() - start > .1:
            raise TimeoutError('model consumer deadline')

    store = ModelStore(TrainingIndex(binding, quotas='apply'), check_stop=check_stop)
    content = b'original'
    digest = hashlib.sha256(content).hexdigest()
    directory = store.objects / digest[:2]
    directory.mkdir()
    fd = os.open(directory, os.O_DIRECTORY | os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        with pytest.raises(TimeoutError, match='consumer deadline'):
            store._put(io.BytesIO(content), digest, len(content))
        assert time.monotonic() - start < 2
        assert not list(directory.iterdir())
    finally:
        os.close(fd)


def _project_settings(binding):
    import json
    import sqlite3
    from contextlib import closing
    with closing(sqlite3.connect(Path(binding.project_dir) / 'fit.sqlite')) as db:
        return {key: json.loads(value) for key, value in db.execute('SELECT name,value FROM fit_settings')}


def test_legacy_quota_settings_upgrade_atomically(tmp_path):
    import json
    import socket
    import sqlite3
    from contextlib import closing
    request, binding = setup_request(tmp_path, paid=True)
    binding = replace(binding, max_model_bytes=100, max_files=10, max_instances=8, max_references=12)
    machine_id = Path('/etc/machine-id')
    old = dict(control_host=machine_id.read_text().strip() if machine_id.is_file() else socket.gethostname(),
               model_store=binding.model_store_uri, model_bytes=100, files=10, instances=8, references=12)
    old['charge_limit/USD'] = binding.project_charge_limit
    # Existing v1 settings use JSON values stored as TEXT in this exact table.
    with closing(sqlite3.connect(Path(binding.project_dir) / 'fit.sqlite')) as db, db:
        db.execute('CREATE TABLE fit_settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)')
        db.executemany('INSERT INTO fit_settings VALUES (?,?)', [(k,json.dumps(v)) for k,v in old.items()])
    larger = replace(binding, max_model_bytes=110, max_files=11, max_instances=9, max_references=13)
    for field in ('max_model_bytes','max_files','max_instances','max_references'):
        mixed = replace(larger, **{field: getattr(binding,field)-1})
        with pytest.raises(FitConflict, match='decrease'):
            TrainingDeployment(request.project_id,mixed,Provider()).apply_project_quotas()
        assert _project_settings(binding) == old
    for changed in (replace(larger, project_charge_limit=6),
                    replace(larger, model_store_uri=str(tmp_path/'wrong'))):
        with pytest.raises(FitConflict, match='project setting'):
            TrainingDeployment(request.project_id,changed,Provider()).apply_project_quotas()
        assert _project_settings(binding) == old
    for readonly in (True, False):
        with pytest.raises(FitConflict,match='exceeds approved'):
            TrainingIndex(larger,readonly=readonly)
    assert _project_settings(binding) == old
    TrainingDeployment(request.project_id,larger,Provider()).apply_project_quotas()
    assert _project_settings(binding) == {**old,'model_bytes':110,'files':11,'instances':9,'references':13}


def test_sealed_consumers_observe_raised_byte_and_file_limits(tmp_path):
    import io
    from demiflow.execution.training.store import ModelStore
    request, binding = setup_request(tmp_path)
    binding = replace(binding, max_model_bytes=4, max_files=1)
    index = TrainingIndex(binding, quotas='apply')
    writer = ModelStore(index)
    reader = ModelStore(TrainingIndex(binding,readonly=True))
    def put(raw):
        return writer._put(io.BytesIO(raw),hashlib.sha256(raw).hexdigest(),len(raw))
    before = put(b'abcd')
    with pytest.raises(FitConflict, match='quota'):
        put(b'z')
    expanded = replace(binding, max_model_bytes=10, max_files=2)
    TrainingDeployment(request.project_id,expanded,Provider()).apply_project_quotas()
    after = put(b'123456')
    reader.verify(before)
    reader.verify(after)
    assert Path(before.uri).read_bytes() == b'abcd'
    assert Path(after.uri).read_bytes() == b'123456'
    with pytest.raises(FitConflict,match='quota'):
        put(b'!')


def _offer_project_quotas(binding, ready, start, result):
    binding = FitBinding.from_dict(binding)
    ready.put(True)
    assert start.wait(20)
    try:
        TrainingDeployment('project',binding,Provider()).apply_project_quotas()
    except FitConflict as exc:
        result.put(str(exc))
    else:
        result.put((binding.max_instances,binding.max_references))


def test_concurrent_incomparable_quota_proposals_never_combine(tmp_path):
    import multiprocessing as mp
    _, binding = setup_request(tmp_path)
    binding = replace(binding,max_instances=1,max_references=1)
    TrainingIndex(binding,quotas='apply')
    ctx = mp.get_context('spawn')
    ready, start, done = ctx.Queue(), ctx.Event(), ctx.Queue()
    offers = ((2,3),(3,2))
    workers = [ctx.Process(target=_offer_project_quotas,args=(
        replace(binding,max_instances=i,max_references=r).to_dict(),ready,start,done)) for i,r in offers]
    for worker in workers:
        worker.start()
    try:
        for worker in workers:
            assert ready.get(timeout=20)
        start.set()
        results = [done.get(timeout=20) for worker in workers]
        accepted, = [item for item in results if type(item) is tuple]
        refused, = [item for item in results if type(item) is str]
        assert accepted in offers and 'decrease' in refused
        settings = _project_settings(binding)
        assert (settings['instances'],settings['references']) == accepted
    finally:
        start.set()
        for worker in workers:
            worker.join(5)
            if worker.is_alive():
                worker.kill()
                worker.join(5)
            assert worker.exitcode == 0


def test_quota_raise_preserves_published_training_and_scoring_table_hold(tmp_path):
    request, binding = setup_request(tmp_path)
    binding = replace(binding,max_instances=1,max_references=1)
    enlarged = replace(binding,max_instances=2,max_references=2)
    provider = Provider()
    with RunContext.start(tmp_path/'first') as run:
        fit = prepared_runtime(run,'fit',request,binding,provider)
        result = finish(fit)
        row = fit.index.read(request.instance_id)
        import pyarrow as pa
        from demiflow.execution.commit import CommitCoordinator
        from demiflow.execution.outputs.lance_sink import LanceSink
        from demiflow.execution.contracts.recovery_contract import SourcePosition
        from demiflow.execution.training.runtime import confirm_table_consumption
        from demiflow.lance.model import LanceWriteSpec
        uri = str(tmp_path/'scores.lance')
        provenance = {'uri':uri, 'selection':[{'step':1}]}
        fit.retain_table('fit',provenance)
        sink = LanceSink(fit.action,LanceWriteSpec(uri=uri,mode='create'))
        fact = CommitCoordinator(fit.action).commit(1,outputs={'lance':(sink,[pa.table({'score':[3]})])},
                                                   position=SourcePosition('holdout',1))
        receipt = fact['receipts']['lance']
        confirm_table_consumption(fit.action,fit.occurrence,binding,receipt)
        def other_instance():
            return fit.index.claim_or_get(replace(request,project_id='second-project'),run_id=run.ref.run_id,
                run_dir=str(run.directory),action='next',occurrence=fit.occurrence,attempt=run.attempt)
        with pytest.raises(FitConflict,match='instance quota'):
            other_instance()
        with pytest.raises(FitConflict,match='reference quota'):
            fit.hold_active()
        TrainingDeployment(request.project_id,enlarged,provider).apply_project_quotas()
        fit.hold_active()
        other_instance()
        with pytest.raises(FitConflict,match='reference quota'):
            fit.index.hold('third',request.instance_id,kind='active')
        confirm_table_consumption(fit.action,fit.occurrence,binding,receipt)
        assert fit.index.read(request.instance_id) == row
        assert read_parent(request.project_id,binding,row['manifest_digest'],1) == result.snapshots[0]
        fit.action.complete({})


def _quota_lance_action(root, resume):
    from demiflow import data
    from demiflow.execution.contracts.recovery_contract import RunRef
    from demiflow.execution.journal.run_journal import RunJournal
    from test_training_dataset_action import recipe
    from training_action_programs import waiting_trainer
    _, binding = setup_request(root,managed=True)
    original = replace(binding,max_files=1,provider_options={'cpu_millis':100,'memory_bytes':16<<20})
    deployment = TrainingDeployment('project',original)
    directory = root/'run'
    if resume:
        ref = RunRef(str(directory),RunJournal(directory).recovery_identity()['run_id'])
        context = RunContext.resume(ref,training=deployment)
    else:
        context = RunContext.start(directory,training=deployment)
    with context:
        model = data.read_lance(str(root/'input.lance'),version=1).fit(recipe=replace(recipe(),trainer=waiting_trainer))
        model.checkpoints.select(steps=[1,3]).predict(on=data.from_items([{'x':7}])).write_lance(
            str(root/'scored.lance'),mode='create')


def test_original_submitted_action_recovers_using_raised_storage_quota(tmp_path):
    import subprocess
    import sys
    import lance
    import pyarrow as pa
    from demiflow.execution.journal.run_journal import RunJournal
    lance.write_dataset(pa.table({'x':[1]}),str(tmp_path/'input.lance'))
    _, binding = setup_request(tmp_path,managed=True)
    original = replace(binding,max_files=1,provider_options={'cpu_millis':100,'memory_bytes':16<<20})
    raised = replace(original,max_files=3)
    env = dict(os.environ,PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
    command = [sys.executable,'-B','-c',
        'import sys; from pathlib import Path; from test_training_domain import _quota_lance_action; '
        '_quota_lance_action(Path(sys.argv[1]),sys.argv[2]=="resume")',str(tmp_path)]
    started, proceed = tmp_path/'project'/'started', tmp_path/'project'/'proceed'
    with subprocess.Popen(command+['start'],env=env) as driver:
        try:
            until = time.monotonic()+80
            while not started.exists():
                assert driver.poll() is None and time.monotonic()<until
                time.sleep(.03)
            driver.kill()
            driver.wait(timeout=10)
            journal = RunJournal(tmp_path/'run')
            assert journal.latest_execution_commit('action-1') is None
            assert _project_settings(original)['files'] == 1
            pin = journal.source_binding(identity(['training-deployment','action-1']))
            assert pin == {'project':'project','binding_digest':identity(original.to_dict())}
            TrainingDeployment('project',raised).apply_project_quotas()
            proceed.touch()
            subprocess.run(command+['resume'],env=env,check=True,timeout=100)
            assert journal.execution_status()['attempt'] == 2
            assert journal.source_binding(identity(['training-deployment','action-1'])) == pin
            original_launch, = (tmp_path/'run'/'training').glob('*/launch.json')
            assert original_launch.parent.name == started.read_text()
            artifacts = list((Path(binding.model_store_uri)/'objects').glob('*/*'))
            assert len(artifacts) == 3
            assert sorted(lance.dataset(str(tmp_path/'scored.lance')).to_table().to_pylist(),
                          key=lambda row:row['step']) == [{'x':7,'step':1},{'x':7,'step':3}]
            assert _project_settings(original)['files'] == 3
        finally:
            proceed.touch()
            if driver.poll() is None:
                driver.kill()


def test_prepared_action_without_original_deployment_must_match_current_quotas(tmp_path):
    request, original = setup_request(tmp_path)
    current = replace(original,max_instances=original.max_instances+1)
    provider = Provider()
    original_deploy = TrainingDeployment('project',original,provider)
    current_deploy = TrainingDeployment('project',current,provider)
    current_deploy.apply_project_quotas()
    directory = tmp_path/'run'
    with pytest.raises(RuntimeError,match='before fit admission'):
        with RunContext.start(directory) as run:
            ref = run.ref
            run.journal.bind_execution_action(run.fence,'action-1',{'kind':'count'})
            raise RuntimeError('before fit admission')
    with RunContext.resume(ref) as run:
        action = RunAction(run,'action-1',{'kind':'count'})
        key = identity(['training-deployment','action-1'])
        for candidate in (original_deploy, TrainingDeployment('project',replace(current,max_files=999),provider)):
            with pytest.raises(FitConflict,match='first fit admission|First fit admission'):
                bind_action_deployment(action,candidate.project_id,candidate.binding)
            assert run.journal.source_binding(key) is None
        bind_action_deployment(action,current_deploy.project_id,current_deploy.binding)
        with pytest.raises(RecoveryContractError,match='binding differs'):
            bind_action_deployment(action,'project',replace(current,max_files=999))
        assert _project_settings(original)['files']==current.max_files
        action.complete({})


def _score_after_reference_quota_failure(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow import data
    from test_training_dataset_action import recipe
    _, original = setup_request(tmp_path)
    original = replace(original,max_references=1)
    provider = Provider()
    source = str(tmp_path/'training.lance')
    scores = str(tmp_path/'scores.lance')
    lance.write_dataset(pa.table({'x':[1]}),source)
    class DeploymentWithHooks(TrainingDeployment):
        def bind_action(self, action):
            raise AssertionError('The driver must own deployment binding')

        def apply_project_quotas(self):
            raise AssertionError('The driver must own first fit admission')

    def score(directory, binding):
        with RunContext.start(directory,training=DeploymentWithHooks('project',binding,provider)):
            fitted = data.read_lance(source,version=1).fit(recipe=recipe())
            fitted.checkpoints.select(steps=[1]).predict(on=data.from_items([{'x':2}])).write_lance(
                scores,mode='create')
    with pytest.raises(FitConflict,match='reference quota'):
        score(tmp_path/'limited',original)
    (request, _), = provider.jobs.values()
    produced = TrainingIndex(original,readonly=True).read(request.instance_id)
    assert produced['state'] == 'published' and not Path(scores).exists()
    score(tmp_path/'expanded',replace(original,max_references=2))
    assert lance.dataset(scores).to_table().to_pylist() == [{'x':2,'step':1}]
    assert len(provider.sends) == 1
    assert TrainingIndex(original,readonly=True).read(request.instance_id) == produced


def test_new_scoring_action_reuses_published_model_after_reference_quota_failure(tmp_path):
    import subprocess
    import sys
    # A failed retention check must return to the caller before an unstarted publisher can hang cleanup.
    command = [sys.executable, '-B', '-c',
        'import sys; from pathlib import Path; from test_training_domain import _score_after_reference_quota_failure; '
        '_score_after_reference_quota_failure(Path(sys.argv[1]))', str(tmp_path)]
    subprocess.run(command, env=dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p)),
                   check=True, timeout=90)
