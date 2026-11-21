"""Driver-owned project fit execution. Each advance makes one job observation."""
from __future__ import annotations

from dataclasses import replace
import json
import time

from ..commit import CommitCoordinator
from ..contracts.training import FitBinding, FitRequest, FitResult, FitSubmissionRejected, InputCopyReceipt, JobLookup, JobRef, ProjectObject, TrainingSnapshotRef
from ..recovery_contract import RecoveryContractError, SourcePosition, identity
from ..run_context import RunAction
from ..run_reader import read_run
from .index import FitConflict, TrainingIndex
from .store import ModelStore, validate_result
from .provider_io import ProviderCalls, check_deadlines


def _consumer_id(action, occurrence, kind, key=None):
    return identity([action.context.ref.run_id, action.action_id, occurrence, kind, key])


def confirm_table_consumption(action, occurrence, binding, receipt):
    """Finish the retention transaction even if the driver died after final ACK."""
    from ..run_reader import fit_table_prefix
    run = action.context
    owner = _consumer_id(action, occurrence, 'table', action.action_id)
    intent = run.journal.source_binding(owner)
    if not intent or intent.get('producer') is None:
        raise FitConflict("No original pending training reference for this scoring table")
    if type(receipt) is not dict or receipt.get('status') != 'committed':
        raise FitConflict("Scoring table lacks a committed native receipt")
    data = receipt.get('data') or {}
    version = data.get('committed_version')
    if type(version) is not int or version < 1 or data.get('uri') != intent['provenance']['uri']:
        raise FitConflict("Scoring table version or URI differs from its original training reference")
    fact = run.journal.execution_commit(receipt['commit_id'])
    if not fact or fact['status'] != 'acked' or fact['action_id'] != action.action_id:
        raise FitConflict("No confirmed original action commit for scoring-table retention")
    # A native Lance receipt may be wrapped by the dataflow or object-row sink.
    found = False
    for outer in fact['receipts'].values():
        publication = outer['data']
        rows = publication.get('rows')
        if (outer == receipt or publication.get('native_receipt') == receipt
                or type(rows) is dict and rows.get('native_receipt') == receipt):
            found = True
            break
    if not found:
        raise FitConflict("Scoring table receipt differs from the original sink transaction")
    with run.journal.fit_guard(run.fence, action.action_id, occurrence):
        index = TrainingIndex(binding)
        index.hold(owner, intent['instance'],kind='table',
                   target=dict(provenance=intent['provenance'], receipt=receipt))
    summary = dict(action_id=action.action_id, attempt=run.attempt, occurrence=occurrence,
                   instance=intent['instance'], fit_key=intent['fit_key'], producer=intent['producer'],
                   uri=data['uri'], version=version, commit_id=receipt['commit_id'],
                   manifest_digest=intent['manifest'], base_model_digest=intent['base_model'],
                   provenance_id=owner, selection_digest=identity(intent['provenance']['selection']),
                   selection_count=len(intent['provenance']['selection']),
                   selected_steps=[s['step'] for s in intent['provenance']['selection'][:8]])
    run.journal.bind_source(run.fence, fit_table_prefix(run.attempt, action.action_id)+occurrence, summary)


def _scope(row):
    return identity(['model_manifest', row['action'], row['occurrence'], row['instance']])


def _receipt(row, manifest, commit_id):
    return dict(commit_id=commit_id, sink_id='model_manifest', status='committed',
                data=dict(instance=row['instance'], fit_key=row['fit_key'],
                          request_key=row['request_key'], manifest=manifest.to_dict()))


def _confirmed_manifest(row):
    ref = ProjectObject.from_dict(json.loads(row['manifest']))
    scope = _scope(row)
    expected = identity([row['run_id'], scope, 1])
    if expected != row['commit_id']:
        raise FitConflict("Project index has an inconsistent producer commit ID")
    with read_run(row['run_dir']) as reader:
        receipt = reader.fit_publication(row['run_id'], row['action'], scope, expected)
    if receipt is None:
        return None
    if receipt != _receipt(row, ref, expected):
        raise FitConflict("Published model differs from its producer receipt")
    return ref


def _selection(result, step, predictor_digest, manifest_digest):
    snapshot, = result.select((step,))
    data = snapshot.to_dict()
    del data['resume_files']
    if type(predictor_digest) is not str or len(predictor_digest) != 64 or any(
            c not in '0123456789abcdef' for c in predictor_digest):
        raise FitConflict("Invalid sealed predictor digest")
    return dict(instance_id=result.instance_id, fit_key=result.fit_key,
                manifest_digest=manifest_digest, predictor_digest=predictor_digest,
                step=step, snapshot=data)

def read_consumed_fit(action, occurrence, project_id, binding):
    """Verify a final row cut against the original model owner, without a live trainer."""
    source = action.context.journal.source_binding(_consumer_id(action,occurrence,'confirmed'))
    if not source or source.get('schema') != 'demiflow.fit_consumption.v1' or (
            source['action'],source['occurrence']) != (action.action_id,occurrence):
        raise FitConflict("No original model consumption proof for the final row checkpoint")
    index = TrainingIndex(binding,readonly=True)
    row = index.read(source['instance'])
    if (not row or row['project'] != project_id or row['state'] != 'published'
            or (row['fit_key'],row['run_id'],row['action'],row['commit_id']) != (
                source['fit_key'],source['producer_run'],source['producer_action'],source['producer_commit'])):
        raise FitConflict("Final row checkpoint references a different model producer")
    manifest = _confirmed_manifest(row)
    if manifest is None or manifest.to_dict() != source['manifest']:
        raise FitConflict("Final row checkpoint lacks confirmed model evidence")
    request = FitRequest.from_dict(json.loads(row['request']))
    result = ModelStore(index, check_stop=action.context.check_stop).load(manifest,request)
    return result, manifest


def consumed_model_digest(result, manifest, step, predictor_digest):
    return identity(_selection(result,step,predictor_digest,manifest.sha256))


def read_parent(project_id, binding, manifest_digest, step, *, consumer_instance=None, check_stop=None):
    """Resolve an explicit, confirmed parent within the same project authority."""
    index = TrainingIndex(binding, readonly=True)
    row = index.find_manifest(project_id, manifest_digest)
    if not row:
        raise FitConflict("No published parent manifest in this project")
    ref = _confirmed_manifest(row)
    if not ref or ref.sha256 != manifest_digest:
        raise FitConflict("Parent manifest lacks the original producer's confirmation")
    request = FitRequest.from_dict(json.loads(row['request']))
    result = ModelStore(index, check_stop=check_stop).load(ref, request)
    snapshot, = result.select((step,))
    if not snapshot.resume_files:
        raise FitConflict("Selected parent has no resumable optimizer state")
    if consumer_instance is not None:
        TrainingIndex(binding).hold(identity(['parent',consumer_instance,step]), result.instance_id, kind='parent',
                   target=dict(consumer=consumer_instance, step=step, manifest=manifest_digest))
    return snapshot


class _ModelManifestSink:
    def __init__(self, index, row):
        self.index = index
        self.row = row

    def prepare(self, blocks, context):
        manifest, = blocks
        return _receipt(self.row, manifest, context.commit_id)['data']

    def _validate(self, prepared, context):
        row = self.row
        manifest = ProjectObject.from_dict(prepared['manifest'])
        expected = _receipt(row, manifest, context.commit_id)
        if (prepared != expected['data'] or context.action_id != _scope(row)
                or context.run_id != row['run_id']):
            raise FitConflict("Prepared model manifest belongs to a different producer")
        return manifest, expected

    def inspect(self, prepared, context):
        manifest, receipt = self._validate(prepared, context)
        row = self.index.read(self.row['instance'])
        if row and row['state'] == 'published' and (row['commit_id'] == context.commit_id
                and json.loads(row['manifest']) == manifest.to_dict()):
            return receipt
        if row and row['state'] == 'claimed' and row['request_key'] == self.row['request_key']:
            return None
        raise FitConflict("Cannot establish the original model publication outcome")

    def commit(self, prepared, context):
        manifest, receipt = self._validate(prepared, context)
        row = self.row
        self.index.publish(row['instance'], row['epoch'], row['request_key'], context.commit_id, manifest)
        return receipt

    def abort(self, prepared, context):
        # Imported content belongs to the project and may have multiple users.
        pass


class FitRuntime:
    def __init__(self, action, occurrence, request: FitRequest, binding: FitBinding, provider):
        if request.restart_snapshot is not None:
            raise FitConflict("Only verified launch recovery can set restart_snapshot")
        self.action = action
        self.run = action.context
        self.occurrence = occurrence
        self.request = request
        self.binding = binding
        self.provider = provider
        self._active_step = None
        self._transport = None
        if binding.hosting == 'ProviderManaged':
            methods = ['submit','lookup','inspect','result','cancel']
            if binding.max_charge:
                methods.extend(('actual_charge','supports_charge_cap'))
            if binding.input_destinations:
                methods.extend(('stage_input','lookup_input'))
            check_deadlines(provider, methods)
            self._transport = ProviderCalls(provider,binding.provider_timeout_s)
        with self.run.journal.fit_guard(self.run.fence, self.action.action_id, occurrence):
            self.index = TrainingIndex(binding)
            self.store = ModelStore(self.index, check_stop=self.run.check_stop)
        self._completed = None
        self._copies = None
        self._capability_checked = False
        self._last_observation = None
        self._sequence = 0
        seal = self.run.journal.source_binding(self.action.action_id+'-dataflow-sealed')
        self._scope_fields = dict(plan_id=seal['plan_id'], dag_id=seal['dag_id'],
                                  plan_revision=seal['plan_revision'], occurrence_id=occurrence)
        self.run.journal.reserve_nodes([(self.action.action_id, occurrence, 0)])

    def _call(self, method, *args, before_start=None):
        if self._transport is None:
            if before_start is not None:
                before_start()
            return getattr(self.provider, method)(*args)
        deadline = time.monotonic() + max(0.0,self.run.deadline-time.time())
        return (yield from self._transport.call(method,args,deadline,before_start=before_start))

    def _observe(self, row, state, detail):
        refs = dict(instance=row['instance'], fit_key=row['fit_key'], producer_run=row['run_id'],
                    producer_action=row['action'], request_key=row['request_key'])
        if row['job']:
            refs['job_identity'] = identity(json.loads(row['job']))
        if row['manifest']:
            refs['manifest'] = json.loads(row['manifest'])['sha256']
            refs['producer_commit'] = row['commit_id']
        if row['snapshot']:
            refs['resumed_from_step'] = str(json.loads(row['snapshot'])['step'])
        latest = (state, detail, refs)
        if self._last_observation == latest:
            return
        self._sequence += 1
        evidence = [dict(kind='training', resource=name, id=value) for name, value in refs.items()]
        self.run.journal.observe_nodes([dict(action_id=self.action.action_id, node_id=self.occurrence,
            occurrence=0, document=dict(self._scope_fields, sequence=self._sequence, state=state,
                                        detail=detail, evidence=evidence))])
        self._last_observation = latest

    def _claim(self):
        run = self.run
        if not getattr(self, '_routed', False):
            # A crash after the project election must leave a discoverable
            # original run route. Recording a candidate never grants ownership.
            route = dict(action=self.action.action_id, occurrence=self.occurrence, instance=self.request.instance_id,
                         project=self.request.project_id, binding_digest=identity(self.binding.to_dict()))
            with run.journal.fit_guard(run.fence, self.action.action_id, self.occurrence):
                pass
            run.journal.bind_source(run.fence,'fit-route-'+identity([self.action.action_id,self.occurrence]),route)
            self._routed = True
        with run.journal.fit_guard(run.fence, self.action.action_id, self.occurrence):
            row = self.index.claim_or_get(self.request, run_id=run.ref.run_id, run_dir=str(run.directory),
                                         action=self.action.action_id, occurrence=self.occurrence,
                                         attempt=run.attempt)
        return row

    def _change(self, operation, *args, **kwargs):
        # An epoch in the project database alone cannot authorize the old
        # driver after the producer run has replaced its execution fence.
        with self.run.journal.fit_guard(self.run.fence, self.action.action_id, self.occurrence):
            return operation(*args, **kwargs)

    def _owner(self, row):
        return (row['run_id'], row['action'], row['occurrence']) == (
            self.run.ref.run_id, self.action.action_id, self.occurrence)

    def _control(self, row, operation='observe', **values):
        # Bound the actual launch payload separately from logical fit identity.
        # A restart/follower may use different run coordinates; the producer's
        # replay always presents the original fixed input materializations.
        return self.run.journal.fit_submission(
            self.run.fence, self.action.action_id, self.occurrence, instance=row['instance'],
            request_key=row['request_key'], digest=identity([self._launch(row).to_dict(),
                                                            self.binding.to_dict()]),
            scope=row['provider_scope'], operation=operation, **values)

    def _launch(self, row):
        if not row['snapshot']:
            return self.request
        return self.request.for_resume(TrainingSnapshotRef.from_dict(json.loads(row['snapshot'])))

    def _coordinator(self, row):
        scope = _scope(row)
        declaration = dict(kind='model_manifest', instance=row['instance'], occurrence=row['occurrence'])
        self.run.journal.bind_commit_scope(self.run.fence, scope, self.action.action_id, declaration)
        action = RunAction(self.run, scope, declaration)
        return CommitCoordinator(action), _ModelManifestSink(self.index, row)

    def _reuse(self, row):
        ref = _confirmed_manifest(row)
        if ref is None:
            if self._owner(row):
                coordinator, sink = self._coordinator(row)
                coordinator.recover({'model_manifest': sink})
            return None
        self._completed = self.store.load(ref, self.request)
        self.run.journal.bind_source(self.run.fence, self._consumer('confirmed'), dict(
            schema='demiflow.fit_consumption.v1', action=self.action.action_id, occurrence=self.occurrence,
            fit_key=row['fit_key'], instance=row['instance'], producer_run=row['run_id'],
            producer_action=row['action'], producer_commit=row['commit_id'], manifest=ref.to_dict()))
        self._manifest = ref
        self._observe(row, 'succeeded' if self._owner(row) else 'reused', 'confirmed_model_manifest')
        return self._completed

    def selected_model(self, result, step, predictor_digest):
        if result is not self._completed or result is None:
            raise FitConflict("Cannot select a model before its verified publication")
        return _selection(result, step, predictor_digest, self._manifest.sha256)

    def _consumer(self, kind, key=None):
        return _consumer_id(self.action, self.occurrence, kind, key)

    def hold_active(self):
        self._change(self.index.hold, self._consumer('active'), self.request.instance_id, kind='active',
                        target=dict(run_id=self.run.ref.run_id, action=self.action.action_id))

    def release_active(self):
        self._change(self.index.release_active, self._consumer('active'), self.request.instance_id)

    def retain_table(self, write_id, provenance, *, receipt=None):
        if write_id != self.action.action_id or type(provenance) is not dict:
            raise FitConflict("Retention requires the consuming action's original write identity")
        if receipt is not None:
            confirm_table_consumption(self.action, self.occurrence, self.binding, receipt)
            return
        owner = self._consumer('table', write_id)
        row = self.index.read(self.request.instance_id)
        if not row or row['state'] != 'published' or self._completed is None:
            raise FitConflict("Scoring table requires its verified model")
        selection = []
        for item in provenance['selection']:
            snap, = self._completed.select((item['step'],))
            selection.append({**item, 'weight_digest':identity({name:[obj.sha256,obj.size_bytes]
                             for name,obj in snap.files.items()}), 'model_format':snap.model_format})
        provenance = {**provenance, 'selection':selection,
                      'training_inputs':{role: {**ref.identity(),'version':ref.version}
                                         for role,ref in self.request.inputs.items()}}
        self.run.journal.bind_source(self.run.fence, owner, dict(
            instance=row['instance'], fit_key=row['fit_key'], producer=row['run_id'],
            manifest=self._manifest.sha256, base_model=self.request.semantics['base_model_digest'],
            provenance=provenance))
        self._change(self.index.hold, owner, self.request.instance_id, kind='pending_table',
                     target=dict(provenance=provenance, receipt=None))

    def retain_parent(self, manifest_digest, step):
        return read_parent(self.request.project_id, self.binding, manifest_digest, step,
                           consumer_instance=self.request.instance_id, check_stop=self.run.check_stop)

    def _permit(self, row, grant):
        # Cancellation commits under the same original publication lease.
        # Keep the project lock out of vendor I/O and the journal transaction.
        run = self.run
        with run.journal.publication_guard(run.fence):
            if not self.index.launch_is_active(row['instance'],row['request_key'], run_id=run.ref.run_id,
                    action=self.action.action_id, occurrence=self.occurrence, attempt=run.attempt):
                raise FitConflict("Cancelled training cannot dispatch an external operation")
            grant()

    def _copy_inputs(self, row):
        """Give every provider upload its own original effect and immutable receipt."""
        if self._copies is not None:
            return True
        copied = dict(self.request.inputs)
        for role, target in self.binding.input_destinations.items():
            if role not in copied:
                raise FitConflict("Provider input copy refers to an undeclared role")
            source = copied[role]
            key = identity(['training_input_copy', self.request.project_id, source.identity(),
                            self.binding.provider_scope, target])
            def effect(operation, **kwargs):
                return self.run.journal.fit_submission(
                    self.run.fence, self.action.action_id, self.occurrence, instance=row['instance'],
                    request_key=key, digest=identity([source.identity(), target]),
                    scope=self.binding.provider_scope, operation=operation, input_copy=True, **kwargs)
            fact = effect('observe')
            if fact['state'] == 'confirmed':
                receipt = InputCopyReceipt.from_dict(fact['copy'])
            elif fact['state'] in ('sent','unknown'):
                try:
                    receipt = (yield from self._call('lookup_input', key))
                except (OSError, TimeoutError):
                    receipt = None
                if receipt is None:
                    if fact['state'] != 'unknown':
                        effect('unknown')
                    return False
            else:
                if fact['state'] == 'absent':
                    effect('reserve')
                def permit_copy():
                    self._permit(row,lambda:effect('send'))
                try:
                    receipt = (yield from self._call('stage_input', source, target, key,before_start=permit_copy))
                except Exception:
                    if effect('observe')['state'] == 'sent':
                        effect('unknown')
                    else:
                        raise
                    return False
            if type(receipt) is not InputCopyReceipt or (receipt.scope, receipt.request_key,
                    receipt.destination) != (self.binding.provider_scope, key, target):
                raise FitConflict("Remote input copy belongs to a different account, key or destination")
            # A copy may receive a different storage version. Keep the original
            # source proof in semantics while verifying all content coordinates.
            remote = replace(receipt.dataset, source_digest=source.identity()['source'])
            if remote.identity() != source.identity():
                raise FitConflict("Remote input content, order or schema differs from the fixed source")
            if fact['state'] != 'confirmed':
                effect('confirm', job=receipt.to_dict(), evidence=dict(scope=receipt.scope, request_key=key))
                return False
            copied[role] = remote
        self._copies = copied
        return True

    @property
    def _same_key_redelivery(self):
        return self.binding.hosting == 'EngineManaged' or self.binding.idempotent_submit

    def _dispatch(self, row, *, evidence=None):
        key = row['request_key']
        def grant():
            if evidence is None:
                self._control(row, 'send')
            else:
                self._control(row, 'redeliver', evidence=evidence, idempotent=self._same_key_redelivery)
        # An ambiguous return must keep the already-sent identity and budget.
        try:
            request = replace(self._launch(row), inputs=self._copies)
            job = (yield from self._call('submit', request, self.binding, key,
                                         before_start=lambda:self._permit(row,grant)))
        except FitSubmissionRejected as exc:
            if self.binding.hosting != 'EngineManaged':
                self._control(row, 'unknown')
                raise FitConflict("Provider admission rejection lacks managed allocation evidence") from exc
            reason = str(exc)[:1024]
            self._control(row, 'reject_unsent', evidence=dict(scope=row['provider_scope'],request_key=key,
                                                              admitted=False,reason=reason))
            self._change(self.index.release_unsent_cost, row['instance'], row['epoch'], key)
            self._change(self.index.terminate, row['instance'], row['epoch'], key, reason)
            raise
        except Exception:
            if self._control(row)['state'] not in ('sent','unknown'):
                raise  # No send permission was acquired.
            self._control(row, 'unknown')
            return
        if type(job) is not JobRef or job.scope != row['provider_scope']:
            self._control(row, 'unknown')
            raise FitConflict("Returned job is outside the sealed provider scope")
        self._control(row, 'confirm', job=job.to_dict(), evidence=dict(
            scope=job.scope, request_key=key, source='submit_response'))

    def _lookup(self, row, fact, *, may_redeliver=True):
        key = row['request_key']
        try:
            lookup = (yield from self._call('lookup', key))
        except (OSError, TimeoutError):
            return
        if type(lookup) is not JobLookup:
            raise FitConflict("Provider returned an invalid lookup")
        if lookup.status == 'unknown':
            if fact['state'] != 'unknown':
                self._control(row, 'unknown')
            return
        evidence = lookup.to_dict()['evidence']
        if evidence.get('scope') != row['provider_scope'] or evidence.get('request_key') != key:
            raise FitConflict("Provider lookup evidence belongs to a different submission")
        if lookup.status == 'found':
            self._control(row, 'confirm', job=lookup.job.to_dict(), evidence=evidence)
            return
        expires = evidence.get('idempotency_expires_at')
        if (not self._same_key_redelivery or evidence.get('authoritative') is not True
                or type(expires) not in (float, int) or not expires > time.time()):
            return
        if row['state'] == 'cancelling':
            self._control(row, 'confirm_absent', evidence=evidence, idempotent=True)
        elif may_redeliver and fact['sends'] == 1:
            yield from self._dispatch(row, evidence=evidence)

    def _snapshot(self, row, snapshot):
        if type(snapshot) is not TrainingSnapshotRef or snapshot.model_format != self.request.semantics['model_format']:
            raise FitConflict("Provider returned an incompatible training snapshot")
        return self.store.snapshot(snapshot, row['instance'], row['request_key'])

    def _interrupted(self, row, job):
        coordinates = row['instance'], row['epoch'], row['request_key']
        if self.binding.hosting == 'EngineManaged' and row['launch'] < self.binding.max_resumes:
            read_steps = getattr(self.provider, 'completed_snapshots', None)
            snapshots = read_steps(job) if read_steps else (self.provider.latest_snapshot(job),)
            if type(snapshots) not in (tuple,list) or len(snapshots) > 16384:
                raise FitConflict("Invalid recovered snapshot list")
            snapshots = [self._snapshot(row, s) for s in snapshots if s is not None]
            saved = self._change(self.index.save_steps, *coordinates, snapshots)
            latest = next((s for s in reversed(saved) if s.resume_files), None)
            # No resume loop on an old snapshot with no new verified progress.
            if latest is not None and (not row['snapshot'] or
                    latest.step > json.loads(row['snapshot'])['step']):
                self._change(self.index.relaunch, *coordinates, latest)
                return
        self._change(self.index.terminate, *coordinates, "Interrupted training has no eligible recovery snapshot")

    def _publish(self, row, job):
        coordinator, sink = self._coordinator(row)
        fact = self.run.journal.latest_execution_commit(_scope(row))
        if fact and fact['status'] != 'pending':
            coordinator.recover({'model_manifest': sink})
            return
        try:
            result = (yield from self._call('result', job))
        except (OSError, TimeoutError):
            return  # The completed remote job remains the sole result source.
        if (type(result) is not FitResult or result.fit_key != self.request.fit_key
                or result.instance_id != self.request.instance_id):
            raise FitConflict("Result belongs to a different training request")
        snapshots = [self._snapshot(row, s) for s in result.snapshots]
        merged = self._change(self.index.save_steps, row['instance'], row['epoch'], row['request_key'], snapshots)
        gpu_seconds = result.metrics.get('allocated_gpu_seconds')
        if self.binding.hosting == 'EngineManaged':
            read_usage = getattr(self.provider,'allocated_gpu_seconds',None)
            gpu_seconds = read_usage(self.index.request_keys(row)) if read_usage else None
        if gpu_seconds is not None and (type(gpu_seconds) not in (int,float) or gpu_seconds < 0):
            raise FitConflict("Invalid training allocation duration")
        result = replace(result, snapshots=merged, metrics={**result.metrics,'allocated_gpu_seconds':gpu_seconds})
        validate_result(result, self.request)
        manifest = self.store.save_manifest(result)
        coordinator.commit(1, outputs={'model_manifest': (sink, [manifest])},
                           position=SourcePosition(manifest.sha256, 1))

    def _inspect_job(self, row, job):
        if row['state'] == 'cancelling':
            # Managed inspection can activate a ready worker. Withdraw the
            # launch before any inspection, including stopped-run reconciliation.
            try:
                yield from self._call('cancel',job)
            except (OSError,TimeoutError):
                return 'unknown'
        try:
            return (yield from self._call('inspect',job))
        except (OSError,TimeoutError):
            return 'unknown'

    def _settle(self, row, billed):
        if self.binding.max_charge:
            self._change(self.index.settle_cost, row['instance'], row['epoch'], row['request_key'], billed)

    def advance(self):
        """Perform one tick. A blocked third-party call never holds the driver."""
        if getattr(self.run, 'stopped_publication', False):
            raise FitConflict("Stopped control cannot dispatch training")
        if self._active_step is None:
            self._active_step = self._advance_once()
        try:
            next(self._active_step)
        except StopIteration as done:
            self._active_step = None
            return done.value
        except BaseException:
            self._active_step = None
            raise
        return None

    def _advance_once(self):
        if self._completed is not None:
            return self._completed
        row = self._claim()
        if row['state'] == 'published':
            return self._reuse(row)
        if row['state'] in ('failed','cancelled'):
            self._observe(row, 'failed' if row['state'] == 'failed' else 'incomplete', row['state'])
            raise FitConflict(row['reason'] or row['state'])
        if not self._owner(row):
            self._observe(row, 'pending', 'waiting_for_original_producer')
            return None
        coordinates = row['instance'], row['epoch'], row['request_key']
        fact = self._control(row)
        if fact['state'] != 'confirmed':
            self._observe(row, 'running', 'submission_' + fact['state'])
        if row['state'] != 'cancelling':
            if not self._capability_checked:
                if self.binding.input_destinations and not all(
                        callable(getattr(self.provider, m, None)) for m in ('stage_input','lookup_input')):
                    raise FitConflict("Remote input destinations require recoverable provider uploads")
                if self.binding.max_charge and (not callable(getattr(self.provider, 'actual_charge', None))
                        or not callable(getattr(self.provider, 'supports_charge_cap', None))
                        or (yield from self._call('supports_charge_cap', self.binding)) is not True):
                    raise FitConflict("Paid training requires a verifiable hard per-job charge cap")
                self._capability_checked = True
            if not (yield from self._copy_inputs(row)):
                return None
        if fact['state'] in ('absent', 'reserved', 'unsent'):
            if fact.get('rejection'):
                reason = fact['rejection']['reason']
                self._change(self.index.terminate, *coordinates, reason)
                raise FitSubmissionRejected(reason)
            if row['state'] == 'cancelling':
                if fact['state'] != 'unsent':
                    self._control(row, 'release_unsent')
                # Before a reservation there is nothing to charge.
                self._change(self.index.release_unsent_cost, *coordinates)
                self._change(self.index.terminate, *coordinates, 'Cancelled before submission')
                return None
            if fact['state'] == 'absent':
                self._change(self.index.reserve_cost, *coordinates)
                self._control(row, 'reserve')
            yield from self._dispatch(row)
            return None
        if fact['state'] in ('sent','unknown'):
            yield from self._lookup(row, fact)
            return None
        job = JobRef.from_dict(fact['job'])
        self._change(self.index.bind_job, *coordinates, job)
        status = yield from self._inspect_job(row, job)
        if status not in ('pending','running','unknown','succeeded','failed','cancelled','interrupted'):
            raise FitConflict("Invalid provider job state")
        self._observe(row, 'running', ('cancel_requested_' if row['state'] == 'cancelling' else 'job_') + status)
        if status in ('pending','running','unknown'):
            return None
        if self.binding.max_charge:
            self._settle(row, (yield from self._call('actual_charge', job)))
        if row['state'] == 'cancelling' or status in ('failed','cancelled'):
            self._change(self.index.terminate, *coordinates, 'Provider job ended: ' + status)
        elif status == 'interrupted':
            self._interrupted(row, job)
        else:
            yield from self._publish(row, job)
        return None

    def reconcile_accepted(self):
        """Stopped-run control can settle an existing submit. It cannot start any job."""
        row = self._claim()
        if not self._owner(row):
            raise FitConflict("Stopped-fit reconciliation requires the original producer")
        if row['state'] == 'published':
            self._reuse(row)
            return 'published'
        if row['state'] in ('cancelled','failed'):
            return row['state']
        fact = self._control(row)
        if fact['state'] in ('sent','unknown'):
            yield from self._lookup(row, fact, may_redeliver=False)
            fact = self._control(row)
        coords = row['instance'], row['epoch'], row['request_key']
        if fact['state'] != 'confirmed':
            if fact.get('rejection') or row['state'] == 'cancelling' and fact['state'] in ('reserved','unsent','absent'):
                if fact['state'] != 'unsent':
                    self._control(row,'release_unsent')
                self._change(self.index.release_unsent_cost,*coords)
                self._change(self.index.terminate,*coords,(fact.get('rejection') or {}).get('reason','cancelled'))
            return fact['state']
        job = JobRef.from_dict(fact['job'])
        self._change(self.index.bind_job,*coords,job)
        status = yield from self._inspect_job(row, job)
        if status in ('running','pending','unknown'):
            return status
        if status not in ('failed','cancelled','interrupted','succeeded'):
            raise FitConflict("Invalid original provider status")
        if self.binding.max_charge:
            self._settle(row,(yield from self._call('actual_charge', job)))
        if row['state'] == 'cancelling' or status in ('failed','cancelled'):
            self._change(self.index.terminate,*coords,status)
        elif status == 'succeeded':
            yield from self._publish(row,job)
            row = self.index.read(row['instance'])
            return ('published' if row['state'] == 'published' and _confirmed_manifest(row)
                    else 'succeeded_unpublished')
        # An interrupted allocation can only be restarted by the original action driver.
        return status

    def cancel(self):
        """Record cancellation of an owned instance and reconcile its existing submit.

        A follower detaches. It never cancels or releases its producer's job.
        """
        run = self.run
        with run.journal.publication_guard(run.fence):
            existing = self.index.read(self.request.instance_id)
            if not existing or not self._owner(existing) or existing['state'] == 'published':
                return True
            row = self._claim()
            coordinates = row['instance'], row['epoch'], row['request_key']
            self._change(self.index.cancel, *coordinates)
        if row['state'] == 'claimed' and self._active_step is not None:
            # Discard the suspended admission/receive continuation. If a call
            # was sent, the ledger still owns it and lookup will recover it.
            self._active_step.close()
            self._active_step = None
        if row['state'] in ('claimed', 'cancelling'):
            self.advance()
        final = self.index.read(row['instance'])
        if final['state'] in ('failed','cancelled'):
            self._observe(final, 'failed' if final['state'] == 'failed' else 'incomplete', final['state'])
            return True
        return final['state'] == 'published'
