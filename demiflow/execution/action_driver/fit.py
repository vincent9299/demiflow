"""Resolve sealed job dependencies before admitting their row consumers."""
import time

from ..contracts.dataflow import thaw_record_value
from ..contracts.dataflow_errors import DataflowError
from ..contracts.training import FitRequest
from ..recovery_contract import identity
from ..seal.plan import load_sealed_dataflow
from ..training.deployment import bind_action_deployment, resolve_training_deployment
from ..training.inputs import fix_input
from ..training.runtime import FitRuntime, confirm_table_consumption, consumed_model_digest, read_consumed_fit, read_parent


class FitDependencies:
    def __init__(self, *, action, services, graph, physical, sealed, scheduler, check_stop):
        self.action, self.services = action, services
        self.graph, self.physical, self.sealed = graph, physical, sealed
        self.scheduler, self.check_stop = scheduler, check_stop
        self.deployment = resolve_training_deployment(action.context)
        _, _, code = load_sealed_dataflow(sealed, action_id=action.action_id,
                                           store=services.store, bounds=services.bounds)
        self.pins = code['programs']
        self.owners = {o: s.stage_id for s in physical.stages for o in s.occurrences}
        self.sources = set(graph.sources)
        self.controls, self.results, self.bindings = {}, {}, {}
        self.fixed = {}
        bind_action_deployment(action, self.deployment.project_id, self.deployment.binding)

    def _control(self, job, parents, image):
        conf = job.configuration
        schemas = {'train': conf['train_schema'], **conf['monitor_schemas']}
        fixed, parent = {}, thaw_record_value(conf['parent'])
        for edge in parents:
            if edge.kind == 'model_manifest':
                producer = self.controls[edge.producer]
                manifest = producer.selected_model(self.results[edge.producer], edge.step,
                                                   identity(self.pins[job.occurrence_id]))['manifest_digest']
                parent = {'manifest_digest': manifest, 'step': edge.step}
                continue
            schema = schemas[edge.input_port]
            media = conf['media_fields'].get(edge.input_port, ())
            key = (edge.producer, identity([dict(schema), list(media)]))
            if key not in self.fixed:
                self.fixed[key] = fix_input(edge.producer, schema, graph=self.graph, physical=self.physical,
                        action=self.action, image=image, services=self.services, check_stop=self.check_stop,
                        media=media, media_roots=self.deployment.binding.provider_options.get('media_roots', ()))
            fixed[edge.input_port] = self.fixed[key]
        budget = conf['semantics']['budget']
        if 'max_epochs' in budget:
            epochs = budget['max_epochs']
            batch = conf['semantics']['topology'].get('effective_batch')
            examples = budget.get('examples_per_epoch', fixed['train'].rows)
            if any(type(v) is not int or v < 1 for v in (epochs, batch, examples)):
                raise ValueError("Epoch budget requires positive epochs, examples and effective batch")
            if budget['optimizer_updates'] > epochs * ((examples + batch - 1) // batch):
                raise ValueError("Training update budget is unreachable for the fixed dataset")
        binding = self.deployment.binding
        parent_snapshot = (read_parent(self.deployment.project_id, binding, **parent, check_stop=self.check_stop) if parent else None)
        semantics = {**thaw_record_value(conf['semantics']), 'trainer_digest': identity(self.pins[job.occurrence_id])}
        if binding.hosting == 'ProviderManaged':
            semantics.update(provider_scope=binding.provider_scope,
                             implementation_digest=binding.implementation_digest)
        request = FitRequest(self.deployment.project_id, fixed, semantics, retrain_id=conf['retrain_id'],
                             parent=parent_snapshot)
        provider = self.deployment.provider
        if binding.hosting == 'EngineManaged':
            from demiflow.execution.hosts.training_host import ManagedFitHost
            provider = ManagedFitHost(action=self.action, services=self.services, scheduler=self.scheduler,
                         sealed=self.sealed, occurrence=job.occurrence_id, binding=binding,
                         job_resources=self.services.job_resources, timeout_ms=self.services.policy.control_timeout_ms)
        runtime = FitRuntime(self.action, job.occurrence_id, request, binding, provider)
        if parent:
            runtime.retain_parent(**parent)
        return runtime

    def ready(self, stage_ids, *, completed, image):
        for job in self.graph.jobs:
            key = job.occurrence_id
            if key in self.results:
                continue
            edges = [edge for edge in self.graph.value_edges if edge.consumer == key]
            if any(edge.producer not in self.results for edge in edges if edge.kind == 'model_manifest'):
                continue
            if any(self.owners[edge.producer] not in completed for edge in edges
                   if edge.kind == 'fixed_dataset' and edge.producer not in self.sources):
                continue
            if key not in self.controls:
                self.controls[key] = self._control(job, edges, image)
            result = self.controls[key].advance()
            if result is None:
                return False
            self.results[key] = result
            self.controls[key].hold_active()
        for edge in self.graph.value_edges:
            if edge.input_port != 'model' or self.owners[edge.consumer] not in stage_ids:
                continue
            if edge.producer not in self.results:
                raise DataflowError('INVALID_CONTRACT', 'Prediction scheduled before its fixed training inputs')
            model = self.controls[edge.producer].selected_model(self.results[edge.producer], edge.step,
                                                                identity(self.pins[edge.consumer]))
            digest = identity(model)
            if image and edge.consumer in (image.model_bindings or {}) and image.model_bindings[edge.consumer] != digest:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Prediction model changed across checkpoint recovery')
            self.action.context.journal.bind_source(self.action.context.fence,
                identity(['prediction', self.action.action_id, edge.consumer]), {'model_digest': digest})
            self.bindings[edge.consumer] = model
        return True

    def checkpoint_bindings(self, image):
        return {**(thaw_record_value(image.model_bindings) if image and image.model_bindings else {}),
                **{occ: identity(model) for occ, model in self.bindings.items()}}

    def verify_completed(self, image):
        edges = [e for e in self.graph.value_edges if e.input_port == 'model']
        recovered = thaw_record_value(image.model_bindings) if image.model_bindings else {}
        if set(recovered) != {e.consumer for e in edges}:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Final checkpoint prediction membership differs')
        confirmed = {}
        for edge in edges:
            if edge.producer not in confirmed:
                confirmed[edge.producer] = read_consumed_fit(self.action,edge.producer,
                    self.deployment.project_id,self.deployment.binding)
            result, manifest = confirmed[edge.producer]
            digest = consumed_model_digest(result,manifest,edge.step,identity(self.pins[edge.consumer]))
            original = self.action.context.journal.source_binding(
                identity(['prediction',self.action.action_id,edge.consumer]))
            if original != {'model_digest': digest} or recovered[edge.consumer] != digest:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Final checkpoint model binding differs')

    def retain_table(self, uri, *, receipt=None):
        for job in self.graph.jobs:
            occurrence = job.occurrence_id
            selection = [{'step': e.step, 'predictor_digest': identity(self.pins[e.consumer]),
                          'prediction': e.consumer}
                         for e in self.graph.value_edges if e.producer == occurrence and e.input_port == 'model']
            if not selection:
                continue
            if receipt is not None:
                confirm_table_consumption(self.action, occurrence, self.deployment.binding, receipt)
                continue
            fixed_sources = {s.source_id: {'identity_digest':identity(thaw_record_value(s.immutable_identity)),
                                           'version':s.immutable_identity.get('version')}
                             for s in self.physical.sources}
            provenance = {'uri': uri, 'selection': selection, 'sources': fixed_sources,
                          'plan_id': self.physical.plan_id, 'dag_id': self.graph.dag_id}
            self.controls[occurrence].retain_table(self.action.action_id, provenance)

    def cancel(self):
        for runtime in self.controls.values():
            runtime.cancel()

    def drain_cancel(self, deadline):
        pending = list(self.controls.values())
        while pending:
            pending = [runtime for runtime in pending if not runtime.cancel()]
            if not pending:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataflowError('UNKNOWN_EFFECT', 'Original fit cancellation still awaits provider confirmation')
            time.sleep(min(.05, remaining))

    def release(self):
        for key in self.results:
            self.controls[key].release_active()
