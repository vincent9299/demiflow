"""Optional native action checks and durable observations, without scheduling.

The caller supplies immutable connection/type facts and a durable event sink.
Actual Python constructs the ordinary Dataset; this module checks its native
logical plan before an action, then attaches value checks to existing callables.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
import inspect
import json
import uuid
import threading
import hashlib
from pathlib import Path

from demiflow.data import plan as ops
from demiflow.data.sources import KeyedSource, LanceSource, MaterializedSource, UnionSource
from demiflow.data.execution_contract import PlanView
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.plan_facts import operation_facts, source_facts
from demiflow.planning.traits import check_composition

_CURRENT = ContextVar('demiflow_checked_execution', default=None)


def current():
    return _CURRENT.get()


def _equal(actual, expected, label):
    if json.dumps(actual, sort_keys=True, allow_nan=False) != json.dumps(expected, sort_keys=True, allow_nan=False):
        raise RuntimeContractError('Native plan differs at ' + label)


class CheckedExecution:
    def __init__(self, *, nodes, edges, actions, operators, source_root, limits, record, check_stop, observe=None, resource_root=None, work_root=None, report_digest=None, targets=None):
        self.nodes = {n['id']: n for n in nodes}
        self.parents = {n['id']: [] for n in nodes}
        for edge in sorted(edges, key=lambda e: e['port']):
            self.parents[edge['to']].append(edge['from'])
        self.actions = list(actions)
        self.operators = {(o['source'], o['name']): o for o in operators}
        self.source_root = Path(source_root).resolve()
        self.resource_root = Path(resource_root).resolve() if resource_root is not None else None
        self.work_root = Path(work_root).resolve() if work_root is not None else self.resource_root / 'work' if self.resource_root is not None else None
        self._limits, self.record, self.check_stop = limits, record, check_stop
        self.report_digest = report_digest
        self.targets = targets
        self.position, self.handles = 0, {}
        self.active = None
        self.operation_id = None
        self.instances = {}
        self.observe = observe
        self.operation_bindings = {}
        self.source_bindings = {}
        self.observations = {}
        self.occurrences = {}
        self.matching_action = None
        self.observation_lock = threading.RLock()
        self.last_flush = 0.0
        self.node_flushed = {}
        self.actor_bindings, self.actor_states = {}, {}
        self.node_actors = {}
        self.values = {}
        self.checkpoints = {}
        self.checkpoint_sources = {}

    @property
    def limits(self):
        from demiflow.execution.pipeline.run_context import current_run
        run = current_run()
        if run is None:
            return self._limits
        from demiflow.execution.contracts.dataflow import PAYLOAD_FIELDS
        return {**self._limits, **{name: run.limits[name] for name in PAYLOAD_FIELDS}}

    from demiflow.execution.journal.dataflow_operator_observations import match_observation as _match

    from demiflow.execution.journal.dataflow_operator_observations import bind_observation_scope

    from demiflow.execution.journal.dataflow_operator_observations import progress

    from demiflow.execution.journal.dataflow_operator_observations import rows

    from demiflow.execution.journal.dataflow_operator_observations import duration

    from demiflow.execution.journal.dataflow_operator_observations import group_duration

    from demiflow.execution.journal.dataflow_operator_observations import actor_created

    from demiflow.execution.journal.dataflow_operator_observations import actor_lifecycle

    from demiflow.execution.journal.dataflow_operator_observations import worker_actor_lifecycle

    from demiflow.execution.journal.dataflow_operator_observations import flush_observations

    def bind(self, spec, node, observation=None, *, callback=None):
        p, operator = ({}, callback) if callback is not None else (node['parameters'], node['operator'])
        target = spec.target
        construction = operator.get('construction')
        if construction is not None:
            owner = target.__self__ if inspect.ismethod(target) else target
            if inspect.isclass(owner) or inspect.isfunction(owner):
                raise RuntimeContractError('Actual actor instance differs from its construction')
            target = type(owner)
            identity = construction['construction_id']
            if identity not in self.instances:
                from demiflow.data.runtime_contract import check_size
                state = object.__getattribute__(owner, '__dict__')
                check_size(state, self.limits['max_row_bytes'])
                _equal(state, construction['state'], node['id'] + '.constructor_state')
                if any(value is owner for value in self.instances.values()):
                    raise RuntimeContractError('Distinct actor constructions share one actual instance')
                self.instances[identity] = owner
            elif self.instances[identity] is not owner:
                raise RuntimeContractError('Shared actor construction changed its actual owner')
        _equal(target.__module__, operator['module'], node['id'] + '.module')
        _equal(target.__name__, operator['name'], node['id'] + '.callable')
        expected_path = self.source_root / (operator['module'].replace('.', '/') + '.py')
        if Path(inspect.getfile(target)).resolve() != expected_path:
            raise RuntimeContractError('Callable is not from the sealed source closure')
        if node['capability_fact']['driver'] != 'stream' and inspect.isclass(target) and any(hasattr(target, k) for k in ('astart', 'astop', 'aclose', 'close', 'start', 'stop')):
            raise RuntimeContractError('Actor resource lifecycle is not enabled in the synchronous profile')
        for name, value in [('fn_args', spec.call_args), ('fn_kwargs', spec.call_kwargs),
                            ('fn_constructor_args', spec.constructor_args), ('fn_constructor_kwargs', spec.constructor_kwargs)]:
            from demiflow.execution.pipeline.values import resolve
            _equal(value, resolve(p.get(name) or ([] if name.endswith('args') and not name.endswith('kwargs') else {}), self.values), node['id'] + '.' + name)
        description = self.operators[(operator['module'].replace('.', '/') + '.py', operator['name'])]
        contract = {k: description[k] for k in ('parameters', 'returns')}
        contract.update(label=node['id'] + ':' + operator['name'], max_row_bytes=self.limits['max_row_bytes'])
        contract['awaitable'] = node['capability_fact']['kind'] in {'async_map', 'batch_map'}
        contract['max_output_rows'] = self.limits['max_output_rows']
        if observation is not None:
            contract['observation'] = list(observation)
        if contract['awaitable']:
            contract['max_return_bytes'] = self.limits['max_batch_bytes']
        if node['capability_fact']['kind'] == 'batch_map':
            contract['max_input_bytes'] = self.limits['max_batch_bytes']
        return replace(spec, runtime_contract=contract)

    def _checked_fit(self, fit, identity, depth):
        """Check the actual recipe and recursively bind its named input plans."""
        if depth > 128 or identity not in self.nodes or self.nodes[identity]['kind'] != 'fit':
            raise RuntimeContractError('Invalid native training dependency')
        if identity in self.checked_fits:
            original, checked = self.checked_fits[identity]
            if original is not fit:
                raise RuntimeContractError('Distinct fits share a checked training declaration')
            return checked
        from demiflow.data.training import ModelSelection, training_json
        expected = self.nodes[identity]['normalized_parameters']
        def callable_ref(target):
            module = target.__module__
            if Path(inspect.getfile(target)).resolve() != self.source_root / (module.replace('.', '/') + '.py'):
                raise RuntimeContractError('Training program is not from the sealed source closure')
            return {'callable': module + ':' + target.__qualname__}
        recipe = fit.recipe
        actual = {**vars(recipe), 'trainer': callable_ref(recipe.trainer), 'predictor': {
            **vars(recipe.predictor), 'factory': callable_ref(recipe.predictor.factory)}}
        _equal(training_json(actual), expected['recipe'], identity + '.recipe')
        _equal(fit.retrain_id, expected['retrain_id'], identity + '.retrain_id')
        parent = fit.parent
        if isinstance(parent, ModelSelection):
            selected = expected['resume_from']
            if not isinstance(selected, dict) or 'fit_node' not in selected:
                raise RuntimeContractError('Training parent differs from the checked model selection')
            _equal(list(parent.steps), selected['steps'], identity + '.parent_steps')
            parent = replace(parent, declaration=self._checked_fit(parent.declaration, selected['fit_node'], depth+1))
        else:
            _equal(training_json(parent), expected['resume_from'], identity + '.resume_from')
        parents = self.parents[identity]
        roles = ['train', *sorted(recipe.monitor_schemas)]
        _equal([v[0] for v in fit.inputs], roles, identity + '.roles')
        if len(parents) != len(roles):
            raise RuntimeContractError('Training input count differs from its checked declaration')
        inputs = tuple((role, *self.plan(source, plan, node, depth+1))
                       for (role, source, plan), node in zip(fit.inputs, parents))
        checked = replace(fit, inputs=inputs, parent=parent)
        observation = self._match(identity)
        if observation is not None:
            self.operation_bindings[id(checked)] = observation
        self.checked_fits[identity] = (fit, checked)
        return checked

    def _operation(self, op, node):
        identity = node['id']
        facts = operation_facts(op)
        _equal(facts['kind'], node['capability_fact']['kind'], identity + '.kind')
        _equal(facts['native_options'], node['capability_fact']['native_options'], identity + '.native_options')
        _equal(facts.get('resource_declaration'), node['capability_fact'].get('resource_declaration'),
               identity + '.resource_declaration')
        native = facts.get('native')
        if type(op) is ops.StreamPrependOp:
            # The second actual Dataset was recursively checked in plan(); its
            # engine semantic hash is never independently rebuilt from the AST.
            native = {**native, 'arguments': {k:v for k,v in native['arguments'].items() if k!='source'}}
        _equal(native, node['analysis'].get('native'), identity + '.native')
        expected = node['analysis'].get('stream_arguments', node['normalized_parameters'])
        for name, value in facts['arguments'].items():
            if name == 'catch':
                value = [{'symbol': cls.__module__ + '.' + cls.__qualname__} for cls in value]
            desired = expected.get(name)
            if name == 'inputs' and isinstance(desired, list):
                desired = {key: key for key in desired}
            if name == 'catch':
                desired = desired or []
            _equal(value, desired, identity + '.' + name)
        observation = self._match(identity)
        if hasattr(op, 'callable'):
            if facts.get('native'):
                from demiflow.data.flow_contract import scalar
                row = {**scalar('record'), 'fields': self.nodes[self.parents[identity][0]]['fields']}
                result = {**scalar('record'), 'fields': node['fields']}
                batch = facts['kind'] == 'batch_map'
                contract = {'label': identity, 'parameters': [{'name': 'rows' if batch else 'row',
                            'type': {**scalar('list'), 'item': row} if batch else row}],
                            'returns': {**scalar('list'), 'item': result} if batch else result,
                            'max_row_bytes': self.limits['max_row_bytes'], 'awaitable': True,
                            'max_return_bytes': self.limits['max_batch_bytes']}
                if batch:
                    contract['max_input_bytes'] = self.limits['max_batch_bytes']
                op = replace(op, callable=replace(op.callable, runtime_contract=contract))
                if node.get('callbacks'):
                    from demiflow.data.runtime_contract import validator
                    owner = op.callable.target.__self__ if inspect.ismethod(op.callable.target) else op.callable.target
                    for port, callback in node['callbacks'].items():
                        target = getattr(owner, port)
                        spec = self.bind(ops.CallableSpec(target), node, callback=callback)
                        # The native actor retains the actual callback; the
                        # validator does not replace its source identity.
                        owner._call_validations[port] = validator(target, spec.runtime_contract)
            else:
                op = replace(op, callable=self.bind(op.callable, node, observation))
        if observation is not None:
            self.operation_bindings[id(op)] = observation
            if hasattr(op, 'callable'):
                owner = op.callable.target.__self__ if inspect.ismethod(op.callable.target) else op.callable.target
                if not (inspect.isclass(owner) or inspect.isfunction(owner) or inspect.isbuiltin(owner)):
                    self.actor_created(owner, observation)
        return op

    def plan(self, source, plan, identity, depth=0):
        if depth > 128:
            raise RuntimeContractError('Native plan depth exceeded')
        if len(plan.operations) > 1024:
            raise RuntimeContractError('Native operation budget exceeded')
        actual = [operation_facts(op) for op in plan.operations]
        capability = check_composition(actual)
        if not capability['supported']:
            raise RuntimeContractError(capability['reason'])
        # Resource admission belongs to the actual unified physical plan.
        # The historical "stream" report label does not describe its worker
        # groups or windows; applying the old folded-stream estimate here can
        # reject a legal graph before the sole allocator ever sees it.
        changed = []
        for op in reversed(plan.operations):
            node = self.nodes[identity]
            parents = self.parents[identity]
            if type(op) is ops.PredictOp:
                selection = node['normalized_parameters']['self']
                if len(parents) != 1:
                    raise RuntimeContractError('Prediction requires one checked holdout')
                op = replace(op, fit=self._checked_fit(op.fit, selection['fit_node'], depth+1))
            elif type(op) is ops.StreamPrependOp:
                if len(parents) != 2:
                    raise RuntimeContractError('Prepend requires main and finite prefix ports')
                other: PlanView = op.source_dataset
                prefix_source, prefix_plan = self.plan(other._source,other._plan,parents[1],depth+1)
                prefix = other.replace_plan(prefix_source, prefix_plan)
                op = replace(op,source_dataset=prefix)
            elif len(parents) != 1:
                raise RuntimeContractError('Unary native plan has invalid ports')
            changed.append(self._operation(op, node))
            identity = parents[0]
        source, _ = self._source(source, identity, depth)
        if capability['driver'] == 'stream' and type(source) is LanceSource:
            key = self.source_bindings.pop(id(source), None)
            source = replace(source, query=replace(source.query, batch_size=1))
            if key is not None:
                self.source_bindings[id(source)] = key
        return source, ops.LogicalPlan(tuple(reversed(changed)))

    def _source(self, source, identity, depth):
        plan = ops.LogicalPlan()
        node = self.nodes[identity]
        p, kind, parents = node['parameters'], node['kind'], self.parents[identity]
        fact = source_facts(source)
        from demiflow.execution.pipeline.values import resolve
        if kind == 'read_checkpoint':
            bound = self.checkpoint_sources.get(id(source))
            if bound is None or bound[0] is not source or bound[1] != identity:
                raise RuntimeContractError('Checkpoint read source is not from the checked bridge')
            _equal(fact, bound[2], identity + '.checkpoint_source')
            fact = {'kind': 'read_checkpoint', 'arguments': {}}
        if type(source) is MaterializedSource and kind == 'from_items' and self.handles.get(id(source.handle)) == identity:
            fact = {'kind': 'from_items', 'arguments': {}}
        _equal(fact['kind'], kind, identity + '.source')
        for name, value in fact['arguments'].items():
            _equal(value, resolve(node['normalized_parameters'].get(name), self.values), identity + '.' + name)
        observation = self._match(identity, reused=kind == 'materialize')
        if type(source) is LanceSource:
            q = source.query
            q = replace(q, batch_size=min(q.batch_size or 256, self.limits['batch_rows']), batch_readahead=1, fragment_readahead=1)
            source = replace(source, query=q)
            if observation is not None:
                self.source_bindings[id(source)] = observation
            return source, plan
        if type(source) is KeyedSource:
            if source.kind == 'groups':
                if len(parents)!=1 or source.right is not None:
                    raise RuntimeContractError('Invalid grouping input')
                child,prefix=self.plan(source.left.source,source.left.plan,parents[0],depth+1)
                source=replace(source,left=replace(source.left,source=child,plan=prefix))
                if observation is not None:
                    self.source_bindings[id(source)]=observation
                return source,plan
            if source.kind == 'reduce':
                if len(parents) != 1 or source.right is not None:
                    raise RuntimeContractError('Invalid reducer input')
                child, prefix = self.plan(source.left.source, source.left.plan, parents[0], depth + 1)
                reducer = self.bind(ops.CallableSpec(source.reducer, call_kwargs=source.reducer_kwargs or {}), node, observation)
                source = replace(source, left=replace(source.left, source=child, plan=prefix), reducer_contract=reducer.runtime_contract)
                if observation is not None:
                    self.source_bindings[id(source)] = observation
                return source, plan
            if len(parents) != 2 or source.right is None:
                raise RuntimeContractError('Invalid join inputs')
            left = self.plan(source.left.source, source.left.plan, parents[0], depth + 1)
            right = self.plan(source.right.source, source.right.plan, parents[1], depth + 1)
            source = replace(source, left=replace(source.left, source=left[0], plan=left[1]), right=replace(source.right, source=right[0], plan=right[1]))
            if observation is not None:
                self.source_bindings[id(source)] = observation
            return source, plan
        if type(source) is UnionSource:
            def flattened(ids):
                for child in ids:
                    if self.nodes[child]['kind'] == 'union':
                        # Native union flattens these boundaries. Keep the
                        # logical identity, but do not fabricate row metrics.
                        self._match(child)
                        yield from flattened(self.parents[child])
                    else:
                        yield child
            parents = list(flattened(parents))
            if len(source.inputs) != len(parents):
                raise RuntimeContractError('Invalid union input count')
            inputs = []
            for item, parent in zip(source.inputs, parents):
                child, prefix = self.plan(item.source, item.plan, parent, depth + 1)
                inputs.append(replace(item, source=child, plan=prefix))
            source = replace(source, inputs=tuple(inputs))
            if observation is not None:
                self.source_bindings[id(source)] = observation
            return source, plan
        if type(source) is MaterializedSource:
            if kind != 'read_checkpoint' and self.handles.get(id(source.handle)) != identity:
                raise RuntimeContractError('Materialized handle is not from the checked action')
            source = replace(source)
            if observation is not None:
                self.source_bindings[id(source)] = observation
            return source, plan
        raise RuntimeContractError('Native source differs at ' + identity)

    def checkpoint_facts(self, checkpoint):
        from demiflow.execution.stream_checkpoint import StreamCheckpoint, normalize_checkpoint
        if type(checkpoint) is not StreamCheckpoint or id(checkpoint) not in self.checkpoints:
            raise RuntimeContractError('Checkpoint is not a resource from the checked action')
        actual, identity, path = self.checkpoints[id(checkpoint)]
        if actual is not checkpoint or checkpoint.path != path:
            raise RuntimeContractError('Checkpoint resource identity changed')
        facts = normalize_checkpoint({**checkpoint.contract_arguments, 'outputs': checkpoint.declared_outputs})
        _equal(facts, self.values[identity], identity + '.checkpoint')
        return facts

    def checkpoint_commits(self, checkpoint):
        """Publish native stage references, never fabricate an aggregate receipt."""
        facts = self.checkpoint_facts(checkpoint)
        _, origin, _ = self.checkpoints[id(checkpoint)]
        for stage, ref in checkpoint.state['outputs'].items():
            matches = [node for node in self.nodes.values() if node['kind'] == 'save_lance'
                       and node['parameters']['stage'] == stage and node['parameters']['uri'] == ref['uri']]
            if len(matches) != 1:
                raise RuntimeContractError('Checkpoint output has no unique checked sink')
            self.record(matches[0]['id'], 'stage_commit', {'reference': ref, 'checkpoint_node': origin,
                        'revision': checkpoint.state['revision'], 'schema_identity': facts['outputs'][stage]['schema_identity']})

    def native_save_policy(self, execution, node, writer):
        """Resolve a native writer through the sealed bridge to its target grant."""
        from demiflow.execution.journal.dataflow_stage_facts import stage_binding
        _, binding = stage_binding(execution._action.context.journal,
            execution._action.context.directory, execution._action.action_id, node, self.report_digest)
        declared = self.nodes.get(binding['report_node_id'])
        if (declared is None or declared['kind'] != 'save_lance'
                or declared['parameters']['uri'] != writer.uri
                or declared['parameters']['stage'] != writer.stage
                or declared['parameters']['mode'] != writer.mode):
            raise RuntimeContractError('Native stream writer differs from its checked report node')
        if type(self.targets) is not dict or writer.uri not in self.targets:
            raise RuntimeContractError('Native stream writer has no sealed output grant')
        policy = dict(self.targets[writer.uri])
        if (writer.mode != 'overwrite' or policy.get('mode') not in {'create', 'overwrite'}
                or policy.get('mode') == 'create' and policy.get('expected_version') is not None
                or policy.get('mode') == 'overwrite' and type(policy.get('expected_version')) is not int):
            raise RuntimeContractError('Native stream writer requires create or fixed-baseline overwrite permission')
        return policy

    def native_stage_commit(self, execution, scope_id, commit_id):
        """Project only the original ACK; never turn UI events into commit authority."""
        from demiflow.execution.journal.dataflow_stage_facts import native_stage_commit
        run = execution._action.context
        fact = native_stage_commit(run.journal, run.directory, scope_id=scope_id,
            commit_id=commit_id, report_digest=self.report_digest)
        if fact is None:
            return
        node = self.nodes.get(fact['report_node_id'])
        if (node is None or node['kind'] != 'save_lance'
                or node['parameters']['uri'] != fact['reference']['uri']
                or node['normalized_parameters']['schema']['identity'] != fact['schema_identity']):
            raise RuntimeContractError('Native stage receipt differs from its checked output')
        self.record(node['id'], 'stage_commit', fact)

    def dependency_identity(self, identity):
        """Bind a completed write to its actual fixed sources and native values."""
        from demiflow.execution.pipeline.values import resolve
        pending, visited, digests = list(self.parents[identity]), set(), {}
        while pending:
            child = pending.pop()
            if child in visited:
                continue
            visited.add(child)
            node = self.nodes[child]
            value = {'parameters': resolve(node['normalized_parameters'], self.values), 'contract': node['contract']}
            if node['kind'] == 'read_checkpoint':
                value['reference'] = self.values[child]['reference']
            digest = hashlib.sha256()
            for chunk in json.JSONEncoder(sort_keys=True, allow_nan=False).iterencode(value):
                digest.update(chunk.encode())
            digests[child] = digest.hexdigest()
            pending.extend(self.parents[child])
        return digests

    def execution_bindings(self, source, plan, *, action_id):
        """Return structural positions matched to report observation keys.

        Each item is ``{role, program_index, report_node_id, report_occurrence,
        mode, input_ports, output_ports, source_token}``.  Only real matched
        items are returned; this reads the existing context bindings, never
        re-matches or adds occurrences.  ``role`` is source/operation/terminal;
        ``program_index`` is a non-negative int only for operations, None
        otherwise; ``mode`` is execute/reuse.  ``input_ports``/``output_ports``
        are the real structural ports of the matched role and ``source_token``
        is the matched source's report identity (None for non-sources); the v3
        occurrence/semantic identity is supplied by the sealed graph through the
        identity bridge, never recomputed here from the report occurrence.
        """
        ports = {"source": ([], ["out"]), "operation": (["in"], ["out"]),
                 "terminal": (["in"], [])}
        result = []
        source_key = self.source_bindings.get(id(source))
        if source_key is not None:
            obs = self.observations.get(tuple(source_key))
            mode = "reuse" if obs and obs.get("state") == "reused" else "execute"
            input_ports, output_ports = ports["source"]
            result.append({"role": "source", "program_index": None,
                           "report_node_id": source_key[1],
                           "report_occurrence": source_key[2], "mode": mode,
                           "input_ports": list(input_ports), "output_ports": list(output_ports),
                           "source_token": source_key[1]})
        for index, op in enumerate(plan.operations):
            op_key = self.operation_bindings.get(id(op))
            if op_key is not None:
                obs = self.observations.get(tuple(op_key))
                mode = "reuse" if obs and obs.get("state") == "reused" else "execute"
                input_ports, output_ports = ports["operation"]
                result.append({"role": "operation", "program_index": index,
                               "report_node_id": op_key[1],
                               "report_occurrence": op_key[2], "mode": mode,
                               "input_ports": list(input_ports), "output_ports": list(output_ports),
                               "source_token": None})
        terminal_key = (action_id, action_id, 0)
        if terminal_key in self.observations:
            obs = self.observations[terminal_key]
            mode = "reuse" if obs.get("state") == "reused" else "execute"
            input_ports, output_ports = ports["terminal"]
            result.append({"role": "terminal", "program_index": None,
                           "report_node_id": action_id,
                           "report_occurrence": 0, "mode": mode,
                           "input_ports": list(input_ports), "output_ports": list(output_ports),
                           "source_token": None})
        return tuple(result)

    @contextmanager
    def action(self, kind, source, plan, spec, arguments=None):
        self.check_stop()
        if self.active is not None or self.position >= len(self.actions):
            raise RuntimeContractError('Unexpected or nested Dataset action')
        identity = self.actions[self.position]
        self.matching_action = identity
        self.occurrences = {}
        self.checked_fits = {}
        self.operation_bindings = {}
        self.source_bindings = {}
        self.actor_bindings, self.actor_states = {}, {}
        self.node_actors = {}
        node = self.nodes[identity]
        _equal(kind, node['kind'], identity + '.action')
        if arguments is not None:
            from demiflow.execution.pipeline.values import resolve
            expected_arguments = resolve(node['normalized_parameters'], self.values)
            if kind == 'run_stream':
                # The native action records executable parameters; unused
                # optional callback/retired-resource defaults are declaration
                # facts only. Never discard a non-null declared value.
                expected_arguments = {key: value for key, value in expected_arguments.items()
                    if not (key in {'on_progress', 'on_drain', 'cancellation', 'queue_factory', 'checkpoint'}
                            and value is None and key not in arguments)}
            _equal(arguments, expected_arguments, identity + '.arguments')
        parents = self.parents[identity]
        if not parents:
            if source is not None or plan is not None:
                raise RuntimeContractError('Native action declared no Dataset inputs')
        elif len(parents) != 1:
            raise RuntimeContractError('Action must consume one Dataset')
        self._match(identity)
        driver_route = None
        if parents:
            source, plan = self.plan(source, plan, parents[0])
            driver_route = check_composition([operation_facts(op) for op in plan.operations])['driver']
        intent = {'kind': kind}
        if driver_route is not None:
            intent['driver_route'] = driver_route
        reused_receipt = None
        if spec is not None:
            from demiflow.lance.model import LanceWriteSpec
            p = node['parameters']
            from demiflow.lance.schema import build_schema, schema_identity
            expected = LanceWriteSpec(uri=p['uri'], mode=p['mode'], expected_version=p.get('expected_version'),
                                      schema=build_schema(p['schema']['schema_declaration']) if p.get('schema') is not None else None)
            _equal(spec.to_dict(), expected.to_dict(), identity + '.write')
            if spec.schema is not None:
                _equal(schema_identity(spec.schema), schema_identity(expected.schema), identity + '.write.schema')
            from demiflow.execution.pipeline.run_context import current_run
            from demiflow.execution.recovery_contract import identity as commit_identity
            intent.update(dependencies=self.dependency_identity(identity),
                          spec=spec.to_dict(), request_hash=spec.content_hash,
                          operation_id=commit_identity([current_run().ref.run_id, identity, 1]))
        from demiflow.execution.pipeline.run_context import current_run
        run_context = current_run()
        if run_context is not None and run_context.attempt > 1:
            native = run_context.journal.execution_action(identity)
            if native is not None and native['result'] is not None:
                history = run_context.journal.action_history([identity],
                    before_attempt=run_context.attempt, kinds=['receipt', 'materialized', 'value'])
                if history:
                    intent['reused_from_attempt'] = history[-1]['attempt']
        self.record(identity, 'intent', intent)
        self.active = identity
        self.operation_id = intent.get('operation_id')
        action = CheckedAction(self, identity, source, plan)
        action.reused_receipt = reused_receipt
        try:
            yield action
            if not action.completed:
                raise RuntimeContractError('Native action returned without completion evidence')
            self.position += 1
            self.flush_observations(final=True, succeeded=True)
        except BaseException as exc:
            self.record(identity, 'failed', {'type': type(exc).__name__, 'message': str(exc)[:2048]})
            self.flush_observations(final=True)
            raise
        finally:
            self.active = None
            self.operation_id = None

    @contextmanager
    def bind_context(self):
        if current() is not None:
            raise RuntimeContractError('Checked execution cannot nest')
        token = _CURRENT.set(self)
        try:
            yield self
            if self.position != len(self.actions):
                raise RuntimeContractError('Flow did not execute all checked actions')
        finally:
            _CURRENT.reset(token)


class CheckedAction:
    def __init__(self, owner, identity, source, plan):
        self.owner, self.identity = owner, identity
        self.source, self.plan, self.completed = source, plan, False
        self.reused_receipt = None

    def value(self, value):
        if self.owner is not None:
            from demiflow.data.runtime_contract import check_size, check_value
            check_size(value, self.owner.limits['max_row_bytes'])
            check_value(value, self.owner.nodes[self.identity]['analysis']['value_type'], self.identity)
            raw = json.dumps({'value': value}, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode() + b'\n'
            if len(raw) > 256 * 1024:
                raise RuntimeContractError('Native value exceeds encoded event budget')
            self.owner.record(self.identity, 'value', {'value': value})
            self.owner.values[self.identity] = value
            self.owner.progress((self.identity, self.identity, 0), calls=1)
        self.completed = True

    def checkpoint(self, checkpoint):
        if self.owner is not None:
            facts = json.loads(json.dumps(checkpoint.contract_arguments))
            self.owner.record(self.identity, 'checkpoint', facts)
            self.owner.values[self.identity] = facts
            self.owner.checkpoints[id(checkpoint)] = (checkpoint, self.identity, checkpoint.path)
        self.completed = True

    def checkpoint_read(self, source, reference):
        if self.owner is not None:
            self.owner.checkpoint_sources[id(source)] = (source, self.identity, source_facts(source))
            self.owner.record(self.identity, 'checkpoint_read', {'reference': reference})
            self.owner.values[self.identity] = {'reference': reference}
        self.completed = True

    def receipt(self, receipt):
        if self.owner is not None:
            self.owner.record(self.identity, 'receipt', receipt.to_dict())
            if receipt.status != 'committed':
                raise RuntimeContractError('Uncertain write cannot supply a downstream fixed read')
            self.owner.values[self.identity] = receipt.to_dict()
            self.owner.progress((self.identity, self.identity, 0), calls=1, rows_output=receipt.written_rows)
        self.completed = True

    def materialized(self, handle):
        if self.owner is not None:
            self.owner.handles[id(handle)] = self.identity
            self.owner.record(self.identity, 'materialized', {'rows': handle.row_count})
            self.owner.progress((self.identity, self.identity, 0), calls=1, rows_output=handle.row_count)
        self.completed = True

    def streamed(self, stats):
        if self.owner is not None:
            self.owner.record(self.identity, 'consumed', {'rows': stats.emitted})
            self.owner.progress((self.identity, self.identity, 0), calls=1, rows_output=stats.emitted)
        self.completed = True


def _validate_action_source(kind, value):
    if value.plan is not None:
        from demiflow.execution.native_queue.queue_producer import producer_actor, validate_source
        producer = producer_actor(value.plan)
        if producer is not None:
            from demiflow.execution.recovery_contract import RecoveryContractError
            if kind != 'run_stream':
                raise RecoveryContractError('Incremental enqueue requires run_stream; source capture and other terminals cannot publish tasks')
            validate_source(value.source, producer[1].producer.source)


@contextmanager
def action(kind, source, plan, spec=None, *, arguments=None):
    from contextlib import nullcontext
    from demiflow.execution.pipeline.run_context import execution_scope
    from demiflow.execution.recovery_identity import plan_identity
    from demiflow.execution.compiler_support.capabilities import check_action
    capability = check_action(kind, arguments)
    if not capability['supported']:
        from demiflow.execution.recovery_contract import RecoveryContractError
        raise RecoveryContractError(capability['reason'])
    from demiflow.execution.executors.plan_capture import current_capture
    capture = current_capture()
    if capture is not None:
        # Validate/build ordinary Dataset requests without allocating an implicit
        # run. The real request is intercepted at the single driver entry.
        from demiflow.execution.pipeline.run_context import payload_scope
        value = CheckedAction(None, None, source, plan)
        _validate_action_source(kind, value)
        with payload_scope(capture.context, plan, arguments=arguments, source=source,
                           physical_dataflow=True):
            yield value
        return
    owner = current()
    checks = (owner.action(kind, source, plan, spec, arguments) if owner is not None
              else nullcontext(CheckedAction(None, None, source, plan)))
    if kind == "from_items":
        # A literal Dataset declaration must not allocate a run or write state.
        with checks as value:
            yield value
        return
    from demiflow.execution.pipeline.run_context import payload_scope
    with execution_scope() as run, payload_scope(run, plan,
            owner_limits=owner._limits if owner is not None else None, arguments=arguments,source=source,
            physical_dataflow=True), checks as value:
        _validate_action_source(kind, value)
        declaration = plan_identity(kind, value.source, value.plan, spec, arguments)
        with run.action(declaration, action_id=value.identity) as execution:
            value.execution = execution
            try:
                yield value
            except GeneratorExit:
                # Closing a delivery iterator is a completed consumption boundary,
                # not proof that its full input ran. Inner adapters have drained
                # and preserved every handed-out artifact before this unwinds.
                if kind in {'iter_rows', 'iter_batches'} and execution.record['result'] is None:
                    execution.complete(dict(kind=kind, delivery='closed'))
                raise
            if execution.record["result"] is None:
                # Per-format durable adapters replace this delivery-only fact.
                # It is never proof that an arbitrary callback can be replayed.
                execution.complete(dict(kind=kind, delivery="completed"))
