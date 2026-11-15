"""Native search operations hosted by the existing driver I/O authority."""
from copy import copy
from dataclasses import asdict

from .contracts.dataflow import QueueBinding, QueueReply
from .contracts.dataflow_errors import DataflowError
from .dataflow_acquisition_call import AcquisitionCall
from .dataflow_search_runtime import SearchRuntime
from .recovery_contract import identity
from .row_scope import RowScope


class NativeSearchCall(AcquisitionCall):
    def __init__(self, declaration_key):
        super().__init__('search')
        self.declaration_key = declaration_key

    def bind(self, services, occurrence_id):
        self.services = services
        self.binding = QueueBinding(**dict(services.journal.read_configuration()['queue_binding']))
        if self.binding.mode not in ('producer', 'raw_fetch', 'native_search'):
            raise DataflowError('FENCED', 'Search worker is not bound to a native search authority')
        self.search_occurrence = occurrence_id


def _search_actors(source, plan, bounds):
    from ..collect.operators import SearchWeb
    from ..planning.dataflow_compile import expand_relational_inputs, expand_operation_inputs
    pending, seen, actors = [(source, plan)], set(), {}
    while pending:
        source, plan = pending.pop()
        key = (id(source), id(plan))
        if key in seen:
            continue
        seen.add(key)
        if len(seen) > bounds.max_nodes:
            raise DataflowError('BUDGET_EXCEEDED', 'Search declaration traversal exceeds graph bound')
        for operation in plan.operations:
            target = getattr(getattr(operation, 'callable', None), 'target', None)
            actor = getattr(target, '__self__', target)
            if type(actor) is SearchWeb:
                if actor.io is not None or actor._session_lease is not None:
                    raise DataflowError('INVALID_CONTRACT', 'Native search requires an unstarted actor')
                actors[identity(actor.recovery_identity())] = actor
            pending.extend((binding.source, binding.plan) for binding in
                expand_operation_inputs(operation, path=(), bounds=bounds))
        pending.extend((binding.source, binding.plan) for binding in
            expand_relational_inputs(source, path=(), bounds=bounds))
    return actors


class NativeSearchOperations:
    """Fixed search RPC and lifecycle, shared by fetch/producer/search services."""

    def configure_searches(self, source, plan, graph):
        actors = _search_actors(source, plan, self._services.bounds)
        self._searches = {}
        for node in graph.nodes:
            if node.kernel.configuration.get('native_operation') != 'search':
                continue
            key = node.kernel.configuration['native_key']
            if key not in actors:
                raise DataflowError('INVALID_CONTRACT', 'Search occurrence has no matching native declaration')
            self._searches[node.occurrence_id] = copy(actors[key])
        if not self._searches:
            raise DataflowError('INVALID_CONTRACT', 'Search authority requires a declared search occurrence')
        self._search_runtime = SearchRuntime(self._action, self._services.bounds.max_nodes)
        if self._binding is None:
            self._mode = 'native_search'
            self._binding = QueueBinding(self.action_id, None, self._mode,
                identity(sorted(self._searches)), None, None, None, next(iter(self._searches)), None)
            key = self.action_id + '-search-binding'
            self._prior_search_binding = self._action.context.journal.source_binding(key)
            self._action.context.journal.bind_source(self._action.context.fence, key, asdict(self._binding))
            self._state = 'configured'
        self._max_inflight = max(self._max_inflight, min(len(self._searches) * 4,
                                                        self._services.bounds.max_pending_events))

    def search(self, command, *, lease):
        self._check_started()
        self._check_lease(lease)
        self._validate_command(command, 'search', require_refs=True)
        self._validate_binding(command.binding)
        if (command.node_id not in self._searches or command.node_id not in
                self._worker_occurrences.get((lease.allocation_id, lease.boot_id), ())):
            raise DataflowError('FENCED', 'Worker does not own this native search occurrence')
        return self._run_operation(command, lambda: self._search_row(command))

    def _search_row(self, command):
        actor = self._searches[command.node_id]
        descriptor = self._read_json(command.delivery_ref, max_bytes=self._services.bounds.max_control_bytes)
        if self._binding.mode == 'producer':
            if self._unit is None or descriptor != self._unit:
                raise DataflowError('FENCED', 'Search does not belong to the active source unit')
        elif descriptor != {'occurrence': command.record_id}:
            raise DataflowError('FENCED', 'Native search invocation identity changed')
        row = self._read_json(command.row_ref, max_bytes=self._services.bounds.max_row_bytes)
        self._admit_io()
        try:
            result = self._run_io(self._search_runtime.call(actor, command, row),
                                  timeout_s=self._services.bounds.timeout_ms / 1000)
        finally:
            self._release_io()
        def facts():
            action = actor._action(RowScope(command.record_id))
            return action.context.journal.execution_commits(action.action_id)
        proofs = self._run_io(actor.io.run(facts), timeout_s=self._services.bounds.timeout_ms / 1000)
        return self._confirmed(command, result, proofs)

    def _confirmed(self, command, result, proofs):
        if not proofs or any(proof is None or proof['status'] != 'acked' for proof in proofs):
            raise DataflowError('UNKNOWN_EFFECT', 'Native effect has no confirmed receipt')
        ref = self._put_json(result, media_type='application/json', max_bytes=self._services.bounds.max_row_bytes)
        proofs = [{key: proof[key] for key in ('commit_id', 'action_id', 'status', 'receipts')}
                  for proof in proofs]
        fact = self._put_json({'commits': proofs}, media_type='application/json',
                             max_bytes=self._services.bounds.max_control_bytes)
        return QueueReply(command.operation_id, 'confirmed', command.delivery_ref, ref, fact, 0)

    def search_metrics(self):
        sessions = {id(actor.session): actor.session for actor in getattr(self, '_searches', {}).values()}
        return {'WebSession:' + str(index): session.snapshot_metrics()
                for index, session in enumerate(sessions.values())}
