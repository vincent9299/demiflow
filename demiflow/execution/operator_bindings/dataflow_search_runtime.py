"""Driver-owned search effects and ordered native WebSession execution."""
import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar

from demiflow.services.blocking_io import BlockingIOPool
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.native_queue.raw_budget import budget_delta, record_admission
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, SourcePosition, UnresolvedCommit, identity
from demiflow.execution.worker.row_scope import RowScope, current_row_scope, set_row_scope
from demiflow.execution.pipeline.run_context import RunAction, action_scope


# Shared WebSessions capture one HTTP admission callback. Each request task
# inherits the actual row's exchange, including when two nodes share a session.
_CURRENT_EXCHANGE = ContextVar('demiflow_native_search_exchange', default=None)


class SearchEffects:
    """One search node bound by its execution owner, outside the sealed actor."""

    def __init__(self, parent, node_id, declaration, wire_bytes, producer_binding):
        self._parent = parent
        self._node_id = node_id
        self._declaration = declaration
        self._wire_bytes = wire_bytes
        self._producer_binding = producer_binding
        self._io = BlockingIOPool(1, name='demiflow-search-facts')
        self.control_bytes = parent.context.limits['max_control_bytes']

    @classmethod
    def for_actor(cls, parent, actor, node_id, *, producer=None):
        if parent is None:
            raise RuntimeError('Search has no native action')
        producer_binding = (producer, producer.search_nodes[actor]) if producer is not None else None
        return cls(parent, node_id, actor.recovery_identity(), actor.session.search_config.max_bytes,
                   producer_binding)

    def bind_node(self, run, node_id):
        if run is not self._parent.context:
            raise RecoveryContractError('Search node bound to another native action')
        self._node_id = node_id

    def _action(self, occurrence):
        if self._producer_binding is not None:
            from demiflow.execution.native_queue.queue_commit import producer_action
            producer, index = self._producer_binding
            key = identity(['search', index, occurrence])
            return producer_action(self._parent, producer.channel, producer.producer,
                                   key, producer.contract, phase='source')
        run = self._parent.context
        declaration = dict(kind='native_search_response', node=self._node_id, occurrence=occurrence,
                           contract=self._declaration)
        key = identity([self._parent.action_id, self._node_id, occurrence, 'search'])
        run.journal.bind_commit_scope(run.fence, key, self._parent.action_id, declaration)
        return RunAction(run, key, declaration)

    @asynccontextmanager
    async def exchange(self):
        scope = current_row_scope()
        if scope is None:
            raise RecoveryContractError('Search lost native source occurrence')
        def begin():
            producer = self._producer_binding[0] if self._producer_binding is not None else None
            return _SearchExchange(self._action(scope.occurrence), self._io, self._wire_bytes, producer)
        exchange = await self._io.run(begin)
        token = _CURRENT_EXCHANGE.set(exchange)
        try:
            yield exchange
        finally:
            _CURRENT_EXCHANGE.reset(token)

    @staticmethod
    async def admit_http():
        exchange = _CURRENT_EXCHANGE.get()
        if exchange is None:
            raise UnresolvedCommit('Prior search exchange has no fixed native response; inspect its original request facts')
        await exchange.admit_http()

    async def apply(self, actor, row, *, include_proof_reader=False):
        actor.validate_row(row)
        async with self.exchange() as exchange:
            result = exchange.response
            if result is None:
                output = await actor.search_row(row, exchange.max_bytes)
                result = await exchange.finish(output)
            if include_proof_reader:
                # The RPC reads this exact exchange after caller-side I/O release.
                # A later call on the same actor cannot replace its receipt scope.
                return result, exchange.proofs
            return result

    async def close(self):
        await self._io.aclose()


class _SearchExchange:
    """A single original source exchange. Network work runs outside the journal transaction."""

    def __init__(self, action, io, wire_bytes, producer):
        from demiflow.collect.search_contract import NativeSearchResultSink
        self._io = io
        self._wire_bytes = wire_bytes
        self._producer = producer
        self._action = action
        self._sink = NativeSearchResultSink(action)
        self._coordinator = CommitCoordinator(action)
        self.max_bytes = self._sink.maximum
        facts = self._coordinator.recover({'search': self._sink})
        self.response = None
        self._permitted = False
        if facts and facts[0]['status'] != 'pending':
            self.response = facts[0]['prepared']['sinks']['search']['row']
            return
        self._position = SourcePosition(action.semantic_id, 1)
        run = action.context
        run.journal.begin_commit(run.fence, action.action_id, 1,
                                 dict(position=self._position.to_dict(), sinks=['search'], states=[], requires_ack=False))
        runtime = getattr(run.journal, 'local_run', run)
        owner = int(identity([run.fence, runtime.fence])[:15], 16)
        with run.journal.execution_guard(run.fence) as db:
            old = db.execute("SELECT amount FROM runtime_reservations WHERE name='search_exchange_owner' "
                             "AND reservation_id=?", (action.action_id,)).fetchone()
            if old is None:
                channel = getattr(run.journal, 'channel', None)
                record_admission(db, channel.config.limits.max_reservations if channel else 100000)
                budget_delta(db, 'control_bytes', 128, run.limits['max_control_bytes'])
                db.execute("INSERT INTO runtime_reservations VALUES ('search_exchange_owner',?,?)",
                           (action.action_id, owner))
            self._permitted = old is None or old[0] == owner

    async def admit_http(self):
        if not self._permitted:
            raise UnresolvedCommit('Prior search exchange has no fixed native response; inspect its original request facts')
        await self._io.run(self._charge_http)

    def _charge_http(self):
        run = self._action.context
        limits = run.limits
        with run.journal.execution_guard(run.fence) as db:
            budget_delta(db, 'requests', 1, limits['max_requests'])
            budget_delta(db, 'raw_wire_bytes', self._wire_bytes,
                         limits['max_output_bytes'] * 8 + limits['max_requests'] * 4096)

    async def proofs(self):
        def read():
            run = self._action.context
            facts = run.journal.execution_commits(self._action.action_id)
            # Confirm ownership before returning the immutable receipts. Keep the
            # read outside the guard so a bounded DB pool needs only one connection.
            with run.journal.execution_guard(run.fence):
                return facts
        return await self._io.run(read)

    async def finish(self, output_row):
        fact = await self._io.run(self._coordinator.commit, 1,
                                  outputs={'search': (self._sink, output_row)}, position=self._position)
        if self._producer is not None:
            self._producer.window_commits += 1
        return fact['prepared']['sinks']['search']['row']


class SearchRuntime:
    def __init__(self, action, maximum, effects):
        self.action = action
        self.maximum = maximum
        self._effects = effects
        self.started = []
        self.queue = None
        self.task = None

    async def start(self):
        self.queue = asyncio.Queue(maxsize=self.maximum)
        self.task = asyncio.create_task(self._serve(), name='demiflow-native-search-owner')

    async def call(self, actor, command, row):
        result = asyncio.get_running_loop().create_future()
        await self.queue.put((actor, command, row, result))
        return await asyncio.shield(result)

    async def _serve(self):
        with action_scope(self.action):
            while True:
                actor, command, row, reply = await self.queue.get()
                if actor is None:
                    errors = []
                    for owner in reversed(self.started):
                        try:
                            await owner.aclose()
                        except BaseException as exc:
                            errors.append(exc)
                    self.started.clear()
                    if errors:
                        reply.set_exception(errors[0])
                    else:
                        reply.set_result(None)
                    return
                set_row_scope(RowScope(command.record_id))
                try:
                    effects = self._effects[command.node_id]
                    if actor not in self.started:
                        self.started.append(actor)
                        actor.bind_search_effects(effects)
                        await actor.astart()
                    reply.set_result(await effects.apply(actor, row, include_proof_reader=True))
                except BaseException as exc:
                    reply.set_exception(exc)
                finally:
                    set_row_scope(None)

    async def close(self):
        if self.task is None:
            return
        try:
            await self.call(None, None, None)
        finally:
            await self.task
            self.task = None


def prepare_search_bindings(actors):
    """Select actual search owners once; bind each at its original start boundary."""
    from functools import partial
    from demiflow.collect.search_contract import is_native_search_actor
    from demiflow.execution.native_queue.queue_producer import QueueProducer
    from demiflow.execution.pipeline.run_context import current_action
    searches = {id(actor) for actor in actors if is_native_search_actor(actor)}
    if not searches:
        return {}
    producer = next((actor for actor in actors if type(actor) is QueueProducer), None)
    def bind(actor):
        effects = SearchEffects.for_actor(current_action(), actor, actor.node_id, producer=producer)
        actor.bind_search_effects(effects)
    return {id(actor): partial(bind, actor) for actor in actors if id(actor) in searches}
