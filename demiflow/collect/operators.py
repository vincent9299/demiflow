"""Native bounded search and durable source response facts."""
import asyncio
from contextvars import ContextVar

_SEARCH_NATIVE = ContextVar("demiflow_native_search_action", default=None)
from .session import bounded
from .connection_manager import WebOperatorLifecycle


class SearchWeb(WebOperatorLifecycle):
    concurrency = 1
    def __init__(self, requests, output, session, when, max_candidates, request_concurrency,
                 checkpoint=None, checkpoint_operator='search_web', checkpoint_lease_s=300.0):
        if checkpoint is not None or checkpoint_operator!='search_web' or checkpoint_lease_s!=300.0:
            from ..execution.recovery_contract import RecoveryContractError
            raise RecoveryContractError('Explicit search checkpoint orchestration is retired')
        if not 1<=max_candidates<=500 or not 1<=request_concurrency<=4:
            raise ValueError('Native search requires <=500 candidates and <=4 concurrent requests')
        self.requests, self.output, self.session, self.when = requests, output, session, when
        self.max_candidates, self.request_concurrency = max_candidates, request_concurrency
        self.resources = (session,)
        self.queue_producer=None
        self.run=self.node_id=self.io=None

    def recovery_identity(self):
        from .search_contract import session_declaration
        from ..execution.recovery_identity import describe
        return dict(contract='demiflow.collect.search_web.v1',requests=self.requests,output=self.output,
            max_candidates=self.max_candidates,request_concurrency=self.request_concurrency,
            when=describe(self.when),session=session_declaration(self.session))

    def bind_execution(self,run,node_id):
        self.run,self.node_id=run,node_id

    def snapshot_state(self):return {}
    def restore_state(self,state):
        if state!={}:raise ValueError('Search progress belongs to native response facts')

    async def astart(self):
        from ..execution.run_context import current_action
        from ..execution.blocking_io import BlockingIOPool
        self.parent=current_action()
        if self.parent is None:raise RuntimeError('Search has no native action')
        if self.run is None:self.run=self.parent.context
        self.io=BlockingIOPool(1,name='demiflow-search-facts')
        self.session._native_execution_admission=self._admit_http
        self.session._native_control_bytes=self.run.limits['max_control_bytes']
        self.session._native_max_hosts=64
        await super().astart()

    async def aclose(self):
        try:await super().aclose()
        finally:
            if self.io is not None:await self.io.aclose()
            self.io=None
            self.session._native_execution_admission=None
            self.session._native_control_bytes=None
            self.session._native_max_hosts=None

    def _claim_exchange(self,action):
        from ..execution.recovery_contract import identity,RecoveryContractError
        runtime=getattr(action.context.journal,'local_run',action.context)
        owner=int(identity([action.context.fence,runtime.fence])[:15],16)
        with action.context.journal.execution_guard(action.context.fence) as db:
            old=db.execute("SELECT amount FROM runtime_reservations WHERE name='search_exchange_owner' AND reservation_id=?",(action.action_id,)).fetchone()
            if old is not None:return old[0]==owner
            from ..execution.raw_budget import record_admission
            channel=getattr(action.context.journal,'channel',None)
            record_admission(db,channel.config.limits.max_reservations if channel else 100000)
            from ..execution.raw_budget import budget_delta
            budget_delta(db,'control_bytes',128,action.context.limits['max_control_bytes'])
            db.execute("INSERT INTO runtime_reservations VALUES ('search_exchange_owner',?,?)",(action.action_id,owner))
            return True

    async def _admit_http(self):
        from ..execution.recovery_contract import UnresolvedCommit
        action,allow=_SEARCH_NATIVE.get()
        if not allow:raise UnresolvedCommit('Prior search exchange has no fixed native response; inspect its original request facts')
        await self.io.run(self._charge_http,action)

    def _charge_http(self,action):
        from ..execution.raw_budget import budget_delta
        limits=action.context.limits
        with action.context.journal.execution_guard(action.context.fence) as db:
            budget_delta(db,'requests',1,limits['max_requests'])
            budget_delta(db,'raw_wire_bytes',self.session.search_config.max_bytes,limits['max_output_bytes']*8+limits['max_requests']*4096)

    def _action(self,scope):
        from ..execution.recovery_contract import identity
        from ..execution.run_context import RunAction
        if self.queue_producer is not None:
            from ..execution.queue_commit import producer_action
            p=self.queue_producer
            occurrence=identity(['search',self.queue_node_id,scope.occurrence])
            return producer_action(self.parent,p.channel,p.producer,occurrence,p.contract,phase='source')
        declaration=dict(kind='native_search_response',node=self.node_id,occurrence=scope.occurrence,
                         contract=self.recovery_identity())
        key=identity([self.parent.action_id,self.node_id,scope.occurrence,'search'])
        self.run.journal.bind_commit_scope(self.run.fence,key,self.parent.action_id,declaration)
        return RunAction(self.run,key,declaration)

    async def __call__(self, row):
        from ..execution.row_scope import current_row_scope
        from ..execution.recovery_contract import RecoveryContractError,SourcePosition,document
        from ..execution.commit import CommitCoordinator
        from .search_contract import NativeSearchResultSink
        if type(row) is not dict or self.output in row:
            raise RecoveryContractError('Search requires a record and an unused output field')
        scope=current_row_scope()
        if scope is None:raise RecoveryContractError('Search lost native source occurrence')
        action=await self.io.run(self._action,scope)
        sink=NativeSearchResultSink(action);coordinator=CommitCoordinator(action)
        facts=await self.io.run(coordinator.recover,{'search':sink})
        if facts and facts[0]['status']!='pending':
            return facts[0]['prepared']['sinks']['search']['row']
        position=SourcePosition(action.semantic_id,1)
        await self.io.run(action.context.journal.begin_commit,action.context.fence,action.action_id,1,
            dict(position=position.to_dict(),sinks=['search'],states=[],requires_ack=False))
        allow=await self.io.run(self._claim_exchange,action)
        values=row[self.requests]
        if type(values) is not list or len(values)>32:
            raise RecoveryContractError('Search request list exceeds 32 requests')
        document(values,min(65536,sink.maximum))
        for value in values:
            if type(value) is not dict or type(value.get('query')) is not str or not 1<=len(value['query'])<=4096:
                raise RecoveryContractError('Search requires bounded query records')
        if self.when is not None and not self.when(row):
            output=row
        else:
            async def one(request):
                parameters = {key: request[key] for key in ('language','pageno','safesearch','time_range',
                    'engines','categories','engine_data','network','fallback_parameters') if key in request}
                token=_SEARCH_NATIVE.set((action,allow))
                try:result=await self.session.search(request['query'],**parameters)
                finally:_SEARCH_NATIVE.reset(token)
                if result.get('status')=='interrupted' or any(r.get('status')=='interrupted' for r in result.get('engine_receipts',[])):
                    from ..execution.recovery_contract import UnresolvedCommit
                    raise UnresolvedCommit('Search request outcome is unresolved')
                return {**request,**result,'candidates':result.get('candidates',[])[:self.max_candidates]}
            results=await bounded(values,one,self.request_concurrency,return_exceptions=True)
            errors=[value for value in results if isinstance(value,BaseException)]
            if errors:raise errors[0]
            output={**row,self.output:results}
        fact=await self.io.run(coordinator.commit,1,outputs={'search':(sink,output)},
                              position=SourcePosition(action.semantic_id,1))
        if self.queue_producer is not None:self.queue_producer.window_commits+=1
        return fact['prepared']['sinks']['search']['row']
