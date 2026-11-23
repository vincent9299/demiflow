"""Native per-task terminal adapter over the existing Dataset stream driver."""
import dis
import inspect

from ..data.datasink import WriteResult
from ..data.plan import (AsyncMapOp,MapOp,BoundMapOp,SelectColumnsOp,
                         DropColumnsOp,RenameColumnsOp)
from ..data.sources import DatasourceSource
from .queue_source import QueueDatasource
from .queue_sinks import QueueResultSink
from .recovery_contract import RecoveryContractError,SourcePosition,identity
from .row_scope import current_row_scope


def queue_source(source):
    return source.datasource if type(source) is DatasourceSource and type(source.datasource) is QueueDatasource else None


def stateless_map_identity(spec):
    """Admit function-equivalent actors without constructing user resources.

    An ordinary Python callback is a declared pure transform, not a sandbox.
    Actor admission additionally proves that it cannot depend on instance state:
    object construction only, no inherited/custom descriptors or class data, and
    no method reads/captures its receiver. Mutable/configured/resource actors
    require a native adapter; snapshot_state alone cannot make a task stateless.
    """
    from .recovery_identity import describe
    target=spec.target
    if inspect.isfunction(target):return describe(spec)
    if inspect.ismethod(target):
        if target.__name__!='__call__':
            raise RecoveryContractError('Task actor must expose its row __call__')
        target=target.__self__
    cls=target if inspect.isclass(target) else type(target)
    if (type(cls) is not type or cls.__bases__!=(object,) or
            cls.__init__ is not object.__init__ or cls.__new__ is not object.__new__ or
            spec.constructor_args or spec.constructor_kwargs or
            (not inspect.isclass(target) and getattr(target,'__dict__',{}))):
        raise RecoveryContractError('Task actor has unknown initialization or mutable instance state')
    methods={}
    for name,method in vars(cls).items():
        if name in {'__module__','__doc__','__dict__','__weakref__','__qualname__','__firstlineno__','__static_attributes__'}:
            continue
        if name not in {'__call__','astart','astop','aclose'} or not inspect.isfunction(method):
            raise RecoveryContractError('Task actor has undeclared class state or resources')
        if not method.__code__.co_argcount:
            raise RecoveryContractError('Task actor method requires an ordinary receiver')
        receiver=method.__code__.co_varnames[0]
        if any(instruction.argval==receiver and instruction.opname in
               {'LOAD_FAST','LOAD_FAST_CHECK','LOAD_FAST_BORROW','LOAD_DEREF','LOAD_CLOSURE'}
               for instruction in dis.get_instructions(method)):
            raise RecoveryContractError('Task actor may not read or mutate instance state')
        if name=='__call__' and inspect.iscoroutinefunction(method):
            raise RecoveryContractError('Task synchronous map actor must return one row')
        methods[name]=describe(method)
    if '__call__' not in methods:
        raise RecoveryContractError('Task actor requires a stateless row __call__')
    # Native class identity alone omits method defaults/closures/source bytes.
    # Bind those too, retaining normal call/field/stream option identity below.
    return {'callable':describe(spec),'stateless_methods':methods}


def validate_topology(source,plan,sink,*,mode='sink',action=None):
    from .raw_fetch import RawFetch
    from .recovery_identity import describe
    if type(sink) is not QueueResultSink or source.settlement!=mode:
        raise RecoveryContractError('Automatic queue settlement requires exactly QueueResultSink and matching settlement mode')
    if source.config.declaration()!=sink.config.declaration() or source.pool!=sink.pool:
        raise RecoveryContractError('Queue source and result sink differ')
    declared=[getattr(getattr(getattr(op,'callable',None),'target',None),'__self__',
                      getattr(getattr(op,'callable',None),'target',None)) for op in plan.operations]
    stores=[actor.store for actor in declared if type(actor) is RawFetch]
    if len(stores)!=1:
        raise RecoveryContractError('Task result sink requires exactly one matching fetch receipt')
    store=stores[0]
    store_type=describe(type(store));store_declaration=store.recovery_identity()
    def operation_identity(value):
        # A pure callback may explicitly read the same native store. Bind its
        # logical store exactly as RawFetch does; the worker's physical root is
        # still validated/registered by RawNamespace, not a consumer semantic.
        # Other paths, other stores and arbitrary actor state remain unchanged.
        if type(value) is dict:
            if set(value)=={'native','declaration'} and value['native']==store_type and value['declaration']==store_declaration:
                return {'native':value['native'],'declaration':{k:v for k,v in store_declaration.items() if k!='directory'}}
            return {k:operation_identity(v) for k,v in value.items()}
        if type(value) is list:return [operation_identity(v) for v in value]
        return value
    fetch=[];operations=[]
    for index,op in enumerate(plan.operations):
        target=getattr(getattr(op,'callable',None),'target',None)
        actor=getattr(target,'__self__',target)
        if type(op) is AsyncMapOp and type(actor) is RawFetch:
            if op.catch or op.hard_timeout is not None:
                raise RecoveryContractError('Queue raw fetch cannot drop rows through catch/timeout')
            contract=actor.recovery_identity()
            # Exactly this native shared consumer identity describes a logical
            # store. Physical roots remain fenced in each worker resource record.
            contract['store']={k:v for k,v in contract['store'].items() if k!='directory'}
            operations.append(contract);fetch.append((index,actor))
        elif type(op) in {MapOp,BoundMapOp}:
            if op.stream_options is not None and op.stream_options.catch:
                raise RecoveryContractError('Task maps cannot drop rows through catch')
            pure=stateless_map_identity(op.callable)
            if type(op) is BoundMapOp:
                reserved={'_queue_delivery'}|({sink.result} if fetch else set())
                destinations={op.output} if op.output is not None else set((op.outputs or {}).values())
                if reserved.intersection(destinations):
                    raise RecoveryContractError('Task field map cannot replace its delivery or acquisition receipt')
            operations.append(operation_identity({'operation':describe(op),'pure_callable':pure}))
        elif type(op) in {SelectColumnsOp,DropColumnsOp,RenameColumnsOp}:
            reserved={'_queue_delivery'}|({sink.result} if fetch else set())
            if type(op) is SelectColumnsOp and not reserved.issubset(op.columns):
                raise RecoveryContractError('Task projection must preserve delivery and acquisition receipt')
            if type(op) is DropColumnsOp and reserved.intersection(op.columns):
                raise RecoveryContractError('Task projection cannot drop delivery or acquisition receipt')
            if type(op) is RenameColumnsOp:
                if type(op.names) is not dict or any(op.names.get(k,k)!=k or
                        any(dst==k and src!=k for src,dst in op.names.items()) for k in reserved):
                    raise RecoveryContractError('Task rename requires a mapping preserving delivery and acquisition receipt')
            operations.append(describe(op))
        else:
            raise RecoveryContractError('Task settlement supports pure one-to-one maps and exactly one raw fetch')
    if len(fetch)!=1 or fetch[0][1].output!=sink.result:
        raise RecoveryContractError('Task result sink requires exactly one matching fetch receipt')
    from .queue_contract import resolve_consumer_contract
    declaration=dict(protocol='demiflow.queue.consumer.v3',
                     operations=operations,result=sink.result,settlement=source.settlement)
    return fetch[0],resolve_consumer_contract(source,declaration,action=action)


def finalize_task(source,delivery,row):
    from .queue_commit import task_action
    from .commit import CommitCoordinator
    from .queue_contract import QueueIncomplete
    action=task_action(source.parent,source.channel,source.pool,delivery,'final','terminal',source.contract,stop_check=source._check)
    sink=source.sink.bind(action)
    coordinator=CommitCoordinator(action)
    facts=coordinator.recover({'result':sink})
    if facts and facts[0]['status']!='pending':
        fact=facts[0]
    else:
        if row is None:raise RecoveryContractError('Unprepared final task has no final row')
        if type(row) is not dict:
            raise RecoveryContractError('Task callbacks must retain exactly one record')
        display=dict(task_id=delivery.task_id,fingerprint=delivery.fingerprint,attempt=delivery.epoch)
        if row.get('_queue_delivery')!=display:
            raise RecoveryContractError('Task callback changed its native delivery display')
        fact=coordinator.commit(1,outputs={'result':(sink,row)},
            position=SourcePosition(identity([delivery.generation,delivery.task_id,delivery.fingerprint]),1))
    exhausted=fact['status']!='acked'
    source.confirmed(delivery,exhausted=exhausted)
    if exhausted:raise QueueIncomplete('Retryable acquisition exhausted its task budget; observation persisted without ACK')
    return fact


def configure_explicit(source,plan,action):
    from .queue_ack import QueueAcknowledge
    from ..data.plan import LogicalPlan
    if not plan.operations or action.declaration['kind']!='run_stream':
        raise RecoveryContractError('Explicit ACK is supported only at graph tail with run_stream')
    operation=plan.operations[-1]
    actor=getattr(getattr(operation.callable,'target',None),'__self__',operation.callable.target) if hasattr(operation,'callable') else None
    if type(operation) is not AsyncMapOp or type(actor) is not QueueAcknowledge or operation.catch or operation.hard_timeout is not None:
        raise RecoveryContractError('Explicit queue source requires exactly one final ack_queue node')
    sink=QueueResultSink(actor.config,pool=source.pool,result=actor.result)
    (index,fetch),contract=validate_topology(source,LogicalPlan(plan.operations[:-1]),sink,mode='explicit',action=action)
    source.configure(action,fetch,sink,contract,'node-'+str(index))
    actor.source=source
