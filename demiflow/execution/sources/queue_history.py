"""Explicit, bounded, read-only publication-prefix inspection."""
from contextlib import contextmanager
from dataclasses import asdict
import json
import time
from types import SimpleNamespace

from demiflow.data.datasource import Datasource, ReadTask, BlockMetadata
from demiflow.execution.native_queue.queue import QueueChannel
from demiflow.execution.queue_contract import queue_config, QueueSnapshot, HistoryReadLimits, HistoryReadLimit
from demiflow.execution.native_queue.queue_commit import QueueCommitStore
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity, identifier


class ReadOnlyQueueStore:
    def __init__(self,channel):self.channel=channel

    @contextmanager
    def _connection(self):
        with self.channel.database.connection() as db:yield db

    def execution_commit(self,commit_id):
        identifier(commit_id)
        with self._connection() as db:return QueueCommitStore._fact(db,commit_id)


def inspect_result(channel,snapshot,task_id,*,verify='artifacts',check_stop=lambda:None):
    """Inspect one exact final fact; this handle has no mutation or lease API."""
    observation=snapshot.kind=='observations'
    table='queue_observations' if observation else 'queue_completions'
    store=ReadOnlyQueueStore(channel)
    with store._connection() as db:
        record=db.execute('SELECT ordinal,proof FROM '+table+' WHERE generation=? AND task_id=?',(snapshot.generation,task_id)).fetchone()
        task=db.execute('SELECT fingerprint,status,artifact_domain,acquisition_scope,final_scope FROM queue_tasks WHERE generation=? AND task_id=?',(snapshot.generation,task_id)).fetchone()
        pool=db.execute('SELECT consumer_contract FROM queue_pools WHERE generation=?',(snapshot.generation,)).fetchone()
    if record is None or task is None or pool is None or record[0]>snapshot.cursor:
        raise UnresolvedCommit('Queue result is outside the fixed publication prefix')
    proof=json.loads(record[1])
    keys={'queue_uuid','generation','task_id','fingerprint','consumer_contract','acquisition_commit','final_commit',
          'receipt_sha256','artifact_domain','ordinal','result_ref','settled','status'}
    if (type(proof) is not dict or set(proof)!=keys or proof['queue_uuid']!=snapshot.queue_uuid or
            proof['generation']!=snapshot.generation or proof['task_id']!=task_id or proof['fingerprint']!=task[0] or
            proof['consumer_contract']!=pool[0] or proof['artifact_domain']!=task[2] or proof['ordinal']!=record[0] or
            type(proof['settled']) is not bool or proof['settled'] is observation or type(proof['ordinal']) is not int or
            proof['status'] not in ({'retryable'} if observation else {'ok','failed'}) or
            task[1] != ('exhausted' if observation else 'done_ok' if proof['status']=='ok' else 'done_failed')):
        raise UnresolvedCommit('Queue completion proof differs from its native task facts')
    fact=store.execution_commit(proof['final_commit']);acquisition=store.execution_commit(proof['acquisition_commit'])
    if (fact is None or acquisition is None or acquisition['status']!='acked' or acquisition['action_id']!=task[3] or
            fact['action_id']!=task[4] or fact['status']!=('checkpoint_committed' if observation else 'acked') or
            set(fact['receipts'])!={'result'} or set(acquisition['receipts'])!={'raw'} or fact['prepared'] is None or
            fact['prepared']['state'] or fact['prepared'].get('raw_acquisitions') or
            proof['receipt_sha256']!=identity(fact['receipts']['result'])):
        raise UnresolvedCommit('Queue result has inconsistent native commit evidence')
    with store._connection() as db:
        for current,phase in ((fact,'final'),(acquisition,'acquisition')):
            scope=db.execute('SELECT parent_action_id,declaration FROM runtime_commit_scopes WHERE scope_id=?',(current['action_id'],)).fetchone()
            declaration=json.loads(scope[1]) if scope is not None else None
            if (declaration is None or scope[0]!='task-'+identity([snapshot.generation,task_id]) or
                    identity(declaration)!=current['action_id'] or
                    current['commit_id']!=identity([identity([snapshot.queue_uuid,snapshot.generation]),current['action_id'],1]) or
                    declaration.get('protocol')!='demiflow.queue.task.v2' or declaration.get('phase')!=phase or
                    declaration.get('queue_uuid')!=snapshot.queue_uuid or declaration.get('generation')!=snapshot.generation or
                    declaration.get('task_id')!=task_id or declaration.get('fingerprint')!=task[0] or
                    identity(declaration.get('consumer'))!=pool[0]):
                raise UnresolvedCommit('Completion native scope identity differs')
    prepared=fact['prepared']['sinks']['result']
    if proof['result_ref']!={'commit_id':fact['commit_id'],'sha256':prepared['row_sha256']}:
        raise UnresolvedCommit('Queue result reference changed')
    result=prepared['row'][prepared['result_field']]
    if result!=acquisition['prepared']['sinks']['raw']['result'] or result['status']!=proof['status']:
        raise UnresolvedCommit('Queue result changed its acquisition receipt')
    context=SimpleNamespace(journal=store,ref=SimpleNamespace(run_id=identity([snapshot.queue_uuid,snapshot.generation])),
        attempt=1,fence='read-only-inspection',directory=None,check_stop=check_stop,
        limits={'max_output_bytes':channel.config.limits.max_object_bytes,'max_row_bytes':channel.config.limits.max_result_bytes})
    # QueueResultSink's read-only inspection checks SQL row equality and the
    # exact RawObjectSink receipts; it cannot claim, prepare, repair or finalize.
    from demiflow.execution.native_queue.queue_sinks import QueueResultSink
    action=SimpleNamespace(context=context,action_id=fact['action_id'])
    store.delivery=SimpleNamespace(generation=snapshot.generation,task_id=task_id)
    sink=QueueResultSink(channel.config,pool=snapshot.pool,result=prepared['result_field']).bind(action)
    sink._validate(prepared)
    if verify=='artifacts':
        from demiflow.execution.checkpoint.commit import CommitCoordinator
        if observation:
            from demiflow.data.datasink import WriteContext
            receipt=sink.inspect(prepared,WriteContext(run_id=context.ref.run_id,action_id=fact['action_id'],
                commit_id=fact['commit_id'],attempt=1,fence=context.fence))
            if receipt!=fact['receipts']['result']:raise UnresolvedCommit('Observation result receipt changed')
        else:CommitCoordinator(action).inspect_completed(fact,{'result':sink})
    else:
        with store._connection() as db:
            row=db.execute('SELECT commit_id,row_document FROM queue_results WHERE generation=? AND task_id=?',
                           (snapshot.generation,task_id)).fetchone()
        if row is None or row[0]!=fact['commit_id'] or json.loads(row[1])!=prepared['row']:
            raise UnresolvedCommit('Historical queue result changed')
    return prepared['row'],proof,acquisition['prepared']['sinks']['raw']['object']


class QueueHistoryDatasource(Datasource):
    def __init__(self,config,*,pool,through,results=False,verify='artifacts',limits=None,after=None):
        self.config=queue_config(config);self.pool=identifier(pool)
        if type(through) is not QueueSnapshot or through.pool!=pool:
            raise ValueError('History requires its matching QueueSnapshot')
        if type(results) is not bool or results!=(through.kind in {'results','observations'}):
            raise ValueError('History results flag differs from snapshot kind')
        if verify not in {'artifacts','metadata'}:raise ValueError('Unknown history verification mode')
        if limits is None:limits=HistoryReadLimits()
        if type(limits) is not HistoryReadLimits:raise ValueError('History limits require HistoryReadLimits')
        self.through,self.results,self.verify,self.limits=through,results,verify,limits
        self.start=1
        if after is not None:
            if (type(after) is not dict or set(after)!={'snapshot','next_ordinal'} or after['snapshot']!=asdict(through) or
                    type(after['next_ordinal']) is not int or not 1<=after['next_ordinal']<=through.cursor+1):
                raise ValueError('History cursor differs from the fixed snapshot')
            self.start=after['next_ordinal']

    def declaration(self):
        return dict(contract='demiflow.queue.history.v2',config=self.config.declaration(),pool=self.pool,
            through=asdict(self.through),results=self.results,verify=self.verify,limits=asdict(self.limits),start=self.start)

    def recovery_identity(self):return self.declaration()
    def snapshot_identity(self):return self.declaration()

    def get_read_tasks(self,parallelism,per_task_row_limit=None,data_context=None):
        return [ReadTask(self.rows,BlockMetadata(),source_id=identity(self.declaration()),partition_id='history')]

    def rows(self):
        channel=QueueChannel(self.config);channel._attach();snapshot=self.through;limits=self.limits
        if channel.database.queue_uuid!=snapshot.queue_uuid:raise RecoveryContractError('History belongs to another physical queue')
        actual=channel.snapshot(self.pool,kind=snapshot.kind)
        if actual.generation!=snapshot.generation or actual.cursor<snapshot.cursor:
            raise RecoveryContractError('History snapshot differs from current queue generation')
        from demiflow.execution.pipeline.run_context import current_run
        run=current_run();started=time.monotonic();position=self.start;count=total=hashed=resident=0
        resident_limit=min(limits.max_total_record_bytes*8,run.limits['memory_bytes']//2 if run else 128*1024**2)
        def cursor():return {'snapshot':asdict(snapshot),'next_ordinal':position}
        def check():
            if run is not None:run.check_stop()
            if time.monotonic()-started>limits.timeout_s:raise HistoryReadLimit('History time budget exhausted',cursor())
        table={'tasks':'queue_tasks','results':'queue_completions','observations':'queue_observations'}[snapshot.kind]
        while position<=snapshot.cursor:
            check()
            with channel.database.connection() as db:
                records=db.execute('SELECT ordinal,task_id FROM '+table+' WHERE generation=? AND ordinal>=? AND ordinal<=? ORDER BY ordinal LIMIT ?',
                    (snapshot.generation,position,snapshot.cursor,limits.page_rows)).fetchall()
            if not records:raise UnresolvedCommit('Queue publication prefix has a missing record')
            page_bytes=0
            for ordinal,task_id in records:
                check()
                if ordinal!=position:raise UnresolvedCommit('Queue publication prefix has an ordinal gap')
                if count>=limits.max_rows:raise HistoryReadLimit('History row budget exhausted',cursor())
                if self.results:
                    row,proof,obj=inspect_result(channel,snapshot,task_id,verify='metadata',check_stop=check)
                    if any(key in row for key in ('queue_record','verification')):
                        raise RecoveryContractError('History metadata overlaps a stored business column')
                    output={**row,'queue_record':dict(task_id=task_id,ordinal=ordinal,kind=snapshot.kind,proof=proof),
                            'verification':{'status':'verified' if self.verify=='artifacts' else 'not_checked'}}
                else:
                    with channel.database.connection() as db:
                        task=db.execute('SELECT fingerprint,payload,occurrence,producer_commit FROM queue_tasks WHERE generation=? AND task_id=?',(snapshot.generation,task_id)).fetchone()
                        fact=QueueCommitStore._fact(db,task[3])
                    prepared=fact['prepared']['sinks']['publish'] if fact and fact['prepared'] else None
                    receipt=fact['receipts'].get('publish') if fact else None
                    if prepared is None or receipt is None or receipt['data']['chunk_sha256']!=identity(prepared):
                        raise UnresolvedCommit('Historical task has no exact native publication')
                    matching=[(i,m) for i,m in enumerate(prepared['messages']) if m['task_id']==task_id]
                    if len(matching)!=1:raise UnresolvedCommit('Historical task differs from native publication')
                    index,message=matching[0]
                    if task[1]!=document(message['payload'],self.config.limits.max_payload_bytes).decode() or task[2]!=identity([prepared['occurrence'],index]):
                        raise UnresolvedCommit('Historical task payload or occurrence changed')
                    output=dict(task_id=task_id,pool=self.pool,payload=message['payload'],fingerprint=task[0],ordinal=ordinal,
                                verification={'status':'verified' if self.verify=='artifacts' else 'not_checked'})
                try:raw=document(output,limits.max_record_bytes)
                except RecoveryContractError:
                    raise HistoryReadLimit('History record exceeds its byte budget',cursor()) from None
                size=len(raw)
                if total+size>limits.max_total_record_bytes:
                    raise HistoryReadLimit('History total record bytes exhausted',cursor())
                if size>limits.page_bytes:raise HistoryReadLimit('History record exceeds page byte budget',cursor())
                if page_bytes+size>limits.page_bytes:
                    # Drop only the bounded metadata page and resume from the
                    # same unconsumed ordinal in the next SQL page.
                    break
                from demiflow.data.runtime_contract import check_size
                try:measured=check_size(output,resident_limit)
                except TypeError:
                    raise HistoryReadLimit('History resident record budget exhausted',cursor()) from None
                if resident+measured>resident_limit:
                    raise HistoryReadLimit('History resident record budget exhausted',cursor())
                if self.results and self.verify=='artifacts':
                    amount=0 if obj is None else obj['bytes']
                    if amount>limits.max_hash_bytes-hashed:
                        raise HistoryReadLimit('History artifact hash budget exhausted before reading object',cursor())
                    inspect_result(channel,snapshot,task_id,verify='artifacts',check_stop=check)
                    hashed+=amount
                page_bytes+=size;total+=size;resident+=measured;count+=1;position+=1
                yield (output,)
