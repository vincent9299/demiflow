"""Exact SQL participants of the native commit coordinator."""
import json

from ..data.datasink import Datasink
from ..data.runtime_contract import check_size
from .queue_contract import queue_config
from .queue import QueueChannel
from .recovery_contract import RecoveryContractError, UnresolvedCommit, document, identity


class QueueResultSink(Datasink):
    sink_id='result'

    def __init__(self,config,*,pool='default',result='result'):
        from .recovery_contract import identifier
        self.config=queue_config(config)
        self.pool,self.result=identifier(pool),identifier(result)
        self.action=None

    def recovery_identity(self):
        return dict(contract='demiflow.queue.result.v2',config=self.config.declaration(),pool=self.pool,result=self.result)

    def bind(self,action):
        target=QueueResultSink(self.config,pool=self.pool,result=self.result)
        target.action=action
        return target

    def write(self,blocks,context):
        raise RecoveryContractError('Queue result sink requires native task commit')

    def prepare(self,blocks,context):
        if self.action is None or type(blocks) is not dict:
            raise RecoveryContractError('Queue task sink requires exactly one final row')
        check_size(blocks,self.action.context.limits['max_row_bytes'])
        row={k:v for k,v in blocks.items() if k!='_queue_delivery'}
        raw=document(row,self.config.limits.max_result_bytes)
        result=row.get(self.result)
        if type(result) is not dict or type(result.get('acquisition_ref')) is not dict:
            raise RecoveryContractError('Final row lost its acquisition receipt')
        fact=self.action.context.journal.execution_commit(result['acquisition_ref']['commit_id'])
        if fact is None or fact['status']!='acked':
            raise RecoveryContractError('Final result has no confirmed acquisition')
        if (fact['prepared']['sinks']['raw']['result']!=result or
                fact['action_id']!=result['acquisition_ref']['scope_id']):
            raise RecoveryContractError('Final row changed its acquisition evidence')
        return dict(format=2,row=row,row_sha256=identity(row),row_bytes=len(raw),result_field=self.result,
                    acquisition_ref=dict(result['acquisition_ref']))

    def _validate(self,prepared):
        if type(prepared) is not dict or set(prepared)!={'format','row','row_sha256','row_bytes','result_field','acquisition_ref'} or prepared['format']!=2:
            raise RecoveryContractError('Invalid prepared task result')
        raw=document(prepared['row'],self.config.limits.max_result_bytes)
        if prepared['row_bytes']!=len(raw) or prepared['row_sha256']!=identity(prepared['row']) or prepared['result_field']!=self.result:
            raise RecoveryContractError('Prepared final row changed')

    def _receipt(self,prepared,context):
        return dict(commit_id=context.commit_id,sink_id=self.sink_id,status='committed',
                    data=dict(row_sha256=prepared['row_sha256'],row_bytes=prepared['row_bytes'],result_ref=context.commit_id))

    def inspect_in_transaction(self,db,prepared,context):
        self._validate(prepared)
        d=self.action.context.journal.delivery
        row=db.execute('SELECT commit_id,row_document FROM queue_results WHERE generation=? AND task_id=?',(d.generation,d.task_id)).fetchone()
        if row is None:return None
        if row[0]!=context.commit_id or json.loads(row[1])!=prepared['row']:
            raise UnresolvedCommit('Task result differs from its native prepared row')
        return self._receipt(prepared,context)

    def publish_in_transaction(self,db,prepared,context):
        receipt=self.inspect_in_transaction(db,prepared,context)
        if receipt is not None:return receipt
        d=self.action.context.journal.delivery
        from .raw_budget import budget_delta
        limits=self.action.context.limits
        for name in ('output_admission_bytes','output_bytes','row_bytes'):
            budget_delta(db,name,prepared['row_bytes'],limits['max_output_bytes'])
        budget_delta(db,'result_row_bytes',prepared['row_bytes'],limits['max_output_bytes'])
        budget_delta(db,'control_bytes',prepared['row_bytes']+256,limits['max_control_bytes'])
        db.execute('INSERT INTO queue_results VALUES (?,?,?,?)',
                   (d.generation,d.task_id,context.commit_id,document(prepared['row'],self.config.limits.max_result_bytes).decode()))
        return self._receipt(prepared,context)

    def verify_acquisition(self,prepared):
        self._validate(prepared)
        from .raw_objects import inspect_acquisition
        fact=inspect_acquisition(self.action.context,prepared['acquisition_ref']['commit_id'])
        if fact['prepared']['sinks']['raw']['result']!=prepared['row'][self.result]:
            raise UnresolvedCommit('Final row changed its raw acquisition facts')

    def inspect(self,prepared,context):
        self.verify_acquisition(prepared)
        with self.action.context.journal._connection() as db:
            return self.inspect_in_transaction(db,prepared,context)

    def commit(self,prepared,context):
        raise RecoveryContractError('Queue SQL publication requires its native transaction')

    def abort(self,prepared,context):
        return None


class QueuePublishSink(Datasink):
    sink_id='publish'

    def __init__(self,action,producer,contract):
        self.action,self.producer,self.contract=action,producer,contract
        self.config=action.context.journal.channel.config

    def recovery_identity(self):
        return dict(contract='demiflow.queue.publish.v2',generation=self.producer.generation,transform=self.contract)

    def write(self,blocks,context):
        raise RecoveryContractError('Task publication requires native producer commit')

    def prepare(self,blocks,context):
        self._validate(blocks)
        return blocks

    def _validate(self,prepared):
        if type(prepared) is not dict or set(prepared) not in ({'format','occurrence','source_position','output_ordinal','messages','unit_end','eos'},
                {'format','occurrence','source_position','output_ordinal','messages','unit_end','eos','source_cursor'}) or prepared['format']!=2:
            raise RecoveryContractError('Invalid native producer chunk')
        document(prepared,1024**2)
        if 'source_cursor' in prepared:document(prepared['source_cursor'],4096)
        values=prepared['messages']
        if type(values) is not list or len(values)>32:
            raise RecoveryContractError('Producer chunk exceeds 32 messages')
        document(values,512*1024)
        for m in values:
            if type(m) is not dict or set(m)!={'task_id','pool','payload'} or m['pool']!=self.producer.pool:
                raise RecoveryContractError('Producer message differs from its pool contract')
            from .queue_contract import task_message
            task_message(m['task_id'],m['payload'],pool=m['pool'])
            document(m['payload'],self.config.limits.max_payload_bytes)
        for n in ('source_position','output_ordinal'):
            if type(prepared[n]) is not int or not 0<=prepared[n]<2**63:
                raise RecoveryContractError('Invalid native producer occurrence')
        if type(prepared['unit_end']) is not bool or type(prepared['eos']) is not bool:
            raise RecoveryContractError('Invalid native producer progress flags')
        if prepared['eos'] and (values or not prepared['unit_end']):
            raise RecoveryContractError('Source EOS cannot carry tasks')

    def _fingerprint(self,prepared,index,message):
        return identity([2,self.producer.source.source_id,self.producer.source.sha256,self.producer.source.version,
                         self.contract,prepared['occurrence'],index,message])

    def inspect_in_transaction(self,db,prepared,context):
        self._validate(prepared)
        # The native publication receipt and source progress are written in the
        # same transaction. There is no partly published producer chunk.
        row=db.execute('SELECT document FROM runtime_receipts WHERE commit_id=? AND sink_id=?',(context.commit_id,self.sink_id)).fetchone()
        if row is None:return None
        receipt=json.loads(row[0])
        for index,m in enumerate(prepared['messages']):
            actual=db.execute('SELECT fingerprint,payload,occurrence FROM queue_tasks WHERE generation=? AND task_id=?',(self.producer.generation,m['task_id'])).fetchone()
            expected=(self._fingerprint(prepared,index,m),document(m['payload'],self.config.limits.max_payload_bytes).decode(),identity([prepared['occurrence'],index]))
            if actual!=expected:
                raise UnresolvedCommit('Published task no longer matches its native prepared chunk')
        if receipt['data']['chunk_sha256']!=identity(prepared):
            raise UnresolvedCommit('Producer receipt changed')
        return receipt

    def publish_in_transaction(self,db,prepared,context):
        old=self.inspect_in_transaction(db,prepared,context)
        if old is not None:return old
        p=self.producer
        owner=QueueChannel._producer(db,p)
        row=db.execute('SELECT producer_contract,source_position,tasks,digest,output_offset FROM queue_pools WHERE generation=?',(p.generation,)).fetchone()
        if row[0] is not None and row[0]!=self.contract:
            raise RecoveryContractError('Producer transformation contract changed')
        if prepared['source_position']!=row[1] or prepared['output_ordinal']!=row[4]:
            raise RecoveryContractError('Producer source occurrence is not the active unit')
        if prepared['source_position']>p.source.max_rows or prepared['eos'] and owner[5]:
            raise RecoveryContractError('Producer source progress exceeds its declaration')
        inserted=duplicates=0;digest=row[3]
        for index,m in enumerate(prepared['messages']):
            fingerprint=self._fingerprint(prepared,index,m)
            occurrence=identity([prepared['occurrence'],index])
            payload=document(m['payload'],self.config.limits.max_payload_bytes).decode()
            previous=db.execute('SELECT fingerprint,payload,occurrence FROM queue_tasks WHERE generation=? AND task_id=?',(p.generation,m['task_id'])).fetchone()
            if previous:
                if previous!=(fingerprint,payload,occurrence):
                    raise RecoveryContractError('Conflicting immutable queue task identity')
                duplicates+=1
                continue
            if row[2]+inserted>=self.config.limits.max_tasks:
                raise RecoveryContractError('Queue task count budget exhausted')
            from .raw_budget import budget_delta
            budget_delta(db,'control_bytes',len(payload.encode('utf8'))+512,self.action.context.limits['max_control_bytes'])
            ordinal=self.action.context.journal.channel.database.publication(db,p.generation,'tasks')
            db.execute("INSERT INTO queue_tasks (generation,task_id,fingerprint,payload,occurrence,ordinal,producer_commit,status) VALUES (?,?,?,?,?,?,?,'pending')",(p.generation,m['task_id'],fingerprint,payload,occurrence,ordinal,context.commit_id))
            inserted+=1;digest=identity([digest,ordinal,m['task_id'],fingerprint])
        QueueChannel._producer(db,p)  # Recheck backend time after publication locks.
        if inserted:db.execute("UPDATE queue_counts SET amount=amount+? WHERE generation=? AND status='pending'",(inserted,p.generation))
        position=prepared['source_position']+(1 if prepared['unit_end'] and not prepared['eos'] else 0)
        db.execute('UPDATE queue_pools SET producer_contract=?,source_position=?,output_offset=?,source_cursor=COALESCE(?,source_cursor),tasks=?,digest=?,eos=? WHERE generation=?',
                   (self.contract,position,0 if prepared['unit_end'] else row[4]+1,
                    document(prepared['source_cursor'],4096).decode() if 'source_cursor' in prepared else None,
                    row[2]+inserted,digest,int(prepared['eos']),p.generation))
        return dict(commit_id=context.commit_id,sink_id=self.sink_id,status='committed',
            data=dict(requested=len(prepared['messages']),inserted=inserted,duplicates=duplicates,chunk_sha256=identity(prepared)))

    def inspect(self,prepared,context):
        with self.action.context.journal._connection() as db:return self.inspect_in_transaction(db,prepared,context)

    def commit(self,prepared,context):
        raise RecoveryContractError('Producer SQL publication requires its native transaction')

    def abort(self,prepared,context):
        return None
