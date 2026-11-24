"""Explicit graph-tail adapter to the same native final task commit."""
from .queue_contract import queue_config
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, identifier
from demiflow.execution.worker.row_scope import current_row_scope


class QueueAcknowledge:
    concurrency=1
    catch=()
    label='ack_queue'

    def __init__(self,config,*,result='result',queue_depth=1,keep_row=True):
        self.config=queue_config(config);self.result=identifier(result)
        if type(queue_depth) is not int or not 1<=queue_depth<=256:raise ValueError('Invalid ACK queue depth')
        if type(keep_row) is not bool:raise ValueError('keep_row must be boolean')
        self.queue_depth,self.keep_row=queue_depth,keep_row
        self.source=None

    def recovery_identity(self):
        return dict(contract='demiflow.queue.ack.v2',config=self.config.declaration(),result=self.result,keep_row=self.keep_row)

    def bind_execution(self,run,node_id):
        if self.source is None or self.source.parent.context is not run:
            raise RecoveryContractError('Explicit ACK requires a validated graph-tail source')

    async def __call__(self,row):
        from .queue_execution import finalize_task
        scope=current_row_scope()
        if scope is None or scope.delivery is None or self.source is None:
            raise RecoveryContractError('Explicit ACK lost its source-owned delivery')
        fact=await self.source.io.run(finalize_task,self.source,scope.delivery,row)
        if self.keep_row:return row
        return {'queue_completion':{'task_id':scope.delivery.task_id,'final_commit':fact['commit_id'],'status':'acked'}}


def ack_facts(actor):
    from .queue_flow import resource
    return dict(native=actor.recovery_identity(), effects=['queue'],
                resources=[resource(actor.config, 'complete')], reserved_bytes=65536)
