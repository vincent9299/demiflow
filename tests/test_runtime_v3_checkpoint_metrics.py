"""Original acknowledged worker counters define checkpoint deltas on resume."""
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.contracts.dataflow_codec import compute_participant_id
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.exchange.deliveries import DeliveryExchange
from demiflow.execution.dataflow_operator_observations import restore_node_metrics
from demiflow.execution.recovery_contract import identity, document
from test_runtime_v3_native_rpc_replay import interrupted_run, service


@pytest.mark.parametrize('change',['none','foreign','negative','bool','legacy'])
def test_original_frontier_counters_restore_owned_checkpoint_delta_once(tmp_path,monkeypatch,change):
    source,output=identity('source'),identity('output')
    stages=[NS(stage_id=identity('stage-'+str(i)),partitions=('p0',),occurrences=(occ,))
            for i,occ in enumerate((source,output))]
    metrics={source:{'rows_input':0,'rows_output':128},output:{'rows_input':128,'rows_output':126}}
    if change=='foreign':metrics[identity('other')]=metrics.pop(output)
    if change=='negative':metrics[output]['rows_output']=-1
    if change=='bool':metrics[source]['rows_output']=True
    if change=='legacy':metrics={}
    with interrupted_run(tmp_path/'run') as run, service(run) as (_,store,lease):
        raw=document({'schema':'demiflow.worker-frontier.v3','metrics':metrics})
        ref=store.put(iter([raw]),max_bytes=len(raw),media_type='application/json')
        image=NS(contributions=tuple(NS(participant_id=compute_participant_id(stage.stage_id,'p0'),
            operator_states={identity(['worker-frontier','p0']):ref}) for stage in stages))
        execution=NS(_physical=NS(stages=stages),_store=store,_bounds=NS(max_state_bytes=4096),
            _graph=NS(sources=(source,)),_checkpoint_source_rows_only=False,
            _exchange=DeliveryExchange(),_last_edge_acks={},_policy=NS(checkpoint_rows=128))
        reads=[];original=store.read
        def read(value,**kwargs):
            reads.append(value)
            return original(value,**kwargs)
        monkeypatch.setattr(store,'read',read)
        if change in ('foreign','negative','bool'):
            with pytest.raises(DataflowError,match='metric ownership|nonnegative'):
                restore_node_metrics(execution,image)
            assert not hasattr(execution,'_node_metrics')
            return
        restore_node_metrics(execution,image)
        assert reads==[ref]  # One original colocated worker frontier, two participants.
        execution._checkpoint_frontiers=lambda:DataflowExecution._checkpoint_frontiers(execution)
        execution._last_checkpoint_rows,execution._last_checkpoint_outputs=execution._checkpoint_frontiers()
        assert DataflowExecution._checkpoint_progress(execution)==0
        assert execution._last_checkpoint_rows==(0 if change=='legacy' else 128)
        execution._node_metrics[(source,'p0')]['rows_output']+=2
        execution._node_metrics[(output,'p0')]['rows_output']+=2
        assert DataflowExecution._checkpoint_progress(execution)==2
        assert execution._exchange.snapshot_acks()[1]==execution._last_edge_acks=={}
