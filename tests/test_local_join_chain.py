"""Same-key reuse preserves values, duplicates, fold order and real work."""
import pytest
from demiflow import data
from demiflow.data.api import DataAPI
from tests.test_local_relational import assert_same_rows
from _native_relational_fixtures import native_region_evidence, oracle_join

pytestmark = pytest.mark.slow


@pytest.mark.parametrize('first_how',['left','inner','semi','anti'])
@pytest.mark.parametrize('second_how',['left','inner','semi','anti'])
def test_join_chain_matches_serial_with_duplicate_and_null_keys(first_how,second_how):
    left=[{'k':None,'v':0},{'k':1,'v':1},{'k':2,'v':2},{'k':1,'v':3},{'k':3,'v':4}]
    right=[{'kb':None,'v':'null'},{'kb':1,'v':'a'},{'kb':1,'v':'b'},{'kb':2,'v':'c'}]
    last=[{'k':None,'z':10},{'k':1,'z':11},{'k':1,'z':12},{'k':4,'z':14}]
    expected_rows=oracle_join(oracle_join(left,right,right_on='kb',how=first_how),last,how=second_how)
    keys=sorted({row['k'] for row in expected_rows},key=lambda key:(key is None,key or 0))
    expected=[{'sequence':[row for row in expected_rows if row['k']==key]} for key in keys]
    def build(api):
        a,b,c=api.from_items(left),api.from_items(right),api.from_items(last)
        return a.join(b,on='k',right_on='kb',how=first_how).join(c,on='k',how=second_how).reduce_by_key(
            'k',lambda s,r:{'sequence':(s or {}).get('sequence',[])+[r]})
    assert build(DataAPI()).take_all()==expected
    with data.local_execution(workers=2,worker_mode='thread',partitions=3,batch_rows=2) as local:
        assert build(data).take_all()==expected
        graph,physical,counters=native_region_evidence(local)
        assert len(counters)==1
        assert counters[0]['partition_builds']==3
        assert counters[0]['leaf_key_rows']==len(left)+len(right)+len(last)
        assert counters[0]['intermediate_key_rebuilds']==0
        assert counters[0]['intermediate_payload_bytes']==0
        assert counters[0]['leaf_payload_bytes']>0
        nodes={node.occurrence_id:node.kernel.kind for node in graph.nodes}
        stage=next(stage for stage in physical.stages if stage.stage_id.startswith('relational-v1-'))
        assert [nodes[occ] for occ in stage.occurrences]==['join','join','reduce']


@pytest.mark.parametrize('mode',['thread','process'])
def test_reuse_join_partitions_on_both_sides_and_reject_changed_keys(mode):
    a=[{'k':1,'v':1},{'k':2,'v':2}]
    b=[{'k':1,'a':10},{'k':2,'a':20}]
    c=[{'x':1,'b':3},{'x':2,'b':4}]
    d=[{'x':1,'c':30},{'x':2,'c':40}]
    def build(api,change=False):
        left=api.from_items(a).join(api.from_items(b),on='k')
        right=api.from_items(c).join(api.from_items(d),on='x')
        if change:
            left=left.map(lambda r:{**r,'k':3-r['k']})
        return left.join(right,on='k',right_on='x')
    completed=[]
    for change in (False,True):
        lhs=oracle_join(a,b)
        if change:
            lhs=[{**row,'k':3-row['k']} for row in lhs]
        expected=oracle_join(lhs,oracle_join(c,d,on='x'),right_on='x')
        assert_same_rows(build(DataAPI(),change).take_all(),expected)
        with data.local_execution(workers=2,worker_mode=mode,partitions=3,batch_rows=1) as local:
            assert_same_rows(build(data,change).take_all(),expected)
            graph,physical,counters=native_region_evidence(local)
            assert len(counters)==1
            assert counters[0]['partition_builds']==(3 if change else 4)
            assert counters[0]['leaf_key_rows']==(6 if change else 8)
            assert counters[0]['intermediate_payload_bytes']==0
            assert counters[0]['intermediate_key_rebuilds']==0
            kinds={node.occurrence_id:node.kernel.kind for node in graph.nodes}
            owner={occ:stage.stage_id for stage in physical.stages for occ in stage.occurrences}
            maps=[node for node in graph.nodes if node.kernel.kind=='MapOp']
            assert bool(maps) is change
            if change:
                edge=next(edge for edge in graph.edges if edge.producer==maps[0].occurrence_id)
                assert kinds[edge.consumer]=='join' and owner[edge.producer]!=owner[edge.consumer]
                assert next(exchange for exchange in physical.exchanges if exchange.edge_id==edge.edge_id).keys==('k',)
        completed.append(change)
    assert completed==[False,True]  # Neither changed-key branch may be short-circuited.


def test_reduce_with_rewritten_visible_key_is_repartitioned():
    def build(api):
        left=api.from_items([{'k':1}]).reduce_by_key('k',lambda s,r:{'k':2})
        return left.join(api.from_items([{'k':2,'v':'matched'}]),on='k')
    with data.local_execution(workers=1,worker_mode='thread',partitions=2) as local:
        assert build(data).take_all()==[{'k':2,'v':'matched'}]
        graph,physical,counters=native_region_evidence(local)
        assert counters==[]
        kinds={node.occurrence_id:node.kernel.kind for node in graph.nodes}
        edge=next(edge for edge in graph.edges if kinds[edge.producer]=='reduce')
        assert kinds[edge.consumer]=='join'
        exchange=next(exchange for exchange in physical.exchanges if exchange.edge_id==edge.edge_id)
        assert exchange.routing=='hash' and exchange.keys==('k',)
