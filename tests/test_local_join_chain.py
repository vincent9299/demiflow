"""Same-key join reuse must retain row values, duplicates and fold order."""
import pytest
from demiflow import data
from demiflow.data.api import DataAPI


@pytest.mark.parametrize('first_how',['left','inner','semi','anti'])
@pytest.mark.parametrize('second_how',['left','inner','semi','anti'])
def test_join_chain_matches_serial_with_duplicate_and_null_keys(first_how,second_how):
    def build(api):
        a=api.from_items([{'k':None,'v':0},{'k':1,'v':1},{'k':2,'v':2},{'k':1,'v':3},{'k':3,'v':4}])
        b=api.from_items([{'kb':None,'v':'null'},{'kb':1,'v':'a'},{'kb':1,'v':'b'},{'kb':2,'v':'c'}])
        c=api.from_items([{'k':None,'z':10},{'k':1,'z':11},{'k':1,'z':12},{'k':4,'z':14}])
        return a.join(b,on='k',right_on='kb',how=first_how).join(c,on='k',how=second_how).reduce_by_key(
            'k',lambda s,r:{'sequence':(s or {}).get('sequence',[])+[r]})
    expected=build(DataAPI()).take_all()
    with data.local_execution(workers=2,worker_mode='thread',partitions=3,batch_rows=2) as local:
        assert build(data).take_all()==expected
        assert sum(s['name']=='datafusion' for s in local.stats['stages'])==1


@pytest.mark.parametrize('mode',['thread','process'])
def test_reuse_join_partitions_on_both_sides_and_reject_changed_keys(mode):
    def build(api,change=False):
        left=api.from_items([{'k':1,'v':1},{'k':2,'v':2}]).join(
            api.from_items([{'k':1,'a':10},{'k':2,'a':20}]),on='k')
        right=api.from_items([{'x':1,'b':3},{'x':2,'b':4}]).join(
            api.from_items([{'x':1,'c':30},{'x':2,'c':40}]),on='x')
        if change:
            left=left.map(lambda r:{**r,'k':3-r['k']})
        return left.join(right,on='k',right_on='x')
    for change in (False,True):
        expected=build(DataAPI(),change).take_all()
        with data.local_execution(workers=2,worker_mode=mode,partitions=3,batch_rows=1) as local:
            assert build(data,change).take_all()==expected
            queries=sum(s['name']=='datafusion' for s in local.stats['stages'])
            assert queries==(2 if change else 1)


def test_reduce_with_rewritten_visible_key_is_repartitioned():
    def build(api):
        left=api.from_items([{'k':1}]).reduce_by_key('k',lambda s,r:{'k':2})
        return left.join(api.from_items([{'k':2,'v':'matched'}]),on='k')
    with data.local_execution(workers=1,worker_mode='thread',partitions=2) as local:
        assert build(data).take_all()==[{'k':2,'v':'matched'}]
        assert sum(s['name']=='datafusion' for s in local.stats['stages'])==2
