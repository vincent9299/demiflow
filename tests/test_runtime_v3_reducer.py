"""Public reducer program and bound initial data cross the unified worker seam."""
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


def _fold(acc,row,*,scale):
    return {"k":row["k"],"v":acc["v"]+row["v"]*scale,"tag":acc["tag"]}


def test_reducer_program_initial_and_kwargs_reach_real_worker(tmp_path):
    rows=[{'k':'b','v':3},{'k':'a','v':2},{'k':'a','v':5}]
    initial={'v':1,'tag':(b'kept',-0.0)}
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        out=data.from_items(rows).reduce_by_key('k',_fold,initial=initial,fn_kwargs={'scale':2}).sort('k').take_all()
        assert out==[{'k':'a','v':15,'tag':(b'kept',-0.0)},{'k':'b','v':7,'tag':(b'kept',-0.0)}]
        assert initial=={'v':1,'tag':(b'kept',-0.0)}
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'

