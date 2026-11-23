"""Actual public relational and file-output seams, with original native commits."""
import json
import pyarrow as pa
import pyarrow.parquet as pq
import lance
import pytest
from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext


def _fold(acc,row,*,scale):
    return {'k':row['k'],'v':acc['v']+row['v']*scale,'tag':acc['tag']}


@pytest.mark.parametrize('kind',['json','csv','parquet'])
def test_real_file_writer_commits_and_readable_rows(kind,tmp_path):
    rows=[{'id':2,'v':'b'},{'id':1,'v':'a'},{'id':1,'v':'a'}]
    target=tmp_path/('out.'+kind)
    with RunContext.start(tmp_path/'run') as run, data.local_execution():
        assert getattr(data.from_items(rows),'write_'+kind)(str(target)) is None
        commits=run.journal.execution_commits('action-1')
        assert commits and all(c['status']=='acked' for c in commits)
        out=getattr(data,'read_'+kind)(str(target)).take_all()
        expected=[{'id':str(r['id']),'v':r['v']} for r in rows] if kind=='csv' else rows
        assert sorted(out,key=lambda x:str(x['id']))==sorted(expected,key=lambda x:str(x['id']))




