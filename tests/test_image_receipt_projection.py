"""Native composite receipt observation is read-only and run-owned."""
import copy
import sqlite3

import lance
import pyarrow as pa
import pytest

from demiflow.execution.outputs.block_sink import BlockSink
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.outputs.lance_sink import LanceSink
from demiflow.execution.outputs.object_sink import ObjectRowsSink
from demiflow.execution.recovery_contract import SourcePosition,UnresolvedCommit
from demiflow.execution.run_context import RunContext
from demiflow.lance.model import LanceWriteSpec
from image_http_fixture import picture


def counters(run):
    with sqlite3.connect(run.journal.path) as db:
        return (list(db.execute('SELECT name,consumed,ceiling FROM runtime_budgets ORDER BY name')),
                db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0])


def fixture_commit(run,tmp_path,multiple):
    with run.action({'kind':'fixture-image-write'}) as action:
        spec=LanceWriteSpec(uri=str(tmp_path/'out.lance'),mode='create')
        rows=LanceSink(action,spec)
        settings={'object_specs':{str(tmp_path/'a'):4096,str(tmp_path/'b'):4096}} if multiple else {'max_object_bytes':4096}
        sink=ObjectRowsSink(action,rows_sink=rows,**settings)
        refs=[store.stage(picture(color='red' if index==0 else 'blue')) for index,store in enumerate(sink.object_sinks.values())]
        fact=CommitCoordinator(action).commit(1,outputs={'lance':(sink,[pa.Table.from_pylist([{'images':refs}])])},position=SourcePosition('fixed',1))
        action.complete({'written':1})
        return action,spec,fact


@pytest.mark.parametrize('multiple',[False,True])
def test_composite_inspection_matches_native_receipt_without_allocating_or_publishing(tmp_path,multiple):
    with RunContext.start(tmp_path/'run') as run:
        action,spec,fact=fixture_commit(run,tmp_path,multiple)
        before=counters(run)
        sink=ObjectRowsSink.for_inspection(action,LanceSink(action,spec),fact['prepared']['sinks']['lance'])
        receipt=sink.inspect(fact['prepared']['sinks']['lance'],CommitCoordinator(action)._context(fact['commit_id']))
        assert receipt==fact['receipts']['lance']
        assert counters(run)==before and lance.dataset(spec.uri).version==1


def test_composite_inspection_cannot_adopt_an_unregistered_logical_root(tmp_path):
    with RunContext.start(tmp_path/'run') as run:
        action,spec,fact=fixture_commit(run,tmp_path,False)
        prepared=copy.deepcopy(fact['prepared']['sinks']['lance'])
        foreign=tmp_path/'foreign';foreign.mkdir();(foreign/'keep').write_bytes(b'preserve')
        prepared['objects']['logical_root']=str(foreign)
        before=counters(run)
        with pytest.raises(UnresolvedCommit,match='registered run namespace'):
            ObjectRowsSink.for_inspection(action,LanceSink(action,spec),prepared)
        assert counters(run)==before and (foreign/'keep').read_bytes()==b'preserve'
        assert not (foreign/'.demiflow-objects').exists() and lance.dataset(spec.uri).version==1
