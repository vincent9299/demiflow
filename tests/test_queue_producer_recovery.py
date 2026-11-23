"""Fixed-source cursor and unknown source-effect recovery through public APIs."""
import json

import pytest
from demiflow import data
from demiflow.execution.queue import ProducerSourceSpec, task_message
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.recovery_contract import UnresolvedCommit
from test_queue_v2_protocol import channel, search_producer
from test_native_search_runtime import engines, server


def test_lance_recovery_seeks_after_confirmed_input_unit(channel, tmp_path, monkeypatch):
    import demiflow.execution.commit as commits
    import demiflow.lance.read as reader
    path=str(tmp_path/'fixed.lance')
    data.from_items([{'messages':[task_message('x'+str(i),{'value':i},pool='p')]} for i in range(3)]).write_lance(path,mode='create')
    producer=channel.open('p',producer_id='producer',source=ProducerSourceSpec(kind='lance',source_id='fixed-lance',uri=path,version=1))
    fired=[]
    def fault(name, commit_id):
        if name!='after_sink_receipt' or fired:return
        with channel.database.connection() as db:
            row=db.execute('SELECT prepared FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
        chunk=json.loads(row[0]).get('sinks',{}).get('publish',{}) if row and row[0] else {}
        if chunk.get('unit_end') and not chunk.get('eos'):
            fired.append(True);raise RuntimeError('after-native-unit')
    monkeypatch.setattr(commits,'fault_point',fault)
    with pytest.raises(Exception,match='after-native-unit'):
        with RunContext.start(tmp_path/'first'):
            data.read_lance(path,version=1).enqueue(channel.config,producer=producer).run_stream()
    offsets=[];scan=reader._iter_scan
    def observed(*args,**kwargs):
        offsets.append(kwargs['offset'])
        yield from scan(*args,**kwargs)
    monkeypatch.setattr(reader,'_iter_scan',observed)
    monkeypatch.setattr(commits,'fault_point',lambda *_:None)
    newer=channel.open('p',producer_id='producer',source=producer.source)
    with RunContext.start(tmp_path/'second'):
        data.read_lance(path,version=1).enqueue(channel.config,producer=newer).run_stream()
    assert offsets==[1] and channel.seal(newer)['actual_tasks']==3


def test_unfixed_search_exchange_cannot_repeat_http_in_a_new_run(channel,tmp_path,engines,server,monkeypatch):
    from demiflow.collect.search_contract import NativeSearchResultSink
    producer=channel.open('p',producer_id='search',source=ProducerSourceSpec(kind='keywords',source_id='fixed-keywords',uri='memory:keywords',version=1))
    original=NativeSearchResultSink.prepare
    def interrupt(self,*args,**kwargs):raise RuntimeError('response-not-fixed')
    monkeypatch.setattr(NativeSearchResultSink,'prepare',interrupt)
    with pytest.raises(Exception,match='response-not-fixed'):
        search_producer(channel,producer,tmp_path,engines,'first')
    assert len(server[1])==1 and channel.state('p')['tasks']==0
    (tmp_path/'search-cache.sqlite').unlink()
    monkeypatch.setattr(NativeSearchResultSink,'prepare',original)
    with pytest.raises(UnresolvedCommit):
        # Reusing the same ProducerRef still gives a different native run fence.
        search_producer(channel,producer,tmp_path,engines,'second')
    assert len(server[1])==1 and channel.state('p')['tasks']==0
