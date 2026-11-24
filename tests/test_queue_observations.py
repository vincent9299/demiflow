"""Retryable exhaustion is an inspectable observation, never a source ACK."""
import pytest
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.native_queue.queue import ProducerSourceSpec, task_message
from demiflow.execution.contracts.queue_contract import QueueIncomplete
from demiflow.execution.native_queue.queue_sinks import QueueResultSink
from demiflow.execution.pipeline.run_context import RunContext
from test_queue_v2_protocol import channel
from test_raw_fetch import origin_server


def test_exhausted_observation_is_read_only_and_never_retried_or_acknowledged(channel,tmp_path):
    producer=channel.open('p',producer_id='producer',source=ProducerSourceSpec(kind='records',source_id='fixed',uri='memory:fixed',version=1))
    with origin_server() as (url,calls):
        with RunContext.start(tmp_path/'producer'):
            data.from_items([{'messages':[task_message('x',{'request':{'uri':url+'/retry'}},pool='p')]}]).enqueue(channel.config,producer=producer).run_stream()
        channel.seal(producer)
        def consume(root):
            (data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=.2)
                .fetch(store=LocalObjectStore(root),concurrency=1,
                    policy=FetchPolicy(max_object_bytes=65536,max_attempts=1,http=HttpSource(allow_private=True)))
                .write_datasink(QueueResultSink(channel.config,pool='p')))
        with pytest.raises(QueueIncomplete):consume(tmp_path/'objects')
        assert calls['/retry']==1 and channel.completion(producer.generation,'x') is None
        snapshot=channel.snapshot('p',kind='observations')
        row,=data.read_queue_records(channel.config,pool='p',through=snapshot,results=True).take_all()
        proof=row['queue_record']['proof']
        assert row['result']['status']=='retryable' and row['result']['terminal'] is False
        assert proof['settled'] is False and row['verification']['status']=='verified'
        with channel.database.connection() as db:
            assert db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',(proof['final_commit'],)).fetchone()==('checkpoint_committed',)
        with pytest.raises(QueueIncomplete):consume(tmp_path/'other-objects')
        assert calls['/retry']==1 and channel.state('p')['counts']=={'exhausted':1}
