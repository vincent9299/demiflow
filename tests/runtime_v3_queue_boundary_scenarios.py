"""V022 negative boundaries, using native queue transactions and public Dataset calls."""
from contextvars import copy_context
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import time


def _channel(root, name):
    from demiflow.execution.queue import QueueChannel, QueueConfig
    root.mkdir(parents=True)
    return QueueChannel(QueueConfig(backend='sqlite', queue_id=name, path=str(root/'queue.sqlite')))


def _open(channel):
    from demiflow.execution.queue import ProducerSourceSpec
    return channel.open('p', producer_id='boundary', source=ProducerSourceSpec(
        kind='manifest', source_id='boundary', uri='memory:boundary', version=1))


def _publish(channel, producer, root, base):
    from demiflow.execution.queue import task_message
    from test_queue_v2_protocol import publish
    from demiflow.execution.run_context import RunContext
    from runtime_v3_queue_business_scenarios import _body
    with RunContext.start(root/'seed') as run, run.action({'kind':'native-boundary-seed'}) as action:
        body = _body('oak-0')
        message = task_message('oak-0', {'key':'oak-0','request':{'uri':base+'/objects/oak-0',
            'expected_size':len(body),'expected_sha256':hashlib.sha256(body).hexdigest()}}, pool='p')
        publish(action, channel, producer, 0, [message])
        publish(action, channel, producer, 1, [], eos=True)
        action.complete({'native_fixture_tasks':1})
    channel.seal(producer)


def _base_stream(channel, root):
    from demiflow import data
    from demiflow.collect import FetchPolicy, HttpSource
    from demiflow.objects import LocalObjectStore
    return data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=35,lease_s=30).fetch(
        store=LocalObjectStore(root/'raw'),policy=FetchPolicy(max_object_bytes=65536,
            max_attempts=1,http=HttpSource(allow_private=True)),concurrency=1)


def run_boundaries(root, base, calls):
    import pytest
    from demiflow import data
    from demiflow.agent import AgentConfig
    from demiflow.environment import OperatorEnvironment
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.queue_sinks import QueueResultSink
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from runtime_v3_queue_business_scenarios import _messages, PACK
    report = {}

    # Empty and open is PENDING, never fabricated EOI. Publish only after the
    # original queue worker exists, then observe that this same worker proceeds.
    empty = root/'empty-open'; channel = _channel(empty,'empty-open'); producer = _open(channel)
    def consume():
        with RunContext.start(empty/'consumer',limits={'timeout_s':90}):
            _base_stream(channel,empty).write_datasink(QueueResultSink(channel.config,pool='p'))
    with ThreadPoolExecutor(1) as pool:
        task = pool.submit(copy_context().run, consume)
        deadline = time.monotonic()+20
        while True:
            with channel.database.connection() as db:
                count = db.execute('SELECT count(*) FROM queue_workers').fetchone()[0]
            if count: break
            assert not task.done() and time.monotonic()<deadline
            time.sleep(.02)
        assert not task.done() and channel.state('p')['tasks']==0 and not channel.state('p')['queue_drained']
        _publish(channel,producer,empty,base)
        task.result(timeout=90)
    assert channel.state('p')['queue_drained']
    report['empty_open']={'waited_before_publication':True,'same_window_settled':True}

    # Topology rejection must precede claim, HTTP, model work and output writes.
    rejected=[]
    for kind in ('model', 'lance', 'flat_map'):
        case=root/('invalid-'+kind); channel=_channel(case,'invalid-'+kind); producer=_open(channel)
        _publish(channel,producer,case,base)
        before=dict(calls)
        with pytest.raises((RecoveryContractError,DataflowError,ValueError),match='[Qq]ueue|native task|pure|ACK'):
            with RunContext.start(case/'consumer'):
                stream=_base_stream(channel,case)
                if kind=='lance': stream.write_lance(str(case/'forbidden.lance'),mode='create')
                elif kind=='model':
                    config=AgentConfig(parse_prompt_pack(PACK),OperatorEnvironment(resources='resources'),{},2)
                    stream.agentmap_async('review',config=config,inputs={'payload':'item'},output='answer',
                        concurrency=1,queue_depth=1).write_datasink(QueueResultSink(channel.config,pool='p'))
                else: stream.flat_map(_messages).write_datasink(QueueResultSink(channel.config,pool='p'))
        assert dict(calls)==before and channel.state('p')['counts']=={'pending':1}
        assert channel.completion(producer.generation,'oak-0') is None and not (case/'forbidden.lance').exists()
        rejected.append(kind)
    report['illegal_automatic_ack_topologies']=rejected

    # Only the external deadline is faulted. Original verification/publication
    # and original lock_delivery must reject a stale lease without a final ACK.
    case=root/'stale-lease'; channel=_channel(case,'stale-lease'); producer=_open(channel)
    _publish(channel,producer,case,base)
    verify=QueueResultSink.verify_acquisition; expired=[]; before=dict(calls)
    def expire(sink,prepared):
        result=verify(sink,prepared)
        if not expired:
            delivery=sink.action.context.journal.delivery
            with channel.database.connection(write=True) as db:
                db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=? AND task_id=?',
                    (delivery.generation,delivery.task_id))
            expired.append(delivery.task_id)
        return result
    with pytest.MonkeyPatch.context() as patch:
        from demiflow.execution.action_driver.dataflow import DataflowExecution
        start = DataflowExecution.start
        def start_with_external_deadline_fault(execution):
            execution.prepare()  # seal the genuine, unchanged sink program first
            patch.setattr(QueueResultSink,'verify_acquisition',expire)
            return start(execution)
        patch.setattr(DataflowExecution,'start',start_with_external_deadline_fault)
        with pytest.raises((RecoveryContractError,DataflowError),match='lease|epoch|owner'):
            with RunContext.start(case/'consumer',limits={'timeout_s':90}):
                _base_stream(channel,case).write_datasink(QueueResultSink(channel.config,pool='p'))
    assert expired==['oak-0'] and channel.completion(producer.generation,'oak-0') is None
    assert calls['/objects/oak-0']-before.get('/objects/oak-0',0)==1
    with channel.database.connection() as db:
        assert db.execute('SELECT count(*) FROM queue_completions').fetchone()[0]==0
        assert db.execute("SELECT count(*) FROM runtime_receipts WHERE sink_id='raw'").fetchone()[0]==1
    report['stale_lease']={'raw_confirmed':True,'completion_count':0,'final_ack':False}

    # Read the exact closed archive through the public API; hashes must remain
    # identical and it must never be presented as native v2 certification.
    from test_sqlite_channel import archive
    case=root/'legacy';case.mkdir()
    path,digest=archive(case)
    with RunContext.start(case/'reader'):
        for results in (False,True):
            rows=data.read_legacy_queue_records(str(path),sha256=digest,pool='p',through=1,results=results).take_all()
            assert len(rows)==1 and rows[0]['verification']['status']=='legacy_not_certified'
    assert hashlib.sha256(path.read_bytes()).hexdigest()==digest
    assert not Path(str(path)+'-wal').exists() and not Path(str(path)+'-shm').exists()
    report['legacy']={'status':'legacy_not_certified','archive_unchanged':True}
    return report
