"""Actual process death across native queue/object commit boundaries."""
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.queue import ProducerSourceSpec,task_message
from demiflow.execution.queue_sinks import QueueResultSink
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.recovery_contract import RunRef
from test_queue_v2_protocol import channel
from test_raw_fetch import origin_server,BODY


WORKER=r'''
from dataclasses import asdict
import json,os,signal,sys
from pathlib import Path
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.objects import LocalObjectStore
from demiflow.execution.queue import QueueConfig,QueueChannel
from demiflow.execution.queue_sinks import QueueResultSink
from demiflow.execution.run_context import RunContext
from demiflow.secrets import Secret
from demiflow.execution.raw_objects import RawWriter
import demiflow.execution.commit as commits
settings=json.loads(Path(sys.argv[1]).read_text())
raw=settings['config']
if raw['dsn'] is not None:raw['dsn']=Secret(raw['dsn']['secret_env'])
config=QueueConfig(**raw)
channel=QueueChannel(config);channel._attach()
root=Path(settings['root']);seam=settings['seam']
def die(**evidence):
    (root/'killed-at.json').write_text(json.dumps({'seam':seam, **evidence}))
    os.kill(os.getpid(),signal.SIGKILL)
claim=QueueChannel.claim
def claim_then_crash(self, *args, **kwargs):
    delivery=claim(self, *args, **kwargs)
    if delivery is not None:
        die(task_id=delivery.task_id, generation=delivery.generation)
    return delivery
if seam=='claimed':QueueChannel.claim=claim_then_crash
original=RawWriter.write
def partial(self,chunk):
    original(self,chunk)
    die()
def fault(name,commit_id):
    if name not in {'after_prepared','after_checkpoint_committed','after_acked'}:return
    with channel.database.connection() as db:
        row=db.execute('SELECT intent FROM runtime_commits WHERE commit_id=?',(commit_id,)).fetchone()
    sinks=json.loads(row[0])['sinks'] if row else []
    if seam=='raw_prepared' and name=='after_prepared' and sinks==['raw']:die()
    if seam=='final_checkpoint' and name=='after_checkpoint_committed' and sinks==['result']:die(commit_id=commit_id)
    if seam=='final_acked' and name=='after_acked' and sinks==['result']:die(commit_id=commit_id)
if seam=='raw_partial':RawWriter.write=partial
commits.fault_point=fault
with RunContext.start(root/'consumer') as run:
    (root/'run-ref.json').write_text(json.dumps(asdict(run.ref)))
    (data.read_queue(config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=2)
        .fetch(store=LocalObjectStore(root/'objects'),policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=1)
        .write_datasink(QueueResultSink(config,pool='p')))
raise RuntimeError('fault seam was not reached')
'''


def consume(config,root):
    return (data.read_queue(config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=2)
        .fetch(store=LocalObjectStore(root/'objects'),policy=FetchPolicy(max_object_bytes=128*1024,http=HttpSource(allow_private=True)),concurrency=1)
        .write_datasink(QueueResultSink(config,pool='p')))


@pytest.mark.parametrize('seam',['claimed','raw_partial','raw_prepared','final_checkpoint','final_acked'])
@pytest.mark.parametrize('resume_same_run',[False,True])
def test_sigkill_and_native_recovery(channel,tmp_path,seam,resume_same_run):
    with origin_server() as (url,calls):
        p=channel.open('p',producer_id='kill-fixture',source=ProducerSourceSpec(
            kind='manifest',source_id='fixed-request',uri='memory:request',version=1))
        with RunContext.start(tmp_path/'producer'):
            data.from_items([{'messages':[task_message('x',{'request':{'uri':url+'/file'}},pool='p')]}]).enqueue(channel.config,producer=p).run_stream()
        channel.seal(p)
        script=tmp_path/'worker.py';script.write_text(WORKER)
        settings=tmp_path/'settings.json';settings.write_text(json.dumps(dict(config=channel.config.declaration(),root=str(tmp_path),seam=seam)))
        env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1')
        result=subprocess.run([sys.executable,str(script),str(settings)],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=30)
        assert result.returncode==-9,result.stdout.decode()[-5000:]
        interrupted=json.loads((tmp_path/'killed-at.json').read_text())
        assert interrupted['seam']==seam
        assert calls.get('/file', 0)==(0 if seam=='claimed' else 1)
        if seam=='claimed':
            assert (interrupted['generation'], interrupted['task_id']) == (p.generation, 'x')
            assert channel.completion(p.generation,'x') is None
        before=channel.state('p')
        assert before['queue_drained']==(seam=='final_acked')
        if seam!='final_acked':
            # Accelerate only the lease clock for this isolated test. The
            # process was genuinely SIGKILLed; no callback cleanup ran.
            with channel.database.connection(write=True) as db:
                db.execute('UPDATE queue_tasks SET lease_until=0 WHERE generation=?',(p.generation,))
        if resume_same_run:
            reference=RunRef(**json.loads((tmp_path/'run-ref.json').read_text()))
            with RunContext.resume(reference):consume(channel.config,tmp_path)
        else:
            with RunContext.start(tmp_path/'replacement'):consume(channel.config,tmp_path)
        assert channel.state('p')['queue_drained']
        assert calls['/file']==(2 if seam=='raw_partial' else 1)
        proof=channel.completion(p.generation,'x')
        if 'commit_id' in interrupted:
            assert proof['final_commit'] == interrupted['commit_id']
        with channel.database.connection() as db:
            assert db.execute('SELECT status FROM runtime_commits WHERE commit_id=?',(proof['final_commit'],)).fetchone()[0]=='acked'
            assert db.execute('SELECT consumed FROM runtime_budgets WHERE name=?',('pool-'+p.generation+'-object_bytes',)).fetchone()[0]==len(BODY)
            retained=db.execute('SELECT consumed FROM runtime_budgets WHERE name=?',('pool-'+p.generation+'-raw_disk_bytes',)).fetchone()[0]
        assert retained==len(BODY)+(128*1024 if seam=='raw_partial' else 0)
        stages=list((tmp_path/'objects').rglob('.pending/*'))
        assert len(stages)==(1 if seam=='raw_partial' else 0)
