"""Public queue postprocessing uses the ordinary native row-map driver."""
import pytest

from demiflow import data
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.queue_sinks import QueueResultSink
from test_queue_v2_protocol import channel
from test_queue_v2_boundaries import seed, worker
from test_raw_fetch import origin_server


class PureAnnotation:
    def __call__(self,row):
        return {**row,'accepted':False,'temporary':'remove-me'}


class MutableActor:
    def __call__(self,row):
        self.count=getattr(self,'count',0)+1
        return row


class InitializingActor:
    def __init__(self):
        raise AssertionError('preflight must not construct this actor')
    def __call__(self,row):
        return row


class EffectActor:
    effects=('network',)
    def __call__(self,row):
        return row


class WrongReceipt:
    def __call__(self,row):
        return {**row,'result':{**row['result'],'size_bytes':1}}


class DropActor:
    def __call__(self,row):
        return None


def annotate(status):
    return 'downloaded' if status=='ok' else 'failed'


def receipt_status(result):
    return result['status']


@pytest.mark.parametrize('explicit',[False,True])
def test_stateless_actor_field_binding_and_projection_settle(channel,tmp_path,monkeypatch,explicit):
    from demiflow.execution.stream_map import _MapWorker
    from demiflow.collect import FetchPolicy,HttpSource
    from demiflow.objects import LocalObjectStore
    events=[];original=_MapWorker._hook
    async def hook(self,name):
        if self.operation.callable.target is PureAnnotation:events.append(name)
        return await original(self,name)
    monkeypatch.setattr(_MapWorker,'_hook',hook)
    with origin_server() as (url,calls):
        seed(channel,tmp_path,url)
        ds=data.read_queue(channel.config,pool='p',max_tasks=1,max_inflight=1,idle_timeout_s=.2,
                           settlement='explicit' if explicit else 'sink').fetch(
            store=LocalObjectStore(tmp_path/'objects'),concurrency=1,
            policy=FetchPolicy(max_object_bytes=131072,http=HttpSource(allow_private=True)))
        ds=(ds.map(PureAnnotation).drop_columns(['request','temporary'])
            .rename_columns({'accepted':'business_accepted'})
            .select_columns(['result','business_accepted','_queue_delivery'])
            .map(receipt_status,inputs={'result':'result'},output='status')
            .map(annotate,inputs={'status':'status'},output='label'))
        if explicit:ds.ack_queue(channel.config).run_stream()
        else:assert ds.write_datasink(QueueResultSink(channel.config,pool='p')) is None
        row,=data.read_queue_records(channel.config,pool='p',through=channel.snapshot('p'),results=True).take_all()
        assert row['business_accepted'] is False and row['label']=='downloaded'
        assert row['result']['status']=='ok' and row['verification']['status']=='verified'
        assert channel.state('p')['queue_drained'] and calls['/file']==1
    assert events==['astart','astop','aclose']


@pytest.mark.parametrize('actor',[MutableActor,InitializingActor,EffectActor])
def test_unknown_actor_state_or_effect_rejected_before_claim(channel,tmp_path,actor):
    with origin_server() as (url,calls):
        seed(channel,tmp_path,url)
        with pytest.raises(RecoveryContractError,match='actor'):
            worker(channel,tmp_path/'objects',transform=lambda ds:ds.map(actor))
        assert not calls and channel.state('p')['counts']=={'pending':1}


@pytest.mark.parametrize('kind',['select','drop','rename','bind'])
def test_projection_cannot_remove_or_replace_native_facts(channel,tmp_path,kind):
    with origin_server() as (url,calls):
        seed(channel,tmp_path,url)
        def transform(ds):
            if kind=='select':return ds.select_columns(['result'])
            if kind=='drop':return ds.drop_columns(['result'])
            if kind=='rename':return ds.rename_columns({'result':'different'})
            return ds.map(receipt_status,inputs={'result':'result'},output='_queue_delivery')
        with pytest.raises(RecoveryContractError):worker(channel,tmp_path/'objects',transform=transform)
        assert not calls and channel.state('p')['counts']=={'pending':1}


@pytest.mark.parametrize('actor',[WrongReceipt,DropActor])
def test_actor_dynamic_receipt_or_cardinality_violation_never_acks(channel,tmp_path,actor):
    with origin_server() as (url,calls):
        p=seed(channel,tmp_path,url)
        with pytest.raises((RecoveryContractError,TypeError)):
            worker(channel,tmp_path/'objects',transform=lambda ds:ds.map(actor))
        assert calls['/file']==1 and channel.completion(p.generation,'x') is None
        assert channel.state('p')['counts']=={'leased':1}
