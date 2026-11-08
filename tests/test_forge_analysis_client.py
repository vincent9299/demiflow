"""Data-broker protocol tests only; the server peer is deliberately synthetic."""
from contextlib import contextmanager
import hashlib
import socket
import struct
import threading

import pyarrow as pa
import pytest

from demiflow.workbench.analysis import client as api
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import decode_chunk
from demiflow.workbench.analysis.arrow_bridge import encode_batches
from demiflow.workbench.analysis.framing import Frames
from demiflow.forge.schema import ForgeError


def handle():
    return dict(id="o"+"a"*32, generation=1, digest="a"*64, rows=2,
                schema=[dict(name="value", type="int64", nullable=True)], retention="ephemeral")


@contextmanager
def broker_peer(callback):
    workers, errors = [], []
    client = api.BrokerClient()

    def connect():
        local, peer = socket.socketpair()
        local.settimeout(3); peer.settimeout(3)

        def run():
            try:
                with peer:
                    frames = Frames(peer)
                    callback(frames, frames.json())
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=run)
        workers.append(worker); worker.start()
        return local

    client._connect = connect
    try:
        yield client
    finally:
        for worker in workers:
            worker.join(timeout=5)
            assert not worker.is_alive()
        assert not errors, errors


def test_frame_rejects_oversize_header_before_allocating_payload():
    one, two = socket.socketpair()
    try:
        one.sendall(struct.pack("!cI", b"A", 1024*1024 + 1))
        frames = Frames(two)
        reads = []
        original = frames._read

        def observed(size):
            reads.append(size)
            return original(size)

        frames._read = observed
        with pytest.raises(ForgeError) as error:
            frames.receive()
        assert error.value.code == "resource_limit" and reads == [5]
    finally:
        one.close(); two.close()


def test_sql_sends_only_opaque_handles_and_reply_loss_does_not_retry():
    requests = []

    def serve(frames, request):
        requests.append(request)
        if request["request_id"] != "lost-query":
            frames.send("json", handle())

    with broker_peer(serve) as client:
        source = api.TableHandle(client, handle())
        result = client.sql('select "value" from _source', sources={"_source": source}, request_id="query-one")
        assert result.reference() == source.reference()
        assert requests[0]["payload"]["sources"] == {"_source": source.reference()}
        assert "uri" not in str(requests[0]) and "path" not in str(requests[0])
        for alias in ("bad-path", "é", "a'", "1bad"):
            with pytest.raises(ForgeError):
                client.sql("select 1", sources={alias: source})
        with pytest.raises(ForgeError, match="original request_id=lost-query"):
            client.sql("select 1", request_id="lost-query")
        assert len(requests) == 2


def test_explicit_table_alias_sends_only_handle_and_optional_previous_value():
    requests=[]
    def serve(frames,request):
        requests.append(request)
        assert request['operation']=='bind-table'
        frames.send('json',handle())
    with broker_peer(serve) as client:
        value=api.TableHandle(client,handle())
        assert value.bind('numbers').reference()==value.reference()
        value.bind('numbers',expected=value)
        assert requests[0]['payload']==dict(alias='numbers',handle=value.reference())
        assert requests[1]['payload']['expected']==value.reference()
        with pytest.raises(ForgeError):value.bind('bad-name')
        with pytest.raises(ForgeError):value.bind('numbers',expected=object())
        assert len(requests)==2


def test_upload_identity_contains_all_data_and_reader_is_consumed_once(tmp_path, monkeypatch):
    monkeypatch.setattr(api, "_SPOOL_DIRECTORY", str(tmp_path))
    seen = []
    seen_payload = None
    generator_visits = []

    def serve(frames, request):
        nonlocal seen_payload
        seen.append(request)
        if seen_payload is not None:
            assert request["payload"]["stream_sha256"] != seen_payload["stream_sha256"]
            frames.send("json", dict(error="same ID, different content", code="conflict"))
            return
        seen_payload = request["payload"]
        frames.send("json", dict(accepted=True))
        stream_hash = hashlib.sha256()
        chunks = list(frames.arrow())
        for chunk in chunks:
            stream_hash.update(struct.pack("!I", len(chunk))); stream_hash.update(chunk)
        assert stream_hash.hexdigest() == seen_payload["stream_sha256"]
        assert sum(4+len(chunk) for chunk in chunks) == seen_payload["encoded_bytes"]
        assert pa.Table.from_batches([decode_chunk(chunk) for chunk in chunks])["value"].to_pylist() == [1, None]
        frames.send("json", handle())

    def generate():
        generator_visits.append(1)
        yield pa.record_batch([pa.array([1, None], type=pa.int64())], names=["value"])

    with broker_peer(serve) as client:
        reader = pa.RecordBatchReader.from_batches(pa.schema([("value", pa.int64())]), generate())
        first = client.publish(reader, request_id="upload-one")
        assert first.reference()["generation"] == 1 and generator_visits == [1]
        with pytest.raises(ForgeError) as error:
            client.publish(pa.table({"value": pa.array([2, None], type=pa.int64())}), request_id="upload-one")
        assert error.value.code == "conflict" and len(seen) == 2
    assert list(tmp_path.iterdir()) == []  # closed bounded scratch spool


def test_arrow_read_preserves_nulls_and_closes_connection_after_consumption():
    original = pa.table({"value": pa.array([1, None], type=pa.int64())})

    def serve(frames, request):
        assert request["operation"] == "read" and request["payload"]["handle"]["digest"] == "a"*64
        frames.send("json", {"accepted": True})
        for chunk in encode_batches(original.to_reader()):
            frames.send("arrow", chunk)
        frames.send("end")

    with broker_peer(serve) as client:
        assert api.TableHandle(client, handle()).to_arrow().equals(original)


def test_missing_broker_has_no_local_kernel_sql_or_file_fallback(monkeypatch):
    attempts = []

    def unavailable():
        attempts.append(1)
        raise ForgeError("Restricted analysis data broker unavailable", code="operation_rejected")

    client = api.BrokerClient(); client._connect = unavailable
    with pytest.raises(ForgeError) as error:
        client.sql("select 1")
    assert error.value.code == "operation_rejected" and attempts == [1]


def test_rich_fixed_source_sample_and_case_view_use_registered_alias_without_sql_casts():
    source=c.source_snapshot(dict(kind='fixed_lance',binding='f'*64,rows=2,retention='external',
        schema=[dict(name='messages',type='list',nullable=False)],case_key=dict(kind='row_id',field=None)))
    requests=[]
    def serve(frames,request):
        requests.append(request)
        if request['operation']=='source-info':frames.send('json',source)
        else:
            assert request['operation']=='source-sample' and request['payload']['digest']==source['digest']
            frames.send('json',dict(source_snapshot=source['digest'],rows=[dict(_rowid='5',messages=[dict(role='user',content='text')])]))
    with broker_peer(serve) as client:
        fixed=client.source('data')
        assert fixed.sample(['messages'],limit=1)['rows'][0]['messages']==[dict(role='user',content='text')]
        value=api.case_view(fixed,fields=[dict(id='messages',field='messages',label='messages',role='input',format='dialogue')])
        assert value.descriptor['source_snapshot']==source['digest']
        with pytest.raises(ForgeError):client.sql('SELECT * FROM data',sources=dict(data=fixed))
    assert [value['operation'] for value in requests]==['source-info','source-sample','source-info']
    assert not any('uri' in value['payload'] or 'path' in value['payload'] for value in requests)


def test_case_view_helper_preserves_v1_and_emits_validated_v2_output_groups():
    from test_forge_analysis_case_groups import manifest,descriptor
    source=c.source_snapshot(manifest());value=descriptor(source)
    def serve(frames,request):
        assert request['operation']=='source-info'
        frames.send('json',source)
    with broker_peer(serve) as client:
        fixed=client.source('data')
        args=dict(fields=value['fields'],questions=value['questions'],layout='comparison')
        old=api.case_view(fixed,**args)
        assert old.descriptor['version']==1 and 'output_groups' not in old.descriptor
        grouped=api.case_view(fixed,**args,output_groups=value['output_groups'])
        assert grouped.descriptor==value
        assert grouped._repr_mimebundle_()["application/vnd.demiflow.case-view+json"]==value
        with pytest.raises(ForgeError):api.case_view(fixed,**args,output_groups=value['output_groups'],variant_key='base_text')
