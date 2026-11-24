"""Reuse only JSON options and fixed field names, never business data or checks."""
from demiflow.execution.contracts import record_codec
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from types import MappingProxyType
import json
import math
import pytest

from demiflow.execution.contracts.dataflow import ArtifactRef, BlockPartition, Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange import dataflow_bridge as codec
from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE


def original_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'),
                      allow_nan=True, default=record_codec._codec_json_default).encode('utf-8')


@pytest.mark.parametrize('value', [None, True, False, 0, 1, -1, 2**256, -(2**256),
    -0.0, 0.0, float('nan'), float('inf'), -float('inf'), '中文é\n\u0000"😀',
    (None, True, -0.0, 2**64), MappingProxyType({'z': [1, -0.0], 'a': MappingProxyType({'中': 'é'})})])
def test_encoder_is_byte_exact_for_scalars_and_immutable_containers(value):
    assert record_codec._codec_dumps(value) == original_json(value)


def test_exact_integer_fast_path_does_not_call_subclass_string_overrides():
    class BusinessInt(int):
        def __str__(self):
            raise AssertionError('business string conversion must not run')
    assert record_codec._codec_dumps(BusinessInt(7)) == original_json(BusinessInt(7)) == b'7'
    with pytest.raises(UnicodeEncodeError):
        record_codec._codec_dumps('\ud800')
    cycle=[];cycle.append(cycle)
    with pytest.raises(ValueError, match='Circular reference'):
        record_codec._codec_dumps(cycle)
    with pytest.raises(TypeError, match='not JSON serializable'):
        record_codec._codec_dumps(object())


@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('schema', [False, True])
@pytest.mark.parametrize('block', [False, True])
def test_original_record_wire_with_optional_schema_block_and_payload_reference(external, schema, block):
    from test_runtime_v3_scalar_values import payload
    import numpy as np
    row=payload()
    row.update(nan=float('nan'), plus=float('inf'), minus=-float('inf'),
        vector=np.array([[-0.0, 0.0, np.nan]], dtype='>f4'), scalar=np.int64(7))
    raw_payload=record_codec._encode_payload(row,max_bytes=65536)
    payload_ref=ArtifactRef('b'*64,len(raw_payload),'local','payload','application/octet-stream') if external else None
    schema_ref=ArtifactRef('c'*64,128,'local','schema',SCHEMA_MEDIA_TYPE) if schema else None
    bp=BlockPartition('d'*64,1,3) if block else None
    record=Record('a'*64,{'offset':5,'parents':[1,{'é':[None,True]}]},payload_ref,
                  None if external else row,len(raw_payload),5,schema_ref,bp)
    doc={'v':3,'sp':record.source_position,'eb':record.encoded_bytes,
         'p':None if external else json.loads(raw_payload),
         'r':asdict(payload_ref) if external else None}
    if schema_ref is not None:doc['s']=asdict(schema_ref)
    if bp is not None:doc['bp']=asdict(bp)
    expected=original_json(doc)
    actual=record_codec._encode_record_document(record,max_bytes=len(expected))
    assert actual==expected
    restored=record_codec._decode_record_document(expected,record_id=record.record_id,
                                          sequence=record.sequence,max_bytes=len(expected))
    assert restored.schema_ref==schema_ref and restored.block_partition==bp
    if external:
        assert restored.payload_ref==payload_ref and restored.inline is None
    else:
        assert record_codec._encode_payload(restored.inline,max_bytes=65536)==raw_payload
        assert math.copysign(1,restored.inline['negative'][0])==-1
    with pytest.raises(DataflowError,match='BUDGET_EXCEEDED'):
        record_codec._encode_record_document(record,max_bytes=len(expected)-1)
    with pytest.raises(DataflowError,match='BUDGET_EXCEEDED'):
        record_codec._decode_record_document(expected,record_id=record.record_id,
                                     sequence=record.sequence,max_bytes=len(expected)-1)
    # Valid JSON with different historical bytes cannot be accepted as canonical.
    noncanonical=b'{ '+expected[1:]
    with pytest.raises(DataflowError,match='canonical'):
        record_codec._decode_record_document(noncanonical,record_id=record.record_id,
                                     sequence=record.sequence,max_bytes=len(noncanonical))


def test_shared_encoder_has_no_cross_thread_traversal_or_value_cache():
    def encode(index):
        item=MappingProxyType({'i':index,'nested':(MappingProxyType({'nan':float('nan'),
            'negative':-0.0,'text':'中'+str(index)}), [index,True,None])})
        return record_codec._codec_dumps(item), original_json(item)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert all(actual==expected for actual,expected in pool.map(encode,range(512)))
    # A small unsupported-key fallback remains identical to the old private helper.
    doc={'extra':7,'é':None,'p':{'t':'null'}}
    assert record_codec._record_document_bytes(doc,payload_raw=b'{"t":"null"}',max_bytes=1024)==original_json(doc)
