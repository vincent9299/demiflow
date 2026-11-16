"""Read-only diagnostic: exact original region rows, no Dataset or services.

Run from this worktree with PYTHONPATH=$PWD:$PWD/tests. Prints JSON; redirect
only to an owned /tmp/demiflow-* path. Does not alter budgets or production.
"""
import gc
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tracemalloc
from collections.abc import Mapping

from _native_relational_fixtures import oracle_join
from demiflow.execution.exchange import dataflow_bridge as codec
from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.dataflow_record_frames import raw_workspace

ROOT = Path(__file__).resolve().parents[2]
assert Path(codec.__file__).resolve().is_relative_to(ROOT)


def deep_bytes(value, seen=None):
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    count = sys.getsizeof(value)
    if isinstance(value, Mapping):
        count += sum(deep_bytes(k, seen) + deep_bytes(v, seen) for k, v in value.items())
    elif isinstance(value, (tuple, list, set, frozenset)):
        count += sum(deep_bytes(v, seen) for v in value)
    return count


def measure(name, function, existing):
    function()  # Warm codec imports outside the measured allocation interval.
    gc.collect()
    tracemalloc.start()
    try:
        output = function()
        current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return output, {'phase': name, 'incremental_peak': peak,
        'incremental_retained': current, 'existing_deep': deep_bytes(existing),
        'peak_plus_existing_deep': peak + deep_bytes(existing)}


results = []
for row_limit, key_bytes in ((4096, 400), (16384, 6144)):
    keys = ['a' * key_bytes, 'b' * key_bytes, None]
    left = [{'k': keys[i % 3], 'v': i} for i in range(24)]
    right = [{'k': key, 'r': j} for key in keys for j in range(2)]
    last = [{'k': key, 'z': j} for key in keys for j in range(2)]
    groups = {}
    for row in oracle_join(oracle_join(left, right, how='left'), last, how='left'):
        groups.setdefault(row['k'], []).append((row['v'], row.get('r'), row.get('z')))
    expected = [{'k': key, 'values': groups[key]}
                for key in [*sorted(k for k in groups if k is not None), None]]
    for index, row in enumerate(expected):
        payload = codec._encode_payload(row, max_bytes=row_limit)
        record = Record('a' * 64, {}, None, row, len(payload), 0)
        raw = codec._encode_record_document(record, max_bytes=2 * row_limit,
                                            payload_max_bytes=row_limit)
        decoded, full = measure('full_decode', lambda: codec._decode_record_document(raw,
            record_id=record.record_id, sequence=0, max_bytes=2 * row_limit,
            payload_max_bytes=row_limit, metadata_max_bytes=1024**2), raw)
        assert decoded.inline == record.inline
        item = {'row_limit': row_limit, 'key_bytes': key_bytes, 'index': index,
            'tuple_count': len(row['values']), 'payload_bytes': len(payload),
            'payload_sha256': hashlib.sha256(payload).hexdigest(), 'document_bytes': len(raw),
            'workspace_admission': raw_workspace(raw), 'full': full}
        if index == 0:
            doc = codec._codec_loads(raw)
            value = codec._decode_value(doc['p'])
            item['phases'] = [
                measure('json_parse', lambda: codec._codec_loads(raw), raw)[1],
                measure('decode_value', lambda: codec._decode_value(doc['p']), (raw, doc))[1],
                measure('second_tag_tree', lambda: codec._encode_value(value), (raw, doc, value))[1],
                measure('canonical_payload', lambda: codec._codec_dumps(doc['p']), (raw, doc, value))[1],
                measure('freeze_record', lambda: Record('a'*64, {}, None, value, len(payload), 0),
                        (raw, doc, value, payload))[1]]
        results.append(item)

paths = ['demiflow/execution/dataflow_record_frames.py',
         'demiflow/execution/exchange/dataflow_bridge.py',
         'demiflow/execution/contracts/dataflow.py',
         'tests/test_runtime_v3_region_terminal_envelope.py',
         'tests/_native_relational_fixtures.py']
print(json.dumps({'kind': 'pure_codec_diagnostic_not_business_acceptance',
    'python': sys.executable, 'version': sys.version,
    'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
    'input_sha256': {path: hashlib.sha256((ROOT/path).read_bytes()).hexdigest() for path in paths},
    'probe_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'provenance': 'Empty source_position and no schema sidecar; actual terminal frames add metadata.',
    'memory_scope': 'Prewarmed tracemalloc interval plus existing deduplicated object graph; not RSS.',
    'results': results}, indent=2) + '\n')
