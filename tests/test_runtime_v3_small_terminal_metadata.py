"""The unfinished terminal manifest shares the original worker frame grant."""
from collections import deque
from types import SimpleNamespace as NS
import threading

import pytest

from demiflow.execution.contracts.dataflow import BlockPartition
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_publication_budget import TERMINAL_WORKSPACE
from demiflow.execution.dataflow_record_frames import frame_parts
from demiflow.execution.dataflow_sinks import MANIFEST_MEDIA
from demiflow.execution.recovery_contract import document, identity
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from demiflow.execution.worker.terminal_blocks import TerminalBlocks
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_c import bounds, make


def unfinished(store, *, columns):
    """Authenticated nonfinal, two-block history; no native publication."""
    limits = bounds(max_resident_bytes=65536, max_row_bytes=8192,
                    max_batch_bytes=16384, max_control_bytes=8192, max_state_bytes=131072)
    services = NS(store=store, bounds=limits)
    writer = TerminalBlocks(services, 'worker')
    names = [f'column-{i:03d}-' + 'name' * 8 for i in range(columns)]
    row = dict.fromkeys(names, 1)
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    try:
        for index in range(2):
            record = make('Record', inline=row, encoded_bytes=len(_encode_payload(row, max_bytes=8192)),
                          block_partition=BlockPartition(identity(['two-blocks']), index, 2))
            size, chunks = frame_parts(record, limits, services=services)
            writer.append_parts(record, chunks, size=size)
        manifest = {'schema':'demiflow.dataflow_sink_manifest.v3', 'sink_id':'write_lance',
                    'partition_id':'worker', 'rows':writer.rows, 'bytes':writer.bytes,
                    'columns':names, 'partitions':writer.manifest_partitions()}
        raw = document(manifest, limits.max_control_bytes)
        ref = store.put(iter((raw,)), max_bytes=len(raw), media_type=MANIFEST_MEDIA)
        return ref, manifest
    finally:
        writer.close()


def owner(store, ref):
    value = WorkerGraphRunner.__new__(WorkerGraphRunner)
    value.bounds = bounds(max_resident_bytes=14336, max_row_bytes=8192,
                          max_batch_bytes=16384, max_control_bytes=8192, max_state_bytes=131072)
    value.services = NS(store=store, bounds=value.bounds, check_stop=lambda:None)
    contribution = NS(participant_id='worker', sinks={'write_lance':ref}, final=False)
    value.e = NS(_restore_image=NS(contributions=(contribution,)),
                 _participant_ids=lambda:('worker',), _kernels={}, _readers={})
    value._terminal_spec = NS(kind='write_lance')
    value._terminal_columns = value._terminal_schema_ref = None
    value._restored_terminal = False
    value._condition = threading.Condition()
    value._wake_generation = 0
    value._peer = NS(_bytes=0)
    value._queue = deque()
    value._terminal_blocks = TerminalBlocks(value.services, 'worker',
        budget={'schema':TERMINAL_WORKSPACE, 'resident_bytes':14336}, frame_call=value._frame_call)
    return value


@pytest.mark.parametrize('columns', [2, 12, 24, 48])
def test_unfinished_multiblock_restore_admits_outer_manifest_with_each_index(tmp_path, record_property, columns):
    with store_fixture(tmp_path) as (run, store, _):
        ref, manifest = unfinished(store, columns=columns)
        runner = owner(store, ref)
        calls = []
        original = runner._frame_call
        def frame(function, *args, **kwargs):
            calls.append((function.__name__, runner._terminal_blocks.retained_bytes))
            return original(function, *args, **kwargs)
        runner._frame_call = frame
        runner._terminal_blocks._frame_call = frame
        try:
            if columns <= 12:
                runner._restore_terminal()
                assert runner._restored_terminal and runner._terminal_rows == 2
                assert runner._terminal_columns == manifest['columns']
                assert runner._terminal_blocks.columns is runner._terminal_columns
                assert len(runner._terminal_blocks.blocks) == 2
                before = runner._terminal_blocks.retained_bytes
                # The live columns alias the same object inside this wrapper.
                # Retaining another alias only adds the tiny owner container.
                with runner._terminal_blocks.hold_metadata(runner._terminal_columns):
                    assert runner._terminal_blocks.retained_bytes - before <= 16
                assert [name for name, _ in calls].count('decode_metadata') == 3
            else:
                with pytest.raises(DataflowError) as failure:
                    runner._restore_terminal()
                assert failure.value.code == 'BUDGET_EXCEEDED'
                assert not runner._restored_terminal
                assert not runner._terminal_blocks._metadata_owners
                # The outer JSON is guarded, before any child index is read.
                assert calls[0][0] == 'read_document'
            assert not run.journal.execution_commits('action-1')
        finally:
            record_property('frame_calls', calls)
            runner._terminal_blocks.close()


def test_manifest_encoding_counts_columns_and_partitions_once_and_admits_before_encoding(tmp_path, monkeypatch):
    import json
    with store_fixture(tmp_path) as (_, store, _):
        ref, manifest = unfinished(store, columns=2)
        runner = owner(store, ref)
        blocks = runner._terminal_blocks
        try:
            runner._restore_terminal()
            parts = blocks.manifest_partitions()
            value = {**manifest, 'partitions':parts, 'columns':runner._terminal_columns}
            with blocks.hold_metadata(parts), blocks.hold_metadata(value):
                retained = blocks.retained_bytes
                with blocks.hold_metadata(value):
                    assert blocks.retained_bytes - retained <= 16
                size, chunks = blocks.manifest_chunks(value)
                encoded = b''.join(chunks)
                assert size == len(encoded)
                assert encoded == document(value, runner.bounds.max_control_bytes)
                # A queued platform control body is live in the same frame;
                # it must not disappear from the snapshot encoder's budget.
                runner._queue.append({'body':'x' * 6000})
                encoded_calls = []
                original = json.JSONEncoder.iterencode
                def encode(*args, **kwargs):
                    encoded_calls.append(True)
                    return original(*args, **kwargs)
                monkeypatch.setattr(json.JSONEncoder, 'iterencode', encode)
                with pytest.raises(DataflowError, match='workspace|resident budget'):
                    blocks.manifest_chunks(value)
                assert not encoded_calls
        finally:
            blocks.close()


def test_index_decode_keeps_the_unfinished_outer_manifest_and_live_control_body(tmp_path):
    from demiflow.execution.dataflow_record_frames import raw_workspace, metadata_bytes
    with store_fixture(tmp_path) as (_, store, _):
        ref, manifest = unfinished(store, columns=12)
        runner = owner(store, ref)
        original = runner._frame_call
        decodes = []
        def frame(function, *args, **kwargs):
            if function.__name__ == 'decode_metadata':
                decodes.append(True)
                if len(decodes) == 2:
                    # One real control body arrives while the first child
                    # index is being decoded. Pick the original grant boundary,
                    # not a giant unrelated object that fails every path.
                    blocks = runner._terminal_blocks
                    outer = blocks._metadata_owners[0]
                    assert outer['columns'] == manifest['columns']
                    assert blocks.retained_bytes >= metadata_bytes(outer)
                    slack = 14336 - blocks.retained_bytes - raw_workspace(args[0], typed=False)
                    assert 0 < slack < metadata_bytes(outer)
                    runner._queue.append({'body':'x' * slack})
            return original(function, *args, **kwargs)
        runner._frame_call = runner._terminal_blocks._frame_call = frame
        try:
            with pytest.raises(DataflowError, match='original resident budget') as failure:
                runner._restore_terminal()
            assert failure.value.context['codec_workspace_bytes'] > 14336
            assert len(decodes) == 2 and not runner._restored_terminal
            assert not runner._terminal_blocks._metadata_owners
        finally:
            runner._terminal_blocks.close()


def test_streamed_manifest_preserves_original_bytes_and_rejects_long_token_before_encoder(tmp_path, monkeypatch):
    import json
    from demiflow.execution.recovery_contract import RecoveryContractError
    with store_fixture(tmp_path) as (_, store, _):
        ref, _ = unfinished(store, columns=2)
        runner = owner(store, ref)
        blocks = runner._terminal_blocks
        value = {'z':None, 'α':['汉字', '🙂', '"\\\n', -0.0, 0.0, 1.25], 'a':True}
        try:
            with blocks.hold_metadata(value):
                size, chunks = blocks.manifest_chunks(value)
                encoded = b''.join(chunks)
                assert encoded == document(value) and size == len(encoded)
            for bad in (float('nan'), float('inf'), float('-inf')):
                with pytest.raises(RecoveryContractError, match='Nonfinite'):
                    blocks.manifest_chunks({'scalar':bad})
            long = {'columns':['x' * 3000]}
            calls = []
            original = json.JSONEncoder.iterencode
            def encode(*args, **kwargs):
                calls.append(True)
                return original(*args, **kwargs)
            monkeypatch.setattr(json.JSONEncoder, 'iterencode', encode)
            with blocks.hold_metadata(long):
                with pytest.raises(DataflowError, match='workspace|resident budget'):
                    blocks.manifest_chunks(long)
            assert not calls
        finally:
            blocks.close()


@pytest.mark.parametrize('blocked', [False, True])
def test_merge_keeps_popped_children_live_before_index_construction(tmp_path, monkeypatch, blocked):
    from demiflow.execution.contracts.dataflow import ArtifactRef
    from demiflow.execution.dataflow_segments import SEGMENTS_MEDIA, block_chunks
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    with store_fixture(tmp_path) as (run, store, _):
        runner = owner(store, None)
        blocks = runner._terminal_blocks
        expected = bytearray()
        original = runner._frame_call
        observed, indices = [], []
        def frame(function, *args, **kwargs):
            if function.__name__ == 'admit_workspace' and args[0] == 768:
                # Both physical entries have left forest.roots, but neither
                # has ceased to exist. The original frame must retain them.
                assert not next(iter(blocks.blocks.values()))[1].roots
                children = blocks._metadata_owners[-1]
                assert len(children) == 2 and all(child['rows'] == 1 for child in children)
                observed.append(blocks.retained_bytes)
                if blocked:
                    slack = 14336 - blocks.retained_bytes - 768
                    assert 0 < slack < 14336
                    runner._queue.append({'body':'x' * slack})
            return original(function, *args, **kwargs)
        runner._frame_call = blocks._frame_call = frame
        put = store.put
        def saving(chunks, *, media_type, **kwargs):
            if media_type == SEGMENTS_MEDIA:
                indices.append(True)
            return put(chunks, media_type=media_type, **kwargs)
        monkeypatch.setattr(store, 'put', saving)
        try:
            for index in range(2):
                row = {'n':index}
                record = make('Record', inline=row, encoded_bytes=len(_encode_payload(row, max_bytes=8192)))
                size, chunks = frame_parts(record, runner.bounds, services=runner.services)
                raw = b''.join(chunks)
                expected.extend(raw)
                blocks.append_parts(record, iter((raw,)), size=size)
                if blocked and index == 1:
                    with pytest.raises(DataflowError, match='original resident budget'):
                        blocks.flush()
                    assert not indices and not blocks._metadata_owners
                    assert not run.journal.execution_commits('action-1')
                    return
                blocks.flush()
            assert observed and indices
            # Subsequent seal is a different operation: its root is already
            # retained, so the pop-specific spy need not apply to it.
            runner._frame_call = blocks._frame_call = original
            part = blocks.manifest_partitions()[0]
            ref = ArtifactRef(**part['data_ref'])
            assert b''.join(block_chunks(store, ref, runner.bounds)) == bytes(expected)
            restored = owner(store, None)._terminal_blocks
            try:
                restored.restore({'rows':2, 'bytes':len(expected), 'partitions':[part]})
                assert restored.manifest_partitions() == [part]
            finally:
                restored.close()
        finally:
            blocks.close()
