"""Cooperative readers of original, authenticated finite input artifacts.

Only immutable references and consumed frame ordinals enter the existing worker
frontier. The handoff ACK means the worker adopted those references, not that it
consumed them. Output/actor state and this cursor commit in the same native cut.
"""
import sys
import time

from ..contracts.dataflow import ArtifactRef
from ..contracts.dataflow_errors import DataflowError
from ..dataflow_finite_input import validate_finite_members
from ..dataflow_record_frames import (
    decode_document, decode_frame, frame_stream, line_limit, read_chunk_bytes,
    admit_workspace, metadata_bytes,
)
from ..dataflow_segments import block_chunks
from ..dataflow_sinks import _artifact_json
from ..recovery_contract import document


def _metadata_ceiling(states, entries):
    # Cursor integers can grow or stop aliasing the initial zero / total_rows.
    # Charge one independent maximum-sized integer per producer, even when
    # identity-based measurement currently counts a shared integer only once.
    integers = sum(sys.getsizeof(entry['total_rows'])
                   for members in entries.values() for entry in members)
    return metadata_bytes((states, entries)) + integers + sys.getsizeof((None, None))


class FiniteInputDrain:
    def __init__(self, runner):
        self.runner = runner
        self._states = {}
        self._entries = {}
        self._active = None
        self._active_entry = None
        self._stream = None
        self._scanned = 0
        self._index_bytes = 0
        self._metadata_bytes = 0

    @property
    def buffered_bytes(self):
        return self._stream.buffered_bytes if self._stream is not None else 0

    @property
    def retained_bytes(self):
        return self.buffered_bytes + self._index_bytes + self._metadata_bytes

    def _retain_index(self, size):
        self._index_bytes = size

    def _edge(self, edge_id):
        runner = self.runner
        spec = runner._exchange.get(edge_id)
        edge = next((e for e in runner.e._graph.edges if e.edge_id == edge_id), None)
        if (spec is None or spec.mode != 'finite_sealed' or edge is None
                or edge.consumer not in runner.e._node_specs
                or runner.partition not in spec.consumer_partitions):
            raise DataflowError('FENCED', 'finite read frontier differs from assigned input')
        return edge, spec

    def adopt(self, edge_id, seal_ref, *, restored=None):
        runner = self.runner
        _, spec = self._edge(edge_id)
        old = self._states.get(edge_id)
        if old is not None:
            if old['seal_ref'] != _artifact_json(seal_ref):
                raise DataflowError('FENCED', 'finite read input changed after adoption')
            return
        # Authenticate every producer, route and record before ACK or invoking
        # business code. Reuse the existing bounded finite-reference validator.
        entries = validate_finite_members(runner.services, seal_ref, spec, target=runner.partition,
                                         frame_call=runner._frame_call)
        # Old frontiers have no partial read cursor: the old atomic consumer
        # either finished the entire producer or committed no consumption.
        positions = {entry['producer']: (entry['total_rows']
            if (edge_id, entry['producer']) in runner._finished_inputs else 0) for entry in entries}
        if restored is not None:
            if (type(restored) is not dict or set(restored) != set(positions)
                    or any(type(restored[e['producer']]) is not int
                           or not 0 <= restored[e['producer']] <= e['total_rows'] for e in entries)):
                raise DataflowError('FENCED', 'finite read cursor exceeds its original sealed input')
            positions = dict(restored)
            for entry in entries:
                if ((edge_id, entry['producer']) in runner._finished_inputs
                        and positions[entry['producer']] != entry['total_rows']):
                    raise DataflowError('FENCED', 'completed finite input has an unfinished cursor')
        state = {'seal_ref': _artifact_json(seal_ref), 'positions': positions}
        states = {**self._states, edge_id: state}
        members = {**self._entries, edge_id: entries}
        ceiling = _metadata_ceiling(states, members)
        from ..dataflow_record_frames import _json_shape
        size, workspace = _json_shape(states, runner.bounds.max_state_bytes)
        # The original grant must also hold the new conservative ceiling.
        # Both old map containers survive until replacement; their children
        # alias the candidate and are already included in its measured tree.
        growth = max(0, ceiling - self._metadata_bytes)
        growth += sys.getsizeof(self._states) + sys.getsizeof(self._entries)
        runner._frame_call(admit_workspace,
            max(workspace + 2 * size + metadata_bytes(entries), growth), runner.bounds)
        document(states, runner.bounds.max_state_bytes)
        self._states, self._entries, self._metadata_bytes = states, members, ceiling

    def restore(self, value):
        if type(value) is not dict or len(value) > self.runner.bounds.max_edges:
            raise DataflowError('FENCED', 'invalid finite read frontier')
        for edge_id, state in value.items():
            if (type(edge_id) is not str or type(state) is not dict
                    or set(state) != {'seal_ref', 'positions'}):
                raise DataflowError('FENCED', 'invalid finite read frontier entry')
            try:
                ref = ArtifactRef(**state['seal_ref'])
            except (TypeError, ValueError) as exc:
                raise DataflowError('FENCED', 'invalid finite read seal reference') from exc
            self.adopt(edge_id, ref, restored=state['positions'])

    def snapshot(self):
        return {edge: {'seal_ref': dict(state['seal_ref']), 'positions': dict(state['positions'])}
                for edge, state in self._states.items()}

    def _next_member(self):
        if self._active_entry is not None:
            edge_id, entry = self._active_entry
            if (edge_id, entry['producer']) not in self.runner._finished_inputs:
                return self._active_entry
        for edge_id, entries in self._entries.items():
            for entry in entries:
                if (edge_id, entry['producer']) not in self.runner._finished_inputs:
                    return edge_id, entry
        return None

    def step(self, *, max_records, deadline_ms):
        runner = self.runner
        deadline = time.monotonic() + deadline_ms / 1000
        processed = 0
        while processed < max_records and time.monotonic() < deadline:
            if runner._cut is not None or runner._paused and not runner._park_requested:
                break
            runner.services.check_stop()
            current = self._next_member()
            if current is None:
                break
            edge_id, entry = current
            edge, _ = self._edge(edge_id)
            key = (edge_id, entry['producer'])
            position = self._states[edge_id]['positions']
            if edge.consumer in runner._finished or edge_id in runner._closed_inputs:
                self.close()
                position[entry['producer']] = entry['total_rows']
                runner._edge_eoi(*key)
                processed += 1
                continue
            if runner._park_mode and not runner._park_can_output(edge.consumer):
                runner._paused = True
                break
            if self._active != key:
                self.close()
                self._active = key
                self._active_entry = current
                self._stream = frame_stream(block_chunks(runner.services.store,
                    entry['manifest_ref'], runner.bounds,
                    chunk_bytes=read_chunk_bytes(runner.bounds, runner.services),
                    frame_call=runner._frame_call, on_retained=self._retain_index),
                    runner.bounds, services=runner.services,
                    check_workspace=lambda size: runner._frame_call(admit_workspace, size,
                        runner.bounds, _frame_reader_growth=True))
            line = self._stream.readline(line_limit(runner.bounds))
            if not line:
                if self._scanned != entry['total_rows']:
                    raise DataflowError('CORRUPT_ARTIFACT', 'finite producer row count differs from seal')
                self.close()
                runner._edge_eoi(*key)
                processed += 1
                continue
            raw, item = runner._frame_call(decode_frame, line, runner.bounds, finite=True)
            del line
            self._scanned += 1
            if self._scanned > entry['total_rows']:
                raise DataflowError('CORRUPT_ARTIFACT', 'finite producer exceeds sealed row count')
            # Restart scans only the already committed prefix, without invoking
            # its consumer again. Even skipped/foreign-target frames consume a
            # step quantum; no replay scan blocks control for an entire input.
            advancing = self._scanned > position[entry['producer']]
            record = None
            if advancing:
                if item['target'] == runner.partition:
                    record = runner._frame_call(decode_document, raw,
                        record_id=item['record_id'], sequence=item['sequence'], bounds=runner.bounds)
            # No second serialized input copy survives into consumer encoding.
            del raw, item
            if record is not None:
                runner._push(edge.consumer, edge.input_port, record)
                del record
            if advancing:
                position[entry['producer']] = self._scanned
            processed += 1
            if self._scanned == entry['total_rows']:
                # Authenticate the original reader's EOF before recording EOI.
                # This is not another business row or unbounded read-ahead.
                if self._stream.readline(line_limit(runner.bounds)):
                    raise DataflowError('CORRUPT_ARTIFACT', 'finite producer exceeds sealed row count')
                self.close()
                runner._edge_eoi(*key)
            if runner._park_requested:
                pending = self._next_member()
                if pending is not None:
                    following, _ = self._edge(pending[0])
                    runner._park_finish_pending = runner._park_can_output(following.consumer)
        return processed

    def close(self):
        if self._stream is not None:
            self._stream.close()
        self._stream = self._active = None
        self._active_entry = None
        self._scanned = 0
        self._index_bytes = 0
