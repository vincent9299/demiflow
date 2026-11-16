"""Cooperative readers of original, authenticated finite input artifacts.

Only immutable references and consumed frame ordinals enter the existing worker
frontier. The handoff ACK means the worker adopted those references, not that it
consumed them. Output/actor state and this cursor commit in the same native cut.
"""
import time

from ..contracts.dataflow import ArtifactRef
from ..contracts.dataflow_errors import DataflowError
from ..dataflow_finite_input import validate_finite_members
from ..dataflow_record_frames import (
    decode_document, decode_frame, frame_stream, line_limit, read_chunk_bytes,
)
from ..dataflow_segments import block_chunks
from ..dataflow_sinks import _artifact_json
from ..recovery_contract import document


class FiniteInputDrain:
    def __init__(self, runner):
        self.runner = runner
        self._states = {}
        self._entries = {}
        self._active = None
        self._stream = None
        self._scanned = 0

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
        entries = validate_finite_members(runner.services, seal_ref, spec, target=runner.partition)
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
        document({**self._states, edge_id: state}, runner.bounds.max_state_bytes)
        self._states[edge_id] = state
        self._entries[edge_id] = entries

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
                self._stream = frame_stream(block_chunks(runner.services.store,
                    entry['manifest_ref'], runner.bounds,
                    chunk_bytes=read_chunk_bytes(runner.bounds, runner.services)),
                    runner.bounds, services=runner.services)
            line = self._stream.readline(line_limit(runner.bounds))
            if not line:
                if self._scanned != entry['total_rows']:
                    raise DataflowError('CORRUPT_ARTIFACT', 'finite producer row count differs from seal')
                self.close()
                runner._edge_eoi(*key)
                processed += 1
                continue
            raw, item = runner._frame_call(decode_frame, line, runner.bounds, finite=True,
                _frame_retained=self._stream.buffered_bytes)
            del line
            self._scanned += 1
            if self._scanned > entry['total_rows']:
                raise DataflowError('CORRUPT_ARTIFACT', 'finite producer exceeds sealed row count')
            # Restart scans only the already committed prefix, without invoking
            # its consumer again. Even skipped/foreign-target frames consume a
            # step quantum; no replay scan blocks control for an entire input.
            if self._scanned > position[entry['producer']]:
                if item['target'] == runner.partition:
                    record = runner._frame_call(decode_document, raw,
                        record_id=item['record_id'], sequence=item['sequence'], bounds=runner.bounds,
                        _frame_retained=self._stream.buffered_bytes)
                    runner._push(edge.consumer, edge.input_port, record)
                    del record
                position[entry['producer']] = self._scanned
            del raw, item
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
        self._scanned = 0
