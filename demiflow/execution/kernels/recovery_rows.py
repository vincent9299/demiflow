"""Bounded actor recovery inputs and output cursors in the original kernel image.

The hook supplies INPUTS to its original actor. Owned spool refs, state and
output position are checkpointed together by the existing coordinator.
"""
import inspect
from itertools import islice

from ..contracts.dataflow import Record
from ..contracts.dataflow_errors import DataflowError
from .dataflow import _hash_id, _row_bytes


def hook_owner(kernel):
    if kernel._shared_actor is not None and kernel._occurrence != kernel._shared_actor.binding['owner']:
        return None
    instance = kernel._instance
    if instance is not None:
        owner = kernel._state_owner(instance)
        return owner if callable(getattr(owner, 'recovery_rows', None)) else None
    # Class declarations remain lazy unless they explicitly declare this hook.
    program = kernel._services.programs.get(kernel._spec.configuration.get('program'))
    target = getattr(getattr(program, 'value', None), 'target', None)
    if inspect.isclass(target) and inspect.getattr_static(target, 'recovery_rows', None) is not None:
        return target
    return None


class RecoveryRows:
    def __init__(self, kernel):
        from .operator import _SpoolPort, _segment_rows
        self.kernel = kernel
        self.inputs = _SpoolPort('actor-recovery-inputs', kernel._services, _segment_rows(kernel._services))
        # A callback's output is staged before publishing its prefix. One-row
        # output segments keep it inside the original input+output windows.
        self.outputs = _SpoolPort('actor-recovery-outputs', kernel._services, 1)
        self.input_cursor = self.output_cursor = 0
        self.prepared = self.finalized = False

    def _put_output(self, record):
        bounds = self.kernel._services.bounds
        if self.outputs._count >= bounds.max_output_rows:
            raise DataflowError('BUDGET_EXCEEDED', 'recovery callback exceeds original output row bound')
        self.outputs.push(record)

    def prepare(self):
        kernel = self.kernel
        instance = kernel._slot_instance(0)
        owner = kernel._state_owner(instance)
        values = instance.resolve(owner.recovery_rows())
        iterator = iter(()) if values is None else iter(values)
        ordinal = 0
        try:
            for value in iterator:
                kernel._services.check_stop()
                # The old batch actor hook permits lists of input rows.
                rows = value if kernel._spec.kind in {'BatchMapOp', 'MapBatchesOp'} and type(value) is list else (value,)
                for row in rows:
                    if ordinal >= kernel._services.bounds.max_output_rows:
                        raise DataflowError('BUDGET_EXCEEDED', 'recovery inputs exceed original row bound')
                    amount = _row_bytes(row, max_bytes=kernel._services.bounds.max_row_bytes)
                    self.inputs.push(Record(record_id=_hash_id(['actor-recovery-v1', kernel._occurrence, ordinal]),
                        source_position={'actor_recovery': {'occurrence': kernel._occurrence, 'ordinal': ordinal}},
                        inline=row, payload_ref=None, encoded_bytes=amount, sequence=ordinal, schema_ref=None))
                    ordinal += 1
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                instance.resolve(close())
        self.inputs.finish()
        self.prepared = True

    def _drain_call(self, iterator):
        kernel = self.kernel
        kernel._recovery_buffering = True
        try:
            for record in iterator:
                self._put_output(record)
            while kernel.pending_calls:
                kernel._services.check_stop()
                for record in kernel.poll():
                    self._put_output(record)
                if kernel.pending_calls:
                    import time
                    time.sleep(.001)
        finally:
            kernel._recovery_buffering = False
        self.outputs.finish()

    def finish(self, port):
        kernel = self.kernel
        if not self.prepared:
            self.prepare()
        inputs = iter(islice(self.inputs.records(), self.input_cursor, None))
        while True:
            for record in islice(self.outputs.records(), self.output_cursor, None):
                self.output_cursor += 1
                yield from kernel._emit(iter((record,)))
            self.outputs.release()
            from .operator import _SpoolPort
            self.outputs = _SpoolPort('actor-recovery-outputs', kernel._services, 1)
            self.output_cursor = 0
            if self.finalized:
                return
            try:
                record = next(inputs)
            except StopIteration:
                self._drain_call(kernel._finish_normal(port, wait_for_calls=True))
                self.finalized = True
            else:
                self._drain_call(kernel.push(port, record))
                self.input_cursor += 1

    def snapshot(self):
        return {'version': 1, 'inputs': self.inputs.snapshot(), 'outputs': self.outputs.snapshot(),
                'input_cursor': self.input_cursor, 'output_cursor': self.output_cursor,
                'prepared': self.prepared, 'finalized': self.finalized}

    def restore(self, value):
        expected = {'version', 'inputs', 'outputs', 'input_cursor', 'output_cursor', 'prepared', 'finalized'}
        if (type(value) is not dict or set(value) != expected or type(value['version']) is not int or value['version'] != 1
                or value['prepared'] is not True or type(value['finalized']) is not bool):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Invalid actor recovery-row image')
        self.inputs.restore(value['inputs']); self.outputs.restore(value['outputs'])
        for name, maximum in (('input_cursor', self.inputs._count), ('output_cursor', self.outputs._count)):
            if type(value[name]) is not int or not 0 <= value[name] <= maximum:
                raise DataflowError('CORRUPT_ARTIFACT', 'Invalid actor recovery-row cursor')
            setattr(self, name, value[name])
        if self.inputs._count > self.kernel._services.bounds.max_output_rows or self.outputs._count > self.kernel._services.bounds.max_output_rows:
            raise DataflowError('BUDGET_EXCEEDED', 'Recovered actor rows exceed original row bound')
        if not self.inputs.eoi or value['finalized'] and self.input_cursor != self.inputs._count:
            raise DataflowError('CORRUPT_ARTIFACT', 'Actor recovery-row image is not a completed input spool')
        self.prepared, self.finalized = True, value['finalized']

    def close(self):
        errors = []
        for port in (self.inputs, self.outputs):
            try:
                port.release()
            except BaseException as exc:
                errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup('actor recovery-row cleanup failed', errors)
