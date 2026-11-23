"""Preserve declared local exception names in the original program payload.

Cloudpickle's dynamic class state omits the type's qualified name on Python
3.11. Catch policies use that full name, so flattening it breaks the worker's
whitelist. Only the already serialized exception class state is augmented;
failure messages remain data-only and never select an import or a constructor.
"""
from tempfile import TemporaryFile
import pickle

import cloudpickle


class ProgramPickler(cloudpickle.CloudPickler):
    def reducer_override(self, value):
        reduced = super().reducer_override(value)
        if (isinstance(value, type) and issubclass(value, Exception)
                and '<locals>' in value.__qualname__
                and isinstance(reduced, tuple) and len(reduced) >= 3):
            state = reduced[2]
            if not (type(state) is tuple and len(state) == 2 and type(state[0]) is dict):
                raise TypeError('Unsupported dynamic exception program state')
            state = ({**state[0], '__qualname__': value.__qualname__}, state[1])
            reduced = (*reduced[:2], state, *reduced[3:])
        return reduced


class _ProgramBuffer:
    """Read a verified buffer without making a second whole-program copy."""
    def __init__(self, buffer):
        self.buffer = buffer
        self.position = 0

    def read(self, size=-1):
        end = len(self.buffer) if size < 0 else min(len(self.buffer), self.position + size)
        value = memoryview(self.buffer)[self.position:end]
        self.position = end
        return value

    def readinto(self, target):
        size = min(len(target), len(self.buffer) - self.position)
        target[:size] = memoryview(self.buffer)[self.position:self.position + size]
        self.position += size
        return size

    def readline(self):
        end = self.buffer.find(b'\n', self.position)
        return self.read(len(self.buffer) - self.position if end < 0 else end + 1 - self.position)


class ProgramUnpickler(pickle.Unpickler):
    """Decode authenticated original source carriers at their current owner.

    Only recorded GLOBAL names and two measured by-value SDK callbacks are
    relocated. Original pickle bytes remain intact; there are no import hooks
    or global aliases. Ordinary business functions use cloudpickle unchanged.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._native_states = {}

    def load(self):
        try:
            result = super().load()
            if any(self._native_states.values()):
                raise pickle.UnpicklingError('Native callback state was not verified')
            return result
        finally:
            self._native_states.clear()

    def _make_function(self, code, globals, name, argdefs, closure):
        from demiflow.execution.recovery_sdk_v3 import native_pickle_function
        from cloudpickle.cloudpickle import _make_function
        native = native_pickle_function(code, globals, name, argdefs, closure)
        if native is None:
            return _make_function(code, globals, name, argdefs, closure)
        value, expected = native
        self._native_states.setdefault(value, []).append(expected)
        return value

    def _function_setstate(self, value, state):
        from cloudpickle.cloudpickle import _function_setstate
        from demiflow.execution.recovery_contract import document, RecoveryContractError
        if value not in self._native_states:
            from types import FunctionType
            from demiflow.execution.contracts.recovery_sdk_v3 import LAYOUT_RELOCATIONS
            if (type(value) is FunctionType and type(value.__module__) is str
                    and type(value.__qualname__) is str):
                entry = LAYOUT_RELOCATIONS.get(value.__module__ + '.' + value.__qualname__, {})
                if entry.get('by_value'):
                    import sys
                    module = sys.modules.get(value.__module__)
                    if module is not None and vars(module).get(entry['name']) is value:
                        raise pickle.UnpicklingError('Canonical native callback has no pending state')
            return _function_setstate(value, state)
        pending = self._native_states[value]
        if (type(state) is not tuple or len(state) != 2
                or type(state[0]) is not dict or type(state[1]) is not dict):
            raise pickle.UnpicklingError('Invalid native callback state')
        try:
            actual = document(state)
        except RecoveryContractError as exc:
            raise pickle.UnpicklingError('Native callback state has non-passive values') from exc
        for index, expected in enumerate(pending):
            if actual == document(expected):
                pending.pop(index)
                return None  # Never write old state into the canonical function.
        raise pickle.UnpicklingError('Native callback state differs from its recorded definition')

    def find_class(self, module, name):
        if module == 'cloudpickle.cloudpickle':
            if name == '_make_function':
                return self._make_function
            if name == '_function_setstate':
                return self._function_setstate
        from demiflow.execution.recovery_sdk_v3 import source_pickle_global, authentic_source_global
        target = source_pickle_global(module, name)
        if target is None:
            return super().find_class(module, name)
        value = super().find_class(*target)
        if not authentic_source_global(value, module):
            raise pickle.UnpicklingError('Relocated source declaration differs from its audited definition')
        return value


def stage_payload(value, *, store, media_type, allow_live=False,
                  serialization_error="Program carries a live handle (connection/lock/FD); "
                                      "task/process scope requires a serializable declaration"):
    """Serialize once. Store limits apply to actual bytes written."""
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with TemporaryFile() as stream:
        try:
            ProgramPickler(stream).dump(value)
        except (OSError, MemoryError, DataflowError):
            raise
        except Exception as exc:
            if allow_live:
                return None
            raise DataflowError("INVALID_CONTRACT", serialization_error + ": " + str(exc)) from exc
        size = stream.tell()
        stream.seek(0)
        return store.put(iter(lambda: stream.read(65536), b""), max_bytes=size, media_type=media_type)


def load_payload(chunks):
    """Finish verification before executing any pickle opcode."""
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with TemporaryFile() as stream:
        stream.writelines(chunks)
        stream.seek(0)
        try:
            return ProgramUnpickler(stream).load()
        except Exception as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "Program payload failed to load: " + str(exc)) from exc


def load_private_payload(store, ref):
    """Verify the complete sealed input before running its first pickle opcode.

    Reads into the same buffer for every storage window, including tighter
    live windows when resuming a previously admitted stage.
    """
    import hashlib
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    buffer = bytearray(ref.size_bytes)
    store.read_into(ref, buffer)
    if hashlib.sha256(buffer).hexdigest() != ref.digest:
        raise DataflowError('CORRUPT_ARTIFACT', 'Private task program digest differs')
    try:
        return ProgramUnpickler(_ProgramBuffer(buffer)).load()
    except Exception as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'Program payload failed to load: ' + str(exc)) from exc
