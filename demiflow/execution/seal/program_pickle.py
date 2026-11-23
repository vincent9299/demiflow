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
        value = bytes(memoryview(self.buffer)[self.position:end])
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

    Only the bounded recorded GLOBAL names are relocated. Old files and
    pickle bytes remain intact; there are no import hooks or global aliases.
    """
    def find_class(self, module, name):
        from demiflow.execution.recovery_sdk_v3 import source_pickle_global, persistent_source_module
        target = source_pickle_global(module, name)
        if target is None:
            return super().find_class(module, name)
        value = super().find_class(*target)
        if persistent_source_module(value) != module:
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
