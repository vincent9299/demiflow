"""Preserve declared local exception names in the original program payload.

Cloudpickle's dynamic class state omits the type's qualified name on Python
3.11. Catch policies use that full name, so flattening it breaks the worker's
whitelist. Only the already serialized exception class state is augmented;
failure messages remain data-only and never select an import or a constructor.
"""
from tempfile import TemporaryFile

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


def stage_payload(value, *, store, media_type, allow_live=False,
                  serialization_error="Program carries a live handle (connection/lock/FD); "
                                      "task/process scope requires a serializable declaration"):
    """Serialize once. Store limits apply to actual bytes written."""
    from .contracts.dataflow_errors import DataflowError
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
    from .contracts.dataflow_errors import DataflowError
    with TemporaryFile() as stream:
        stream.writelines(chunks)
        stream.seek(0)
        try:
            return cloudpickle.load(stream)
        except Exception as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "Program payload failed to load: " + str(exc)) from exc
