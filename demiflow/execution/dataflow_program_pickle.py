"""Preserve declared local exception names in the original program payload.

Cloudpickle's dynamic class state omits the type's qualified name on Python
3.11. Catch policies use that full name, so flattening it breaks the worker's
whitelist. Only the already serialized exception class state is augmented;
failure messages remain data-only and never select an import or a constructor.
"""
import io

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


def dumps(value):
    stream = io.BytesIO()
    ProgramPickler(stream).dump(value)
    return stream.getvalue()
