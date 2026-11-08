"""CPU-only reproducer for Torch's borrowed pool-ID reference bug.

Run in the target model interpreter. No model files or CUDA device are used.
Extra references keep the integers alive; compensate for the observed invalid
decref so the reproducer itself does not leave a corrupted reference count.
"""
import ctypes
import json
import os
import sys

os.environ['CUDA_VISIBLE_DEVICES'] = ''
import torch


def main():
    first, second = int('1000000007'), int('1000000009')
    keepalive = [first, second] * 8
    argument = (first, second, False)
    before = (sys.getrefcount(first), sys.getrefcount(second))
    result = torch._C._cuda_memorySnapshot(argument)
    after = (sys.getrefcount(first), sys.getrefcount(second))
    delta = tuple(b - a for a, b in zip(before, after))
    # PyDLL retains the GIL. This compensates only the measured missing refs,
    # never patches Torch or changes the application interpreter's behavior.
    incref = ctypes.pythonapi.Py_IncRef
    incref.argtypes = [ctypes.py_object]
    incref.restype = None
    for value, change in zip((first, second), delta):
        if change not in (0, -1): raise RuntimeError('Unexpected reference-count change')
        if change == -1: incref(value)
    assert len(keepalive) == 16 and not result['segments']
    print(json.dumps({'torch': torch.__version__, 'git': torch.version.git_version,
        'refcount_before': before, 'refcount_after': after, 'delta': delta,
        'borrowed_reference_bug': delta == (-1, -1), 'cuda_used': False}))


if __name__ == '__main__': main()
