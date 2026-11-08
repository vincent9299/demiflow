"""Temporary graph-capture optimizations; native reference functions stay intact.

The worker owns one model and drains every GPU reader before entering here.
Only known Qwen3.5 modules are considered. Unsupported implementations retain
their native operations. Importing this module never imports a GPU runtime.
"""
from contextlib import contextmanager
import inspect
import sys


def _torch_convolution(function):
    # Transformers freezes the selected implementation in this closure. Do not
    # replace an installed fused kernel with a different rounding convention.
    try:
        values = inspect.getclosurevars(function).nonlocals
    except TypeError:
        return False
    return (values.get('is_new_implementation') is False
            and values.get('implementation') is values.get('torch_function')
            and callable(values.get('torch_function')))


@contextmanager
def capture_kernels(model, gate):
    if not gate.writing or gate.readers:
        raise RuntimeError('Capture kernels require exclusive GPU ownership')
    qwen = sys.modules.get('transformers.models.qwen3_5.modeling_qwen3_5')
    if qwen is None:
        yield
        return
    types = {getattr(qwen, name, None): kind for name, kind in (
        ('Qwen3_5RMSNorm', 'plain'), ('Qwen3_5RMSNormGated', 'gated'),
        ('Qwen3_5GatedDeltaNet', 'convolution'))}
    selected = []
    # This inspection has a fixed bound, independent of request count/length.
    for index, module in enumerate(model.modules()):
        if index >= 10000 or len(selected) >= 1024:
            yield  # Unrecognized/oversized architecture: capture native ops.
            return
        if type(module) in types: selected.append((module, types[type(module)]))
    if not selected:
        yield
        return
    if __package__:
        from . import pointwise_cuda as kernels
    else:
        import pointwise_cuda as kernels
    original_conv = qwen.causal_conv1d_fn
    replace_conv = any(kind == 'convolution' for _,kind in selected) and _torch_convolution(original_conv)
    restore = []
    try:
        for module, kind in selected:
            if kind == 'convolution': continue
            own = 'forward' in module.__dict__
            original = module.forward
            restore.append((module, own, original))
            module.forward = kernels.normalization(module, original, gated=kind == 'gated')
        if replace_conv: qwen.causal_conv1d_fn = kernels.convolution(original_conv, qwen)
        yield
    finally:
        if replace_conv: qwen.causal_conv1d_fn = original_conv
        for module, own, original in reversed(restore):
            if own: module.forward = original
            elif 'forward' in module.__dict__: del module.forward
