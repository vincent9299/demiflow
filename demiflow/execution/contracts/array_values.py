"""Data-only NumPy values; object/structured arrays never carry Python code."""
def numpy_value(value):
    if type(value).__module__.split('.')[0] != 'numpy':
        return None
    import numpy as np
    if type(value) is np.ndarray or isinstance(value, np.generic):
        return value
    return None


def validate_array(value):
    if (value.dtype.hasobject or value.dtype.fields is not None
            or value.dtype.kind not in 'biufcSUVmM' or value.dtype.itemsize <= 0):
        raise ValueError('Only primitive data-only NumPy dtypes are supported')


def freeze_array(value):
    import numpy as np
    validate_array(value)
    if isinstance(value, np.generic):
        return value
    # A read-only ndarray alone can be made writable again by its owner.
    # Back it with immutable bytes to detach the caller and forbid that alias.
    return np.frombuffer(value.tobytes(order='C'), dtype=value.dtype).reshape(value.shape)
