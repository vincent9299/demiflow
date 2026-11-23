"""Arrow cast admission shared by native writers and sealed cast callbacks."""


def _cast_allocation(table, schema):
    """Upper bound on new payload buffers, while the source remains resident.

    Equal types share their buffers. Reserve data and validity allocations at
    Arrow's 64-byte alignment for conversions, including nested arrays. Unknown
    allocation shapes fail before cast, never after an unbounded allocation.
    This is payload admission, not an RSS bound on Arrow's implementation.
    """
    import pyarrow as pa
    from .recovery_contract import RecoveryContractError
    def aligned(size):
        return ((size + 63) // 64) * 64
    def bound(array, target, depth=0):
        if array.type == target:
            return 0
        if depth > 16:
            raise RecoveryContractError('Arrow cast type depth exceeds admission budget')
        n = len(array)
        validity = aligned((n + 7) // 8)
        if pa.types.is_null(target):
            return 0
        try:
            bits = target.bit_width
        except (ValueError, AttributeError):
            bits = None
        if bits is not None:
            dictionary = (bound(array.dictionary, target, depth + 1)
                          if pa.types.is_dictionary(array.type) else 0)
            return validity + aligned((n * bits + 7) // 8) + dictionary
        if pa.types.is_struct(target) and pa.types.is_struct(array.type):
            if array.type.names != target.names:
                raise RecoveryContractError('Arrow cast field mapping has no bounded allocation contract')
            return validity + sum(bound(array.field(i), f.type, depth + 1)
                                  for i, f in enumerate(target))
        if (pa.types.is_list(target) or pa.types.is_large_list(target)) and (
                pa.types.is_list(array.type) or pa.types.is_large_list(array.type)):
            width = 8 if pa.types.is_large_list(target) else 4
            return validity + aligned((n + 1) * width) + bound(array.values, target.value_type, depth + 1)
        binary = (pa.types.is_string, pa.types.is_large_string, pa.types.is_binary, pa.types.is_large_binary)
        if any(check(target) for check in binary):
            width = 8 if pa.types.is_large_string(target) or pa.types.is_large_binary(target) else 4
            if any(check(array.type) for check in binary) or pa.types.is_fixed_size_binary(array.type):
                data = array.get_total_buffer_size()
            elif (pa.types.is_integer(array.type) or pa.types.is_floating(array.type)
                  or pa.types.is_boolean(array.type) or pa.types.is_decimal(array.type)):
                data = n * 128  # bounded textual representation of primitive values
            else:
                raise RecoveryContractError('Arrow cast needs a bounded allocation contract for ' + str(target))
            return validity + aligned((n + 1) * width) + aligned(data)
        raise RecoveryContractError('Arrow cast needs a bounded allocation contract for ' + str(target))
    if table.schema.names != schema.names:
        raise RecoveryContractError('Arrow cast schema fields differ')
    return sum(bound(chunk, field.type) for column, field in zip(table.columns, schema)
               for chunk in column.chunks)
