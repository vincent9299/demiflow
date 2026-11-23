"""Block partition implementation."""
from __future__ import annotations

import hashlib
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import canonical_record_json
from demiflow.operators.ordering import shuffle_key_bytes


def block_count(configuration, *, rows, max_partitions):
    count = configuration.get('num_blocks')
    if count is None:
        target = configuration.get('target_num_rows_per_block')
        count = max(1, (rows + target - 1) // target) if target is not None else 1
    if type(count) is not int or not 1 <= count <= max_partitions:
        raise DataflowError('BUDGET_EXCEEDED', 'logical block count exceeds max_partitions')
    return count

def block_index(*, key, ordinal, rows, count, target):
    if key is not None:
        index = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big') % count
    elif target is not None:
        index = ordinal // target
    else:
        # Balanced contiguous blocks: first remainder blocks have one extra.
        size, remainder = divmod(rows, count)
        prefix = (size + 1) * remainder
        index = (ordinal // (size + 1) if ordinal < prefix
                 else remainder + (ordinal - prefix) // size)
    return index

def collect_block_identities(entries, *, max_partitions):
    blocks = set()
    layouts = set()
    for tag, identity in entries:
        if tag is not None and (tag[0], tag[2]) not in layouts:
            occurrence_id, _, count = tag
            if count > max_partitions:
                raise DataflowError('BUDGET_EXCEEDED', 'logical block count exceeds bound')
            layouts.add((occurrence_id, count))
            blocks.update(canonical_record_json(['logical', occurrence_id, i]) for i in range(count))
        else:
            blocks.add(canonical_record_json(['logical', tag[0], tag[1]]) if tag is not None else identity)
        if len(blocks) > max_partitions:
            raise DataflowError('BUDGET_EXCEEDED', 'randomized block metadata exceeds bound')
    return blocks

def shuffled_layout(blocks, *, seed, occurrence_id):
    ordered = sorted(blocks, key=lambda key: (shuffle_key_bytes(key, seed=seed, occurrence_id=occurrence_id), key))
    return {key: i for i, key in enumerate(ordered)}
