"""Explicit directory maintenance; delegates directly to Forge project index operations."""
from demiflow.forge import index
from demiflow.forge.runs import manager as runs


def synchronize(root):
    return runs.synchronize_index(root)


def rebuild(root, *, max_entries=index.MAX_ENTRIES, timeout_s=300):
    return runs.rebuild_index(root, max_entries=max_entries, timeout_s=timeout_s)
