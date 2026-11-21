"""Explicit directory maintenance; domain owners interpret their own sources."""
from itertools import chain
import os
import time

from demiflow.forge import operation_budget
from demiflow.forge.schema import ForgeError
from demiflow.forge import project_io as io
from demiflow.workbench.projects import index, workspace
from demiflow.workbench.runs import manager as runs


def synchronize(root):
    errors = []
    with io.control_lock(root):
        operation_budget.check()
        index._ensure(root)
        for name, marker, error in index.pending_markers(root):
            if error is None:
                publish = runs.publish_run_index if marker['kind'] == 'run' else workspace.publish_report_index
                error = publish(root, marker)
            if error:
                errors.append(name + ': ' + error)
    return {**index.status(root), 'errors': errors}


def rebuild(root, *, max_entries=index.MAX_ENTRIES, timeout_s=300):
    if type(max_entries) is not int or not 1 <= max_entries <= index.MAX_ENTRIES or type(timeout_s) is not int or not 1 <= timeout_s <= 300:
        raise ForgeError('Invalid index rebuild budget')
    deadline = time.monotonic() + timeout_s
    current = operation_budget.current_deadline()
    if current is not None:
        deadline = min(deadline, current)
    count = 0
    def check():
        nonlocal count
        count += 1
        if count > max_entries or time.monotonic() >= deadline:
            raise ForgeError('Index rebuild exceeded its entry/time budget')
    with io.control_lock(root):
        run_entries = runs.run_index_entries(root, check=check, deadline=deadline)
        report_entries = workspace.report_index_entries(root, check=check, deadline=deadline)
        try:
            return index.rebuild_from_entries(root, chain(run_entries, report_entries),
                                              max_entries=max_entries, deadline=deadline)
        finally:
            run_entries.close()
            report_entries.close()
