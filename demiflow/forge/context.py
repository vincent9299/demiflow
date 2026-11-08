"""Read-only project facts for CLI and product consumers.

Saved engineering evidence is not current execution authority. This projection
never creates tasks/runs, refreshes baselines, or writes reading receipts.
"""
from pathlib import Path
import time

from .development.history import Budget, view
from .io import no_symlinks
from .platform import bounded_json, content_digest
from .schema import ForgeError

MAX_BYTES = 65536


def query(project, *, task=None):
    project = no_symlinks(Path(project).absolute())
    if task is not None and (type(task) is not str or not task):
        raise ForgeError('Invalid task selector', code='invalid_request')
    budget = Budget()
    facts = []
    contract = no_symlinks(project / 'contract.lock.json')
    if contract.exists():
        value = budget.json(contract)
        facts.append(dict(kind='saved_contract', source=str(contract),
                          version=content_digest(value), observed_at=time.time(), value=value))
    if task is not None:
        value = view(project, task_id=task)
        budget.check()
        facts.append(dict(kind='saved_development_task', source=str(project / '.demiforge/tasks' / task),
                          version=content_digest(value), observed_at=time.time(), value=value))
    result = dict(format='demiflow_forge_project_facts_v1', project=str(project), read_only=True,
                  observed_at=time.time(), atomic_snapshot=False, facts=facts,
                  scope=['saved_contract', 'selected_development_task'],
                  current_evidence='unverified', budget=dict(output_bytes=MAX_BYTES, seconds=10),
                  notice='Saved facts only; no execution, refresh, validation or reading receipts.')
    bounded_json(result, MAX_BYTES)
    budget.check()
    return result
