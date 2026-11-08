"""Pure, bounded operational context from domain-owned run facts.

Development preparation/reading receipts remain in development_chat.context.
This first consumer covers run identity/state/log pages and tool contracts;
Notebook, SQL, Case and mandatory engineering documents are not silently
assembled or claimed complete. They retain their existing explicit interfaces.
"""
from pathlib import Path
import time

from demiflow.forge.platform import bounded_json
from demiflow.workbench.runs.authority import LocalAuthority
from demiflow.forge.schema import ForgeError

VERSION = 'demiflow_workbench_run_context_v1'
MAX_BYTES = 65536
MAX_READS = 8
MAX_REFERENCES = 32
MAX_SECONDS = 10


def query(root, run_id, *, attempt=None, logs=False, offset=0, max_bytes=MAX_BYTES, authority=None):
    if (type(logs) is not bool or type(offset) is not int or not 0 <= offset < 2**64
            or type(max_bytes) is not int or not 4096 <= max_bytes <= MAX_BYTES):
        raise ForgeError('Invalid context budget or log selector', code='invalid_request')
    from demiflow.workbench.runs import view as run_view
    from demiflow.workbench.runs import manager as run_manager
    from demiflow.workbench.runs import tools
    from demiflow.forge import operation_budget
    root = Path(root).absolute()
    authority = authority if authority is not None else LocalAuthority(str(root))
    deadline = time.monotonic() + MAX_SECONDS
    response = dict(format=VERSION, owner='project_run', observed_at=time.time(), atomic_snapshot=False,
                    facts=[], references=[], diagnostics=[], coverage=['run_identity', 'run_state'],
                    excluded=['engineering_document_delivery', 'notebook', 'sql', 'case', 'development_receipts'],
                    budget=dict(bytes=max_bytes, reads=MAX_READS, references=MAX_REFERENCES, seconds=MAX_SECONDS),
                    reads=0, complete=False)

    def check():
        if time.monotonic() >= deadline or response['reads'] >= MAX_READS:
            raise ForgeError('Context deadline/read budget exceeded', code='resource_limit')
        operation_budget.check()
        authority.require(root, 'read')

    with operation_budget.scope(deadline=deadline):
        check()
        facts = run_view.context_facts(root, run_id, attempt=attempt, deadline=deadline)
        response['reads'] += 1
        ref = dict(owner='pipeline_run', kind='run_attempt', run_id=run_id, attempt=facts['attempt'],
                   version=facts['state']['revision'], seal_digest=facts['seal_digest'],
                   source_format=facts['source_format'], observed_at=time.time(), historical=facts['historical'])
        response['references'].append(ref)
        response['facts'].append(dict(source=0, value=facts))
        # All-or-nothing sections: never truncate a mandatory contract and call
        # it complete. The full tool contract has its own explicit read entry.
        descriptions = tools.describe()
        if len(bounded_json(descriptions, 24576)) + len(bounded_json(response, MAX_BYTES)) + 1024 <= max_bytes:
            response['tools'] = descriptions
            response['coverage'].append('run_tools')
        else:
            response['diagnostics'].append(dict(section='run_tools', reason='requires_separate_read', operation='run tools'))
        if logs:
            check()
            remaining = max_bytes - len(bounded_json(response, MAX_BYTES)) - 2048
            if remaining < 6:
                response['diagnostics'].append(dict(section='logs', reason='budget_exhausted', next_offset=offset))
            else:
                # JSON escaping may expand a byte to six bytes. Reserve before
                # reading; the underlying reader already caps its page at 64KiB.
                page = run_manager.logs(root, run_id, attempt=facts['attempt'], offset=offset,
                                        limit=min(4096, remaining // 6))
                response['reads'] += 1
                response['references'].append(dict(ref, kind='run_log_page', observed_at=time.time(), offset=offset,
                                                    next_offset=page['next_offset']))
                response['facts'].append(dict(source=1, value=page))
                response['coverage'].append('run_log_page')
        response['complete'] = not response['diagnostics']
        bounded_json(response, max_bytes)
        return response
