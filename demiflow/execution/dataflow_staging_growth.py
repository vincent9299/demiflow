"""Amortize fenced disk admission without increasing the shared byte budget."""
from .contracts.dataflow import ArtifactCommand
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identity
from .run_journal import ExecutionBudgetExceeded


def grow_staging(journal, operation, *, reserved, required, maximum):
    # Disk reservation only: this does not allocate a resident byte buffer.
    # Start small and grow geometrically, then in at most 1 MiB increments.
    # Every unused byte is charged to the SAME original journal until settlement.
    extent = min(maximum, max(required, reserved + min(max(reserved, 65536), 1 << 20)))

    def admit(size):
        reply = journal.artifact(ArtifactCommand(identity([operation, 'grow', size]),
            'grow_staging', None, operation, size))
        if type(reply.charged_bytes) is not int or not size <= reply.charged_bytes <= maximum:
            raise DataflowError('INVALID_CONTRACT', 'staging growth reply differs from its byte grant')
        return reply.charged_bytes

    try:
        return admit(extent)
    except ExecutionBudgetExceeded:
        if extent == required:
            raise
        # Only an atomic, definite budget rejection can take this path. The
        # failed transaction changed no charge. A smaller exact request avoids
        # denying a write merely because optional growth headroom did not fit.
        # Unknown/transport/fence failures propagate; they are never retried.
        return admit(required)
