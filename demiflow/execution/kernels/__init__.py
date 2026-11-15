"""Unified v3 compute kernels (lane K).

Controlled compute kernels for the single dataflow engine. They own operator
runtime state through :class:`OperatorKernel` but never a second run/commit
ledger; durable authority stays with the native RunJournal/CommitCoordinator.
"""