"""Everything the plan compiler needs besides the request and its bounds.

The compiler used to read two things from its surroundings while it worked: the
executor attached to the action, for three defaults, and the checked-execution
context variable, for report bindings. Both are now handed to it as one explicit
``CompileInputs`` value, and ``compile_inputs`` is the single place where the
surroundings are read.

``lower_action(request, bounds=..., inputs=...)`` accepts the value; when a
caller passes none, the compiler calls ``compile_inputs(request)`` once, at its
entry, before it captures anything. A caller that lowers the same request more
than once may resolve the inputs once and pass the same value each time.

Equivalence with the previous behaviour:

* The defaults are the same three attribute reads with the same fallbacks.
* The report bindings are asked through the same context object, lazily, when
  the compiler reaches a source, an operation or the terminal, exactly as the
  old code read the three mappings. Nothing is copied or cached.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompileDefaults:
    """Defaults a plan takes from the executor that requested it. ``None`` means "not provided".

    ``batch_rows``  the local kernel's batch size. It is the default batch size of
                    ``map_batches`` and the task batch size of task-scoped kernels
                    (8192 when absent).
    ``block_size``  the executor's block size. It is the default batch size of
                    ``map_batches`` only when ``batch_rows`` is absent or zero.
    ``partitions``  the local kernel's partition count. It is the block count of a
                    ``random_shuffle`` that declares none.
    """
    batch_rows: int | None = None
    block_size: int | None = None
    partitions: int | None = None

    @property
    def map_batch_size(self):
        return self.batch_rows or self.block_size

    @property
    def task_batch_rows(self):
        return self.batch_rows or 8192


class ReportBindings:
    """The four questions the compiler asks a report context. Read-only and lazy.

    A report key is ``(action_id, report_node_id, report_occurrence)``. Bindings
    are held by object identity: a source or an operation the compiler creates
    itself (a zero-row take, the key-only side of exclude_keys) has none.
    """
    __slots__ = ('_context',)

    def __init__(self, context):
        self._context = context

    def source(self, source):
        """The report key bound to this source object, or ``None``."""
        return self._context.source_bindings.get(id(source))

    def operation(self, operation):
        """The report key bound to this operation object, or ``None``."""
        return self._context.operation_bindings.get(id(operation))

    def observed(self, key) -> bool:
        """Whether the report has an observation under this key."""
        return key in self._context.observations

    def reused(self, key) -> bool:
        """Whether the observation under this key says the result was reused."""
        return self._context.observations.get(tuple(key), {}).get("state") == "reused"


@dataclass(frozen=True)
class CompileInputs:
    defaults: CompileDefaults = CompileDefaults()
    report: ReportBindings | None = None


def executor_defaults(executor) -> CompileDefaults:
    """The three defaults of an executor; every missing attribute is ``None``."""
    local = getattr(executor, '_local_kernel', None)
    return CompileDefaults(batch_rows=getattr(local, 'batch_rows', None),
                           block_size=getattr(executor, '_block_size', None),
                           partitions=getattr(local, 'partitions', None))


def compile_inputs(request) -> CompileInputs:
    """Read the compiler's inputs from the request's action and the current report context."""
    from .checked import current
    context = current()
    return CompileInputs(defaults=executor_defaults(getattr(request.action, '_dataflow_executor', None)),
                         report=ReportBindings(context) if context is not None else None)
