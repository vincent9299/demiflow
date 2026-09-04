"""Single Driver program contract for one Demiflow Pipeline run."""
from __future__ import annotations

import abc


class PipelineProgram(abc.ABC):
    """Base class for one statically discoverable Demiflow Pipeline program.

    A Candidate stores generated Python modules and API-required resources under
    the Workplace ``pipeline/`` package. The package must define exactly one
    top-level concrete ``PipelineProgram`` subclass; the platform derives the
    entrypoint from that class and never requires a separate entrypoint file.

    The concrete class must declare ``execution`` exactly once. ``execution`` is
    Candidate-owned and is frozen with the package. It may be either a literal
    mapping or the basename of a top-level YAML file inside ``pipeline/`` whose
    content is that same mapping. The execution mapping is closed:

    Portable example::

        class Main(PipelineProgram):
            execution = {"mode": "portable"}

    Native Ray example::

        class Main(PipelineProgram):
            execution = {"mode": "native", "backend": "ray"}

    YAML resource example::

        class Main(PipelineProgram):
            execution = "ray-execution.yaml"

    The YAML file contains only the execution mapping; it has no schema version,
    entrypoint, target address, or outer ``execution`` key. Per-run backend,
    Ray Job address, namespace, and timeout belong to ``PipelineExecutionTarget``.
    Do not invent fields: shared storage roots, sources,
    sinks, model connections, and business thresholds belong to the APIs that
    use them, such as ``write_json`` paths, Lance source and write arguments, or
    ``Dataset.map_prompt`` Prompt Packs.

    ``run(ctx)`` may call small, bounded, synchronous, statically reachable
    Python helpers. Concurrent, batched, or distributed data work belongs in
    ``ctx.data`` Dataset plans; Candidate code must not create threads,
    processes, Ray tasks, or another execution lifecycle.

    ``run(ctx)`` constructs lazy Dataset plans and triggers every required
    terminal action. Successful execution must return ``None``. Durable business
    records, conclusions, and statistics belong in explicit Dataset sinks, not
    in the return value.

    Generated modules must be import-pure. Module scope may contain imports,
    class and function definitions, and literal constants. Construct runtime
    values such as ``ResourceRef``, ``ScanRequest``, and ``AppendRequest``
    inside ``run`` or a statically reachable helper, not at module scope.
    """

    @abc.abstractmethod
    def run(self, ctx) -> None:
        """Execute once, trigger all required actions, and return ``None``.

        Do not return Dataset values, business rows, write receipts, execution
        summaries, readback values, or status dictionaries.
        """
        raise NotImplementedError
