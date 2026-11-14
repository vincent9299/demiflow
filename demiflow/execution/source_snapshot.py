"""Fixed, bounded source capture before any downstream effect is dispatched."""
from .run_context import RunAction
from .block_sink import BlockSink
from .commit import CommitCoordinator
from .recovery_contract import SourcePosition, identity


def freeze_source(action, source, path="root"):
    """Resolve a mutable Lance head once, before capture or any downstream work."""
    from dataclasses import replace
    from ..data.sources import LanceSource
    from .local_kernel import KeyedSource, UnionSource
    def bound(frozen):
        from .checked import current
        checked = current()
        if checked is not None and id(source) in checked.source_bindings:
            checked.source_bindings[id(frozen)] = checked.source_bindings[id(source)]
        return frozen
    if isinstance(source, KeyedSource):
        left = replace(source.left, source=freeze_source(action, source.left.source, path + '-left'))
        right = source.right
        if right is not None:
            right = replace(right, source=freeze_source(action, right.source, path + '-right'))
        return bound(replace(source, left=left, right=right))
    if isinstance(source, UnionSource):
        return bound(replace(source, inputs=tuple(replace(item,
            source=freeze_source(action, item.source, path + '-' + str(index)))
            for index, item in enumerate(source.inputs))))
    if isinstance(source, LanceSource):
        journal = action.context.journal
        key = action.action_id + "-" + identity(path)[:16]
        binding = journal.source_binding(key)
        if binding is None:
            from ..lance.storage import inspect_lance
            version = source.query.version
            if version is None:
                version = inspect_lance(source.query.uri, None, source.query.storage_options).resolved_version
            binding = dict(kind="lance", uri=source.query.uri, version=version)
            journal.bind_source(action.context.fence, key, binding)
        return bound(replace(source, query=replace(source.query, version=binding["version"])))
    return source


def capture_source(action, rows_factory):
    run = action.context
    source_id = action.action_id + "-source"
    declaration = dict(kind="fixed-source-capture", source=action.declaration["source"])
    run.journal.bind_execution_action(run.fence, source_id, declaration)
    capture = RunAction(run, source_id, declaration)
    sink = BlockSink(capture, "source", input_capture=True)
    coordinator = CommitCoordinator(capture)
    facts = coordinator.recover({"source": sink})
    record = capture.record
    if record["result"] is None:
        if not facts or facts[-1]["status"] == "pending":
            rows = iter(rows_factory())
            try:
                for row in rows:
                    sink.append(row)
            finally:
                close = getattr(rows, "close", None)
                if close is not None:
                    close()
            fact = coordinator.commit(1, outputs={"source": (sink, None)},
                position=SourcePosition(identity(declaration), sink.rows))
        else:
            fact = facts[-1]
        capture.complete(fact["receipts"]["source"]["data"])
        record = capture.record
    fixed = record["result"]
    sink._validate(fixed)
    reference = dict(source_id=identity([source_id, fixed]), data=fixed)
    run.journal.set_action_snapshot(run.fence, action.action_id, reference)
    return sink, reference


def prefix_rows(action, dataset, *, stage_key, max_rows, max_row_bytes):
    """Capture a finite prefix under the parent run's existing source protocol.

    The internal capture never enters a public Dataset action. A mutable source
    head is fixed once, and resume reuses the exact captured rows before skipping
    the already-published prefix frontier.
    """
    from contextlib import closing
    from itertools import islice
    from .recovery_identity import plan_identity
    from .stream_grouping import row_size
    source = freeze_source(action, dataset._source, path='prefix-' + stage_key)
    declaration = plan_identity('prefix-input', source, dataset._plan)
    child_id = action.action_id + '-prefix-' + stage_key
    action.context.journal.bind_execution_action(action.context.fence, child_id, declaration)
    child = RunAction(action.context, child_id, declaration)
    def rows():
        with closing(dataset._executor._apply_plan(source, dataset._plan)) as stream:
            for row in islice(stream, max_rows + 1):
                row_size(row, max_row_bytes)
                yield row
    sink, snapshot = capture_source(child, rows)
    if snapshot['data']['rows'] > max_rows:
        from .recovery_contract import RecoveryContractError
        raise RecoveryContractError('Prepend prefix exceeds its declared row bound')
    child.complete({'kind': 'prefix-input', 'snapshot': snapshot})
    yield from sink.iter_rows(snapshot['data'])
