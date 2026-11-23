"""Drive a co-located, sealed fixed-source key exclusion with ordinary worker credits and cuts."""
from ..contracts.dataflow_errors import DataflowError
from ..contracts.relational_region import KEY_BATCH_SUFFIXES, KEY_BATCH_SQL_SUFFIX, fixed_key_inputs
from ..kernels.lance_keys import BatchKeySelection
from ..lance_key_source import PinnedKeyPayload


class FixedKeyExclusion:
    @classmethod
    def open(cls, runner):
        executor = runner.e
        stage = next((s for s in executor.assignment.stages if s.stage_id.endswith(KEY_BATCH_SUFFIXES)), None)
        if stage is None:
            return None
        shape = fixed_key_inputs(executor._graph)
        if not shape or set(shape) != set(executor._node_specs):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'fixed key stage differs from the sealed graph')
        left, right, _, exclude, _ = shape
        splits = {s.source_id: s.split_id for s in executor.assignment.source_splits}
        if len(splits) != 2 or set(splits) != {left, right}:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'fixed key stage requires its original splits')
        spec = executor._node_specs[exclude].kernel
        field = spec.configuration['on'][0]
        scans = {occ: executor._readers[split].pinned_keys(field, positions=occ == left)
                 for occ, split in splits.items()}
        sql = stage.stage_id.endswith(KEY_BATCH_SQL_SUFFIX)
        if any(s is None for s in scans.values()) or not BatchKeySelection.admits(scans[left], scans[right], sql=sql):
            return None
        service = executor._finite_services
        payload = PinnedKeyPayload(spec, service)
        if not payload.supports_batch_output():
            return None
        return cls(shape, splits, scans, payload, sql=sql)

    def __init__(self, shape, splits, scans, payload, *, sql):
        self.left, self.right, self.project, self.exclude, self.terminal = shape
        self.splits, self.scans, self.payload = splits, scans, payload
        self.keys = BatchKeySelection(sql=sql)
        self.selection = None

    def step(self, runner):
        if self.terminal in runner._finished:
            return 0
        runner.services.check_stop()
        if self.scans:
            source = next(iter(self.scans))
            credit = runner._source_credit
            rows, size = runner.bounds.max_output_rows, runner.bounds.max_batch_bytes
            if credit is not None:
                rows = min(rows, credit['rows'] - runner._unconfirmed_rows)
                size = min(size, credit['bytes'] - runner._unconfirmed_bytes)
            if not runner._publication_overlap:
                rows = min(rows, runner._source_quantum - runner._source_rows_since_cut)
            if rows < 1 or size <= 0 or (runner._unconfirmed_bytes and size < runner.bounds.max_row_bytes):
                runner._paused = True
                runner._flush_metrics()
                return 1
            step = self.scans[source].advance(rows, size)
            if step is None:
                del self.scans[source]
                runner._finish_source_split(self.splits[source], source)
                if source == self.right:
                    self.finish_node(runner, self.project)
                return 1
            new, count, charged = step
            if new is not None:
                self.keys.append('lkeys' if source == self.left else 'rkeys', *new)
            runner._metrics[source]['rows_output'] += count
            runner._metrics[self.exclude]['rows_input'] += count
            runner._dirty_metrics.update((source, self.exclude))
            if source == self.right:
                runner._metrics[self.project]['rows_input'] += count
                runner._metrics[self.project]['rows_output'] += count
                runner._dirty_metrics.add(self.project)
            runner._unconfirmed_rows += count
            runner._unconfirmed_bytes += charged
            runner._source_rows_since_cut += count
            if not runner._publication_overlap and runner._source_rows_since_cut >= runner._source_quantum:
                runner._paused = True
                runner._flush_metrics()
            return 1
        if self.selection is None:
            self.selection = self.keys.survivors(runner.e._finite_services, skip=runner._terminal_rows,
                                                 batch_size=self.payload.hydration_rows(bulk=True))
        positions = next(self.selection, None)
        if positions is None:
            self.finish_node(runner, self.exclude)
            self.finish_node(runner, self.terminal)
            return 1
        table = self.payload.read_table(positions.column('row_id').to_numpy())
        start = 0
        for batch in table.to_batches():
            runner._accept_arrow_result(self.terminal, batch, positions=positions.slice(start,batch.num_rows))
            start += batch.num_rows
        runner._metrics[self.exclude]['rows_output'] += start
        runner._dirty_metrics.add(self.exclude)
        return 1

    def close(self):
        for scan in self.scans.values():
            scan.close()
        self.scans.clear()
        if self.selection is not None:
            self.selection.close()
        self.keys = self.payload = None

    def finish_node(self, runner, occurrence):
        runner._finished_ports.update(e.edge_id for e in runner._incoming.get(occurrence, ()))
        runner._finish_node(occurrence)
