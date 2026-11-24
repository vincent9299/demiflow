"""Journal-backed transfer of a bounded durable window between resident groups.

Parking is not consumption ACK. The producer's credit remains in its actual
snapshot; only an actual consumer frontier can mark a delivery consumed.
"""

from demiflow.execution.contracts.dataflow_errors import DataflowError

from demiflow.execution.checkpoint.residency import DURABLE_SCHEMA as SCHEMA, PARTITION_SCHEMA, member_partitions
from demiflow.execution.scheduler.residency import partition_phases, choose_durable_phase


class DurableResidency:
    def __init__(self, physical, graph, exchange, *, bounds, prior, image):
        self._bounds = bounds
        self._physical = physical
        self._sources = frozenset(graph.sources)
        self._exchange = exchange
        self.visited = set()
        self.completed = set()
        self.epoch = 0
        self.active_index = 0
        from demiflow.execution.scheduler.residency import value_predecessors
        members = {s.stage_id: s.partitions for s in physical.stages}
        self._value_inputs = {s: {p for predecessor in predecessors for p in members[predecessor]}
                              for s, predecessors in value_predecessors(physical).items()}
        phases = partition_phases(physical)
        self.groups = tuple(group for group, _ in phases)
        self.partition_groups = tuple(parts for _, parts in phases)
        self.all_partitions = {p for s in physical.stages for p in s.partitions}
        self.schema = (PARTITION_SCHEMA if any(len(s.partitions) > s.replica_target
                       for s in physical.stages) else SCHEMA)
        if prior is not None:
            if prior['schema'] != self.schema:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'cannot reinterpret finite phase history as parked streaming')
            exchange.restore_parked_channels(prior)
            active = member_partitions(physical, prior, 'active')
            self.visited = member_partitions(physical, prior, 'visited') | active
            self.completed = member_partitions(physical, prior, 'completed')
            if prior['phase_complete']:
                candidate = self.completed | active
                if candidate == self.all_partitions:
                    # A non-final cut may have observed completion while its
                    # participants were freezing. Reopen the last real owner to
                    # obtain final publication, without inventing a contribution.
                    self.epoch = image.cut.sequence
                    self.active_index = next(i for i, g in enumerate(self.partition_groups)
                                             if set(g) == active)
                    return
                self.completed = candidate
            self.epoch = image.cut.sequence
        self.active_index = choose_durable_phase(self._physical, self.groups, self.partition_groups,
            completed=self.completed, value_inputs=self._value_inputs, sources=self._sources,
            finite_seals=self._exchange.finite_seals, edge_eoi=self._exchange.edge_eoi,
            waiting_for=self._exchange.waiting_for)


    def launched(self, endpoints, by_partition):
        assignments = ((endpoint, set(endpoint._assignment.lease.partition_ids)) for endpoint in endpoints.values())
        yield from self._exchange.launch_parked(assignments, by_partition, self._physical.exchanges)
        active_sources = any(set(s.occurrences) & self._sources
                             for s in self._physical.stages if s.stage_id in self.groups[self.active_index])
        if not active_sources and not self.can_drain_in_place():
            for endpoint in endpoints.values():
                endpoint.send_business({'kind': 'park'})

    def can_drain_in_place(self):
        """All other cohorts have finished and no parked channel requires handoff."""
        active = set(self.partition_groups[self.active_index])
        if self.all_partitions - active - self.completed:
            return False
        return not any(edge.mode == 'durable_incremental' and (
            active.intersection(edge.producer_partitions) or active.intersection(edge.consumer_partitions))
            for edge in self._physical.exchanges)

    def final(self, phase_done):
        return phase_done and self.all_partitions <= (
            self.completed | set(self.partition_groups[self.active_index]))

    def metadata(self, phase_done, satisfied_splits):
        from demiflow.execution.checkpoint.residency import durable_metadata
        return durable_metadata(self._physical, schema=self.schema, epoch=self.epoch,
            active_stages=self.groups[self.active_index], active_partitions=self.partition_groups[self.active_index],
            visited=self.visited, completed=self.completed, phase_done=phase_done,
            finite_seals=self._exchange.finite_seals, satisfied_splits=satisfied_splits,
            deliveries=self._exchange.parked_snapshot(), edge_eoi=self._exchange.edge_eoi,
            satisfied_inputs=self._exchange.satisfied_inputs, bounds=self._bounds)


    def committed(self, image, phase_done):
        self.visited.update(self.partition_groups[self.active_index])
        if phase_done:
            self.completed.update(self.partition_groups[self.active_index])
        self._exchange.committed_parked()
        self.epoch = image.cut.sequence
        self.active_index = choose_durable_phase(self._physical, self.groups, self.partition_groups,
            completed=self.completed, value_inputs=self._value_inputs, sources=self._sources,
            finite_seals=self._exchange.finite_seals, edge_eoi=self._exchange.edge_eoi,
            waiting_for=self._exchange.waiting_for)
