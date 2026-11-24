"""One prepare/publish/progress/ACK protocol inside the existing run driver."""
from dataclasses import asdict

from demiflow.data.datasink import WriteContext, Datasink
from demiflow.execution.contracts.recovery_contract import CommitContext, SourcePosition, RecoveryContractError, MAX_SINKS, MAX_STATES, identifier, validate_receipt, identity
from demiflow.execution.storage.state_artifacts import StateArtifacts


def fault_point(name, commit_id):
    """Internal fault-injection seam; never a pipeline recovery option."""


class CommitCoordinator:
    """Coordinates durable effects only; does not execute or schedule a graph."""
    def __init__(self, action):
        self.action = action
        self.run = action.context
        self.journal = self.run.journal
        self._artifacts = None

    @property
    def artifacts(self):
        if self._artifacts is None:self._artifacts = StateArtifacts(self.run)
        return self._artifacts

    def _context(self, commit_id):
        return WriteContext(**asdict(CommitContext(self.run.ref.run_id, self.action.action_id,
            commit_id, self.run.attempt, self.run.fence)))

    def commit(self, sequence, *, outputs, position, states=None, acknowledge=None):
        """outputs maps stable sink IDs to (native Datasink adapter, blocks).

        All staging and semantic state becomes durable before any sink publishes.
        On resume, prepared artifacts/receipts are authoritative; input computation
        is not repeated to invent the missing half of a partially committed unit.
        """
        self.run.check_stop()
        if not isinstance(position, SourcePosition):
            raise RecoveryContractError("A commit needs a fixed source position")
        if type(outputs) is not dict or not 1 <= len(outputs) <= MAX_SINKS:
            raise RecoveryContractError("Invalid bounded sink set")
        states = states or {}
        if type(states) is not dict or len(states) > MAX_STATES:
            raise RecoveryContractError("State participant count exceeds budget")
        for sink_id, (sink, _) in outputs.items():
            identifier(sink_id)
            for method in ("prepare", "commit", "inspect", "abort"):
                if not callable(getattr(sink, method, None)) or getattr(type(sink), method, None) is getattr(Datasink, method):
                    raise RecoveryContractError("Sink lacks native durable " + method)
        from demiflow.execution.contracts.recovery_contract import document
        state_bytes = sum(len(document(value, self.run.limits['max_state_bytes'])) for value in states.values())
        if state_bytes > self.run.limits['max_state_bytes']:
            raise RecoveryContractError('Aggregate actor state exceeds byte budget')
        intent = dict(position=position.to_dict(), sinks=sorted(outputs), states=sorted(states),
                      requires_ack=acknowledge is not None)
        fact = self.journal.begin_commit(self.run.fence, self.action.action_id, sequence, intent)
        context = self._context(fact["commit_id"])
        fault_point("after_intent", context.commit_id)
        if fact["status"] == "pending":
            state_refs = {key: self.artifacts.put(key, value) for key, value in states.items()}
            prepared = {}
            for sink_id, (sink, blocks) in outputs.items():
                # Preparation may execute ordinary operators and reserve budgets.
                # It cannot publish, and must not hold the publication transaction.
                with self.journal.execution_guard(self.run.fence):
                    pass
                prepared[sink_id] = sink.prepare(blocks, context)
            fault_point("after_prepare_before_record", context.commit_id)
            descriptor = dict(sinks=prepared, state=state_refs)
            acquisitions = sorted(getattr(self.action, 'raw_acquisitions', ()))
            if acquisitions:
                descriptor['raw_acquisitions'] = acquisitions
            self.journal.prepare_commit(self.run.fence, context.commit_id, descriptor)
            fault_point("after_prepared", context.commit_id)
            fact = self.journal.execution_commit(context.commit_id)
        result = self._finish(fact, {key: value[0] for key, value in outputs.items()}, acknowledge)
        pending = getattr(self.action, 'raw_acquisitions', None)
        if pending is not None:
            pending.difference_update(result['prepared'].get('raw_acquisitions', ()))
        return result

    def recover(self, sinks, *, acknowledge=None):
        """Reconcile already prepared units before any source/actor resumes."""
        facts = self.journal.execution_commits(self.action.action_id)
        for fact in facts:
            if fact["status"] == "pending":
                continue  # No publication was permitted; its calculation may repeat.
            self._finish(fact, sinks, acknowledge)
        return self.journal.execution_commits(self.action.action_id)

    def inspect_completed(self, fact, sinks):
        """Read-only verification; never reacquires or uses a former owner lease.

        Completion proofs are immutable. Missing bytes or inconsistent native
        states are integrity failures, never invitations to repair a done task.
        """
        if fact['status'] != 'acked' or fact['prepared'] is None:
            raise RecoveryContractError('Completion has inconsistent native commit state')
        context = self._context(fact['commit_id'])
        if set(sinks) != set(fact['intent']['sinks']) or set(sinks) != set(fact['receipts']):
            raise RecoveryContractError('Completion sink set differs')
        from demiflow.execution.native_queue.raw_objects import inspect_acquisition
        for commit_id in fact['prepared'].get('raw_acquisitions', ()):
            inspect_acquisition(self.run, commit_id)
        for reference in fact['prepared']['state'].values():
            self.artifacts.read(reference)
        for sink_id, sink in sinks.items():
            receipt = sink.inspect(fact['prepared']['sinks'][sink_id], context)
            receipt = validate_receipt(receipt, context, sink_id)
            if receipt != fact['receipts'][sink_id]:
                raise RecoveryContractError('Completed receipt differs from artifact evidence')
        return fact

    def _finish(self, fact, sinks, acknowledge):
        context = self._context(fact["commit_id"])
        if set(sinks) != set(fact["intent"]["sinks"]):
            raise RecoveryContractError("Recovery sink set differs")
        if bool(acknowledge is not None) != fact["intent"]["requires_ack"]:
            raise RecoveryContractError("Recovery source ACK contract differs")
        prepared = fact["prepared"]
        from demiflow.execution.native_queue.raw_objects import inspect_acquisition
        for commit_id in prepared.get('raw_acquisitions', ()):
            inspect_acquisition(self.run, commit_id)
        for reference in prepared["state"].values():
            self.artifacts.read(reference)
        # Even confirmed receipts are inspected, so corruption cannot turn into
        # a silently skipped source prefix.
        from demiflow.execution.native_queue.raw_objects import RawObjectSink
        from demiflow.execution.native_queue.queue_sinks import QueueResultSink, QueuePublishSink
        for sink_id, sink in sinks.items():
            self.run.check_stop()
            if type(sink) is RawObjectSink:
                from demiflow.execution.native_queue.raw_objects import ConcurrentRawPublication
                for publication_try in range(3):
                    try:
                        # Hashing may take minutes; never hold the control DB lock.
                        with sink.inspection(prepared['sinks'][sink_id]) as inspected:
                            self.run.check_stop()
                            with self.journal.execution_guard(self.run.fence) as db:
                                receipt = sink.inspect_guarded(prepared['sinks'][sink_id], context, inspected)
                                if receipt is None:
                                    if sink_id in fact['receipts']:
                                        raise RecoveryContractError('Previously committed sink data disappeared')
                                    fault_point('before_sink_commit', context.commit_id)
                                    receipt = sink.publish_guarded(prepared['sinks'][sink_id], context, inspected)
                                    fault_point('after_sink_commit_before_receipt', context.commit_id)
                                receipt = validate_receipt(receipt, context, sink_id)
                                self.journal.publish_receipt(db, context.commit_id, sink_id, receipt)
                        break
                    except ConcurrentRawPublication:
                        if publication_try == 2:
                            raise
                fault_point('after_sink_receipt', context.commit_id)
            elif type(sink) in {QueueResultSink, QueuePublishSink}:
                if type(sink) is QueueResultSink:
                    sink.verify_acquisition(prepared['sinks'][sink_id])
                self.run.check_stop()
                with self.journal.execution_guard(self.run.fence) as db:
                    receipt = sink.inspect_in_transaction(db, prepared['sinks'][sink_id], context)
                    if receipt is None:
                        if sink_id in fact['receipts']:
                            raise RecoveryContractError('Previously committed task SQL data disappeared')
                        fault_point('before_sink_commit',context.commit_id)
                        receipt = sink.publish_in_transaction(db,prepared['sinks'][sink_id],context)
                        fault_point('after_sink_commit_before_receipt',context.commit_id)
                    receipt = validate_receipt(receipt,context,sink_id)
                    self.journal.publish_receipt(db,context.commit_id,sink_id,receipt)
                fault_point('after_sink_receipt',context.commit_id)
            else:
                guard = getattr(self.journal, 'publication_guard', self.journal.execution_guard)
                with guard(self.run.fence) as publication_db:
                    receipt = sink.inspect(prepared["sinks"][sink_id], context)
                    if receipt is None:
                        if sink_id in fact["receipts"]:
                            raise RecoveryContractError("Previously committed sink data disappeared")
                        fault_point("before_sink_commit", context.commit_id)
                        receipt = sink.commit(prepared["sinks"][sink_id], context)
                        fault_point("after_sink_commit_before_receipt", context.commit_id)
                        verified = sink.inspect(prepared['sinks'][sink_id], context)
                        if verified is None or verified != receipt:
                            from demiflow.execution.contracts.recovery_contract import UnresolvedCommit
                            raise UnresolvedCommit('Sink publication returned without matching inspect evidence')
                    receipt = validate_receipt(receipt, context, sink_id)
                    if publication_db is None:
                        with self.journal.execution_guard(self.run.fence) as db:
                            self.journal.publish_receipt(db, context.commit_id, sink_id, receipt)
                    else:
                        self.journal.publish_receipt(publication_db, context.commit_id, sink_id, receipt)
                fault_point("after_sink_receipt", context.commit_id)
        # Every publication call has returned; only its removed private staging
        # registrations are eligible. Durable receipts and prepared data stay.
        from demiflow.execution.outputs.object_sink import ObjectRowsSink
        for sink in sinks.values():
            participants = ([sink.rows, *sink.object_sinks.values()]
                            if type(sink) is ObjectRowsSink else [sink])
            for participant in participants:
                marker = getattr(participant, '_marker', None)
                if marker is not None:
                    path = marker(context)
                    self.run.release_temporaries([path.with_name(path.name + '.staging')])
        self.journal.confirm_commit(self.run.fence, context.commit_id,
            expected="prepared", status="data_committed")
        fault_point("after_data_committed", context.commit_id)
        self.journal.confirm_commit(self.run.fence, context.commit_id,
            expected="data_committed", status="checkpoint_committed")
        fault_point("after_checkpoint_committed", context.commit_id)
        from demiflow.execution.native_queue.queue_commit import QueueCommitStore
        if type(self.journal) is QueueCommitStore and self.journal.phase == 'final':
            self.journal.finalize_source_commit(self.run.fence, context.commit_id)
            fault_point('after_ack_before_record', context.commit_id)
            fault_point('after_acked', context.commit_id)
            return self.journal.execution_commit(context.commit_id)
        if fact["status"] != "acked":
            self.run.check_stop()
            if acknowledge is not None:
                with self.journal.execution_guard(self.run.fence):
                    acknowledge(SourcePosition(**fact["intent"]["position"]), context)
                fault_point("after_ack_before_record", context.commit_id)
            self.journal.confirm_commit(self.run.fence, context.commit_id,
                expected="checkpoint_committed", status="acked")
        fault_point("after_acked", context.commit_id)
        return self.journal.execution_commit(context.commit_id)


def commit_write_context(action, commit_id):
    """The write context a sink's ``inspect`` expects for one recorded commit."""
    return CommitCoordinator(action)._context(commit_id)
