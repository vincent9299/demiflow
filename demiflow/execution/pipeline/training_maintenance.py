"""Reconcile accepted fit jobs under the original stopped producer's lease."""
import json
import os
import time

from demiflow.execution.contracts.training import FitRequest
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.pipeline.run_context import RunAction, stopped_publication_context
from demiflow.execution.journal.run_reader import read_run
from demiflow.execution.journal.training_index import FitConflict, TrainingIndex
from demiflow.execution.training.runtime import FitRuntime


def reconcile_stopped_fits(journal, *, expected_attempt, expected_revision, deadline,
                           confirm_stopped, project_root, deployment=None, deployment_ref=None):
    """A control command. Never creates an attempt, row action or training submission."""
    with read_run(journal.directory, deadline=deadline) as reader:
        routes = reader.fit_routes()
    if not routes:
        return {}
    with stopped_publication_context(journal, expected_attempt=expected_attempt, expected_revision=expected_revision,
                                     deadline=deadline, confirm_stopped=confirm_stopped) as run:
        if deployment is None:
            from demiflow.execution.training.deployment import load_training_deployment
            location = deployment_ref['path'] if deployment_ref else os.environ.get('DEMIFLOW_TRAINING_DEPLOYMENT')
            if not location:
                raise FitConflict("Stopped fit requires its original project deployment")
            deployment = load_training_deployment(location, project_root=project_root,
                        expected_digest=deployment_ref['sha256'] if deployment_ref else None)
        statuses = {}
        for route in routes:
            run.check_stop()
            binding = deployment.binding
            if route['project'] != deployment.project_id or route['binding_digest'] != identity(binding.to_dict()):
                raise FitConflict("Original stopped-fit project/deployment binding differs")
            row = TrainingIndex(binding, readonly=True).read(route['instance'])
            if not row or (row['run_id'],row['action'],row['occurrence']) != (
                    run.ref.run_id, route['action'], route['occurrence']):
                continue  # Candidate crashed before election or followed a different producer.
            declaration = run.journal.execution_action(row['action'])['declaration']
            action = RunAction(run,row['action'],declaration)
            provider = deployment.provider
            if binding.hosting == 'EngineManaged':
                from demiflow.execution.hosts.training_host import ManagedFitHost
                provider = ManagedFitHost.recover(action, row['occurrence'], binding, deadline=deadline)
            runtime = FitRuntime(action,row['occurrence'],FitRequest.from_dict(json.loads(row['request'])),binding,provider)
            step = runtime.reconcile_accepted()
            while True:
                try:
                    next(step)
                except StopIteration as done:
                    statuses[row['instance']] = done.value
                    break
                run.check_stop()
                time.sleep(min(.01,max(0,deadline-time.monotonic())))
        return statuses
