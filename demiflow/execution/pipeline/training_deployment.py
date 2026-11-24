"""Training services selected by the run's deployment, outside the sealed recipe."""
from dataclasses import dataclass

from hashlib import sha256
import json
import os
from pathlib import Path

from demiflow.execution.contracts.training import FitBinding, FitJobProvider
from demiflow.execution.contracts.recovery_contract import document, identifier, identity
from demiflow.execution.journal.training_index import TrainingIndex


@dataclass(frozen=True)
class TrainingDeployment:
    project_id: str
    binding: FitBinding
    provider: FitJobProvider | None = None

    def __post_init__(self):
        identifier(self.project_id)
        if not isinstance(self.binding, FitBinding):
            raise TypeError("training deployment needs a FitBinding")
        required = ('submit', 'lookup', 'inspect', 'result', 'cancel')
        if self.binding.hosting == 'ProviderManaged':
            if not all(callable(getattr(self.provider, name, None)) for name in required):
                raise TypeError("ProviderManaged deployment needs a complete job provider")
        elif self.provider is not None:
            raise ValueError("EngineManaged deployment selects its host through the scheduler")

    def apply_project_quotas(self):
        """Apply the four nondecreasing project storage/admission limits as one operation."""
        TrainingIndex(self.binding, quotas='apply')


def bind_action_deployment(action, project_id, binding):
    run = action.context
    key = identity(['training-deployment', action.action_id])
    original = {'project': project_id, 'binding_digest': identity(binding.to_dict())}
    # The original lease serializes this decision against fence replacement.
    # Domain changes commit independently and a retry rechecks the same proposal.
    with run.journal.publication_guard(run.fence):
        if run.journal.source_binding(key) is None:
            if run.attempt == 1:
                TrainingIndex(binding, quotas='apply')
            else:
                # No prior fit admission authorizes a stale proposal on resume.
                TrainingIndex(binding, readonly=True, quotas='match')
        run.journal.bind_source(run.fence, key, original)


def resolve_training_deployment(context):
    if context.training is not None:
        return context.training
    location = os.environ.get('DEMIFLOW_TRAINING_DEPLOYMENT')
    if not location:
        raise ValueError("The run needs a project training deployment")
    return load_training_deployment(location, project_root=context.training_project_root,
                                    expected_digest=os.environ.get('DEMIFLOW_TRAINING_DEPLOYMENT_SHA256'))


def _read_deployment(location, project_root, expected_digest=None):
    path = Path(location)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("Training deployment must be an absolute, direct file")
    with path.open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError("Training deployment exceeds 64 KiB")
    digest = sha256(raw).hexdigest()
    if expected_digest is not None and digest != expected_digest:
        raise ValueError('Original training deployment file changed')
    value = json.loads(raw)
    document(value, 65536)
    if project_root is not None and value.get('project_root') != str(Path(project_root).resolve(strict=True)):
        raise ValueError("Training deployment does not authorize this project root")
    if set(value) - {'project_id', 'project_root', 'binding', 'provider_factory'}:
        raise ValueError("Unknown training deployment option")
    return value, digest


def training_deployment_reference(location, *, project_root):
    """Capture ownership for a workbench plan without importing a provider."""
    if Path(location).resolve().is_relative_to(Path(project_root).resolve()):
        raise ValueError("Workbench training deployment must be outside the project")
    value, digest = _read_deployment(location, project_root)
    return {'path': location, 'sha256': digest, 'hosting': FitBinding.from_dict(value['binding']).hosting}


def load_training_deployment(location, *, project_root=None, expected_digest=None):
    value, _ = _read_deployment(location, project_root, expected_digest)
    binding = FitBinding.from_dict(value['binding'])
    provider = None
    if 'provider_factory' in value:
        if binding.hosting != 'ProviderManaged':
            raise ValueError("A provider factory requires ProviderManaged hosting")
        from demiflow.execution.hosts.training_provider_io import instantiate_provider
        provider = instantiate_provider(value['provider_factory'], binding)
    return TrainingDeployment(value['project_id'], binding, provider)
