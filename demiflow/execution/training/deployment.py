"""Training services selected by the run's deployment, outside the sealed recipe."""
from dataclasses import dataclass
from importlib import import_module
import json
import os
from pathlib import Path

from ..contracts.training import FitBinding, FitJobProvider
from ..recovery_contract import document, identifier


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


def resolve_training_deployment(context):
    if context.training is not None:
        return context.training
    location = os.environ.get('DEMIFLOW_TRAINING_DEPLOYMENT')
    if not location:
        raise ValueError("The run needs a project training deployment")
    return load_training_deployment(location, project_root=context.training_project_root)


def load_training_deployment(location, *, project_root=None):
    path = Path(location)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("Training deployment must be an absolute, direct file")
    with path.open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError("Training deployment exceeds 64 KiB")
    value = json.loads(raw)
    document(value, 65536)
    if project_root is not None and value.get('project_root') != str(Path(project_root).resolve(strict=True)):
        raise ValueError("Training deployment does not authorize this project root")
    if set(value) - {'project_id', 'project_root', 'binding', 'provider_factory'}:
        raise ValueError("Unknown training deployment option")
    binding = FitBinding.from_dict(value['binding'])
    provider = None
    if 'provider_factory' in value:
        if binding.hosting != 'ProviderManaged':
            raise ValueError("A provider factory requires ProviderManaged hosting")
        module, separator, name = value['provider_factory'].partition(':')
        if not separator or not all(part.isidentifier() for part in module.split('.')) or not name.isidentifier():
            raise ValueError("Invalid deployment provider factory")
        provider = getattr(import_module(module), name)(binding)
    return TrainingDeployment(value['project_id'], binding, provider)
