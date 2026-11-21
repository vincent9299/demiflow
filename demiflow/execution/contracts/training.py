"""Value and callable contracts at the sealed fit/predict boundary."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Protocol
import re

from ..recovery_contract import document, identity


def _text(value, label):
    if type(value) is not str or not value or len(value) > 4096 or "\0" in value:
        raise ValueError(f"Invalid {label}")
    return value


def _digest(value):
    if type(value) is not str or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Invalid training content digest")
    return value


def _number(value, label, *, positive=False):
    minimum = 1 if positive else 0
    if type(value) is not int or not minimum <= value < 2 ** 63:
        raise ValueError(f"Invalid {label}")
    return value


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(child) for child in value]
    if hasattr(type(value), 'to_dict'):
        return value.to_dict()
    return value


def _immutable(value):
    if type(value) is dict:
        return MappingProxyType({k: _immutable(v) for k, v in value.items()})
    if type(value) in (tuple, list):
        return tuple(_immutable(v) for v in value)
    return value


def _json_map(value):
    if isinstance(value, MappingProxyType):
        value = _plain(value)
    if type(value) is not dict:
        raise ValueError("Training parameters must be a JSON object")
    document(value)
    return _immutable(value)


class _Value:
    wire_schema = None

    def to_dict(self):
        data = {f.name: _plain(getattr(self, f.name)) for f in fields(self)}
        if self.wire_schema:
            data['schema'] = self.wire_schema
        document(data)
        return data

    @classmethod
    def from_dict(cls, data):
        document(data)
        if type(data) is not dict:
            raise ValueError("Invalid training value")
        if cls.wire_schema:
            data = data.copy()
            if data.pop('schema', None) != cls.wire_schema:
                raise ValueError("Unsupported training value schema")
        return cls(**data)


@dataclass(frozen=True)
class FixedDatasetRef(_Value):
    uri: str
    version: int
    content_digest: str
    schema_digest: str
    order_digest: str
    rows: int
    source_digest: str | None = None

    def __post_init__(self):
        _text(self.uri, "dataset URI")
        _number(self.version, "dataset version", positive=True)
        _number(self.rows, "dataset size")
        if self.source_digest is not None:
            _digest(self.source_digest)
        for item in (self.content_digest, self.schema_digest, self.order_digest):
            _digest(item)

    def identity(self):
        # Content includes the transitive, ordered source-version proof. A new
        # scratch table may have another path/version while carrying that proof.
        return dict(source=self.source_digest or identity([self.uri, self.version]),
                    content=self.content_digest, schema=self.schema_digest, order=self.order_digest,
                    rows=self.rows)


@dataclass(frozen=True)
class ProjectObject(_Value):
    uri: str
    sha256: str
    size_bytes: int

    def __post_init__(self):
        _text(self.uri, "object URI")
        _digest(self.sha256)
        _number(self.size_bytes, "object bytes")


def _file_entries(value):
    if not isinstance(value, Mapping) or len(value) > 16384:
        raise ValueError("Invalid model file manifest")
    data = {}
    for name, ref in value.items():
        _text(name, "model member name")
        path = PurePosixPath(name)
        if path.is_absolute() or any(part in ('', '.', '..') for part in name.split('/')) or '\\' in name:
            raise ValueError("Invalid model member path")
        data[name] = ref if type(ref) is ProjectObject else ProjectObject.from_dict(ref)
    return MappingProxyType(data)


@dataclass(frozen=True)
class TrainingSnapshotRef(_Value):
    step: int
    model_format: str
    files: Mapping[str, ProjectObject]
    resume_files: Mapping[str, ProjectObject] = field(default_factory=dict)

    def __post_init__(self):
        _number(self.step, "optimizer step", positive=True)
        _text(self.model_format, "model format")
        object.__setattr__(self, 'files', _file_entries(self.files))
        object.__setattr__(self, 'resume_files', _file_entries(self.resume_files))
        if not self.files or self.files.keys() & self.resume_files.keys():
            raise ValueError("Model and resume files must be unambiguous")
        names = self.files.keys() | self.resume_files.keys()
        if any(str(parent) in names for name in names for parent in PurePosixPath(name).parents):
            raise ValueError("A model file cannot be a directory")
        document(self.to_dict())


@dataclass(frozen=True)
class FitRequest(_Value):
    wire_schema = 'demiflow.fit_request.v1'

    project_id: str
    inputs: Mapping[str, FixedDatasetRef]
    semantics: Mapping
    retrain_id: str | None = None
    parent: TrainingSnapshotRef | None = None
    restart_snapshot: TrainingSnapshotRef | None = None

    def __post_init__(self):
        _text(self.project_id, "project identity")
        if self.retrain_id is not None:
            _text(self.retrain_id, "retrain identity")
        if not isinstance(self.inputs, Mapping) or 'train' not in self.inputs or len(self.inputs) > 64:
            raise ValueError("Training requires named fixed inputs including train")
        inputs = {_text(k, "input name"): (v if type(v) is FixedDatasetRef else FixedDatasetRef.from_dict(v))
                  for k, v in self.inputs.items()}
        object.__setattr__(self, 'inputs', MappingProxyType(inputs))
        sem = _json_map(self.semantics)
        for key in ('trainer_digest', 'base_model_digest', 'environment_digest'):
            _digest(sem.get(key))
        _text(sem.get('model_format'), "model format")
        for key in ('parameters','budget','topology'):
            if not isinstance(sem.get(key), Mapping):
                raise ValueError(f"Missing training {key}")
        steps = sem.get('save_steps')
        if type(steps) is not tuple or not steps or tuple(sorted(set(steps))) != steps:
            raise ValueError("Expected ordered unique save_steps")
        for step in steps:
            _number(step, 'saved step', positive=True)
        object.__setattr__(self, 'semantics', sem)
        for name in ('parent','restart_snapshot'):
            value = getattr(self, name)
            if value is not None:
                if type(value) is not TrainingSnapshotRef:
                    value = TrainingSnapshotRef.from_dict(value)
                if not value.resume_files or value.model_format != sem['model_format']:
                    raise ValueError("Continuation requires compatible optimizer state")
                object.__setattr__(self, name, value)
        document(self.to_dict())

    @property
    def fit_key(self):
        parent = self.parent
        if parent is not None:
            parent = {'step': parent.step, 'model_format': parent.model_format,
                      **{group: {k: [obj.sha256, obj.size_bytes] for k, obj in getattr(parent, group).items()}
                         for group in ('files', 'resume_files')}}
        return identity(['demiflow.fit.v1', self.project_id, {k: v.identity() for k,v in self.inputs.items()},
                         _plain(self.semantics), parent])

    @property
    def instance_id(self):
        return identity(['demiflow.fit.instance.v1', self.fit_key, self.retrain_id])

    @property
    def parameters(self):
        return self.semantics['parameters']

    def for_resume(self, snapshot):
        return replace(self, restart_snapshot=snapshot)


@dataclass(frozen=True)
class FitResult(_Value):
    wire_schema = 'demiflow.fit_result.v1'

    instance_id: str
    fit_key: str
    snapshots: tuple[TrainingSnapshotRef, ...]
    final_step: int
    metrics: Mapping = field(default_factory=dict)

    def __post_init__(self):
        _digest(self.instance_id)
        _digest(self.fit_key)
        _number(self.final_step, "final step", positive=True)
        if type(self.snapshots) not in (tuple, list) or not 1 <= len(self.snapshots) <= 16384:
            raise ValueError("Invalid training snapshot count")
        snapshots = tuple(v if type(v) is TrainingSnapshotRef else TrainingSnapshotRef.from_dict(v)
                          for v in self.snapshots)
        steps = [v.step for v in snapshots]
        if steps != sorted(set(steps)) or steps[-1] != self.final_step:
            raise ValueError("Snapshot steps must be unique, increasing and include the final step")
        object.__setattr__(self, 'snapshots', snapshots)
        object.__setattr__(self, 'metrics', _json_map(self.metrics))
        document(self.to_dict())

    def select(self, steps):
        by_step = {item.step: item for item in self.snapshots}
        if not steps or len(set(steps)) != len(steps) or any(type(i) is not int or i not in by_step for i in steps):
            raise ValueError(f"Invalid model selection; available steps: {tuple(by_step)}")
        return tuple(by_step[step] for step in steps)


@dataclass(frozen=True)
class FitBinding(_Value):
    project_dir: str
    model_store_uri: str
    provider_scope: str
    hosting: str
    max_model_bytes: int
    max_files: int
    max_instances: int
    max_resumes: int = 0
    max_charge: int = 0
    project_charge_limit: int = 0
    currency: str | None = None
    idempotent_submit: bool = False
    provider_options: Mapping = field(default_factory=dict)

    max_references: int = 16384
    input_destinations: Mapping = field(default_factory=dict)
    implementation_digest: str | None = None
    provider_timeout_s: float = 30.0

    def __post_init__(self):
        for name in ('project_dir','model_store_uri','provider_scope'):
            _text(getattr(self, name), name)
        if self.hosting == 'ProviderManaged' or self.implementation_digest is not None:
            _digest(self.implementation_digest)
        if type(self.provider_timeout_s) not in (int,float) or not 0 < self.provider_timeout_s <= 3600:
            raise ValueError("Invalid per-provider operation timeout")
        if self.hosting not in ('EngineManaged','ProviderManaged'):
            raise ValueError("Unknown training resource owner")
        for name in ('max_model_bytes','max_files','max_instances','max_references'):
            _number(getattr(self, name), name, positive=True)
        for name in ('max_resumes','max_charge','project_charge_limit'):
            _number(getattr(self,name),name)
        if self.max_resumes > 16384:
            raise ValueError("Training restart count exceeds launch metadata budget")
        if self.max_charge > self.project_charge_limit or (self.max_charge and
                (self.hosting != 'ProviderManaged' or type(self.currency) is not str or
                 not re.fullmatch(r'[A-Z]{3}', self.currency))):
            raise ValueError("Invalid provider spending limit")
        if type(self.idempotent_submit) is not bool:
            raise ValueError("Invalid provider idempotence capability")
        object.__setattr__(self,'provider_options',_json_map(self.provider_options))
        destinations = _json_map(self.input_destinations)
        if destinations and self.hosting != 'ProviderManaged':
            raise ValueError("Input transport is a provider hosting capability")
        for role, value in destinations.items():
            _text(role, 'copy role')
            _text(value, 'input copy destination')
        object.__setattr__(self, 'input_destinations', destinations)


@dataclass(frozen=True)
class JobRef(_Value):
    scope: str
    job_id: str

    def __post_init__(self):
        _text(self.scope,'provider scope')
        _text(self.job_id,'job ID')


@dataclass(frozen=True)
class JobLookup(_Value):
    status: str
    job: JobRef | None = None
    evidence: Mapping | None = None

    def __post_init__(self):
        if self.status not in ('found', 'confirmed_absent', 'unknown'):
            raise ValueError("Invalid job lookup status")
        if self.job is not None and type(self.job) is not JobRef:
            object.__setattr__(self, 'job', JobRef.from_dict(self.job))
        if (self.status == 'found') != (self.job is not None):
            raise ValueError("Job lookup result does not match status")
        if self.evidence is not None:
            object.__setattr__(self,'evidence',_json_map(self.evidence))
        if self.status != 'unknown' and not self.evidence:
            raise ValueError("A conclusive lookup requires a receipt")


@dataclass(frozen=True)
class InputCopyReceipt(_Value):
    scope: str
    request_key: str
    destination: str
    dataset: FixedDatasetRef

    def __post_init__(self):
        _text(self.scope, 'copy scope')
        _digest(self.request_key)
        _text(self.destination, 'copy destination')
        if type(self.dataset) is not FixedDatasetRef:
            object.__setattr__(self, 'dataset', FixedDatasetRef.from_dict(self.dataset))


class FitInputTransfer(Protocol):
    def stage_input(self, source: FixedDatasetRef, destination: str, request_key: str, *, deadline: float) -> InputCopyReceipt: ...
    def lookup_input(self, request_key: str, *, deadline: float) -> InputCopyReceipt | None: ...


class FitSubmissionRejected(RuntimeError):
    """Managed host proved this original allocation never admitted a writer."""


class FitJobProvider(Protocol):
    def submit(self, request: FitRequest, binding: FitBinding, request_key: str, *, deadline: float) -> JobRef: ...
    def lookup(self, request_key: str, *, deadline: float) -> JobLookup: ...
    def inspect(self, job: JobRef, *, deadline: float) -> str: ...
    def result(self, job: JobRef, *, deadline: float) -> FitResult: ...
    def cancel(self, job: JobRef, *, deadline: float) -> None: ...


class FitContext(Protocol):
    work_dir: Path
    model_dir: Path
    request_key: str
    binding: FitBinding

    def check_cancelled(self) -> None: ...
    def report_progress(self, metrics: Mapping) -> None: ...
    def report_snapshot(self, snapshot: TrainingSnapshotRef) -> None: ...
    def run_group(self, entry: str, args: Sequence[str], deadline: float) -> None: ...
