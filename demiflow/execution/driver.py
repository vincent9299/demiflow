"""Pipeline Driver shared by Local subprocess and Ray Job execution."""
from __future__ import annotations

import argparse
import importlib
import json
import re
import os
import sys
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from demiflow._compat.error_transport import error_from_exception, validate_error
from demiflow._compat.observability import setup_logging

from ..operator_llm import load_referenced_prompt_packs, required_environment
from ..pipeline import Pipeline, discover_pipeline_definition
from .contracts import PipelineBundleRef
from ..planning.policy import BUILT_IN_RULE_VERSION, parse_platform_planning_policy
from .executors.local import LocalDatasetExecutor
from .package_loader import load_bundle_packages


@dataclass(frozen=True)
class PipelineDriverRequest:
    run_id: str
    bundle: PipelineBundleRef
    bundle_root: str
    backend: str
    platform_preflight_imports: tuple[str, ...]
    planning_policy: Mapping[str, Any]
    planning_policy_digest: str
    planning_rule_version: str
    namespace: str = ""
    bundle_namespace: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "demiflow_pipeline_driver_v9",
            "run_id": self.run_id,
            "bundle": asdict(self.bundle),
            "bundle_root": self.bundle_root,
            "backend": self.backend,
            "namespace": self.namespace,
            "bundle_namespace": self.bundle_namespace,
            "platform_preflight_imports": list(self.platform_preflight_imports),
            "planning_policy": dict(self.planning_policy),
            "planning_policy_digest": self.planning_policy_digest,
            "planning_rule_version": self.planning_rule_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PipelineDriverRequest":
        allowed = {
            "schema_version", "run_id", "bundle", "bundle_root", "backend",
            "namespace", "bundle_namespace", "platform_preflight_imports",
            "planning_policy", "planning_policy_digest", "planning_rule_version",
        }
        extra = set(value) - allowed
        if extra:
            raise ValueError(
                "PipelineDriverRequest contains unsupported fields: "
                f"{sorted(extra)}"
            )
        if value.get("schema_version") != "demiflow_pipeline_driver_v9":
            raise ValueError("unsupported PipelineDriverRequest schema")
        bundle_value = dict(value["bundle"])
        expected = {"root", "content_digest", "runtime_abi"}
        if set(bundle_value) != expected:
            raise ValueError("invalid PipelineBundleRef fields")
        imports = value.get("platform_preflight_imports")
        if not isinstance(imports, list) or not imports or any(
            not isinstance(item, str)
            or re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", item,
            ) is None
            for item in imports
        ) or len(set(imports)) != len(imports):
            raise ValueError("invalid platform_preflight_imports")
        return cls(
            run_id=str(value["run_id"]),
            bundle=PipelineBundleRef(**bundle_value),
            bundle_root=str(value["bundle_root"]),
            backend=str(value["backend"]),
            namespace=str(value.get("namespace") or ""),
            bundle_namespace=str(value.get("bundle_namespace") or ""),
            platform_preflight_imports=tuple(imports),
            planning_policy=dict(value["planning_policy"]),
            planning_policy_digest=str(value["planning_policy_digest"]),
            planning_rule_version=str(value["planning_rule_version"]),
        )


def execute_driver(request: PipelineDriverRequest) -> None:
    bundle_root = Path(request.bundle_root).expanduser()
    if not bundle_root.is_absolute():
        bundle_root = Path.cwd() / bundle_root
    bundle_root = bundle_root.resolve()
    bundle = PipelineBundleRef(
        root=str(bundle_root), content_digest=request.bundle.content_digest,
        runtime_abi=request.bundle.runtime_abi,
    )
    bundle.verify()
    definition = discover_pipeline_definition(bundle_root)
    policy=parse_platform_planning_policy(request.planning_policy)
    if policy.digest != request.planning_policy_digest:
        raise ValueError("Driver planning policy digest mismatch")
    if request.planning_rule_version != BUILT_IN_RULE_VERSION:
        raise ValueError("Driver planning rule version mismatch")
    for module_name in request.platform_preflight_imports:
        importlib.import_module(module_name)
    packages = load_bundle_packages(
        bundle_root, namespace=request.bundle_namespace or None,
    )
    prompt_packs = load_referenced_prompt_packs(bundle_root)
    required_environment(tuple(sorted({
        name for pack in prompt_packs.values()
        for name in pack.required_environment_names
    })))
    pipeline = Pipeline.load(bundle_root, packages.pipeline_package)
    if definition.execution.backend_affinity and definition.execution.backend_affinity != request.backend:
        raise ValueError("Driver backend does not match Pipeline native affinity")
    if request.backend == "local" and request.namespace:
        raise ValueError("Local Driver request cannot contain a Ray namespace")

    with ExitStack() as resources:
        if request.backend == "local":
            dataset_executor = LocalDatasetExecutor(
                workers=max(1,min(policy.max_parallelism,os.cpu_count() or 1)),
                prompt_packs=prompt_packs,
                planning_policy=policy,
                candidate_execution=definition.execution,
            )
        elif request.backend == "ray":
            import ray
            if not ray.is_initialized():
                ray.init(
                    address="auto", namespace=request.namespace,
                    ignore_reinit_error=True, logging_level="ERROR",
                )
            from .executors.ray import RayDatasetExecutor
            dataset_executor = RayDatasetExecutor(
                prompt_packs=prompt_packs, planning_policy=policy,
                candidate_execution=definition.execution,
            )
        else:
            raise ValueError(f"unsupported Driver backend: {request.backend}")
        resources.callback(dataset_executor.close)
        pipeline.run(dataset_executor=dataset_executor)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request")
    args = parser.parse_args(argv)
    setup_logging(force=True)
    raw = (
        Path(args.request).read_text(encoding="utf-8")
        if args.request else sys.stdin.read()
    )
    request = PipelineDriverRequest.from_dict(json.loads(raw))
    try:
        execute_driver(request)
    except Exception as exc:
        context = None
        if hasattr(exc, "responsibility") and hasattr(exc, "code"):
            context = {
                "code": str(exc.code),
                "responsibility": str(exc.responsibility),
                "stage_ordinal": int(getattr(exc, "stage_ordinal", -1)),
            }
        print(json.dumps(_driver_error(exc, context=context), ensure_ascii=False), file=sys.stderr)
        return 1
    return 0


def _driver_error(exc: Exception, *, context=None) -> dict[str, Any]:
    raw = getattr(exc, "error", None)
    if isinstance(raw, Mapping):
        value = validate_error(raw)
    else:
        value = error_from_exception(exc)
    merged = dict(value["context"])
    if context:
        merged.update(context)
    if bool(getattr(exc, "reconciliation_required", False)):
        merged["reconciliation_required"] = True
    receipt = getattr(exc, "receipt", None)
    if receipt is not None and callable(getattr(receipt, "to_dict", None)):
        receipt_value = dict(receipt.to_dict())
        receipt_value.pop("error", None)
        merged["write_receipt"] = receipt_value
    return validate_error({**value, "context": merged})


if __name__ == "__main__":
    raise SystemExit(main())
