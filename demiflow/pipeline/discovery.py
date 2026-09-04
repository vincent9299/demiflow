"""Statically discover one PipelineProgram and its Candidate-owned execution."""
from __future__ import annotations

import ast
import importlib
import re
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

from .core import (
    PipelineContractDiagnostic, PipelineContractError, PipelineExecution,
    parse_pipeline_execution, validate_pipeline_execution,
)
from .operator import PipelineProgram
from .static_values import inspect_static_value

_ENTRYPOINT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
_RESOURCE_SUFFIXES = frozenset({".yaml", ".yml"})


@dataclass(frozen=True)
class PipelineDefinition:
    entrypoint: str
    execution: PipelineExecution
    execution_resource: str = ""


@dataclass(frozen=True)
class ProgramDeclaration:
    source_path: str
    class_name: str
    run_line: int
    run_column: int


@dataclass(frozen=True)
class PipelineInspection:
    definition: PipelineDefinition | None
    declaration: ProgramDeclaration | None = None
    diagnostics: tuple[PipelineContractDiagnostic, ...] = ()


def inspect_pipeline_definition(
    bundle_root: str | Path, *, reject_nested_programs: bool = False,
) -> PipelineInspection:
    root = Path(bundle_root).expanduser().resolve()
    pipeline_path = root / "pipeline"
    issues: list[PipelineContractDiagnostic] = []
    if not pipeline_path.is_dir() or pipeline_path.is_symlink():
        return PipelineInspection(None, diagnostics=(PipelineContractDiagnostic(
            "pipeline_package_missing",
            "Pipeline bundle requires a regular pipeline/ package",
            path="pipeline",
        ),))
    pipeline = pipeline_path.resolve()
    declarations: list[tuple[Path, ast.Module, ast.ClassDef]] = []
    for path in sorted(pipeline.rglob("*.py") if reject_nested_programs else pipeline.glob("*.py")):
        if path.name == "__init__.py" or not path.is_file() or path.is_symlink():
            continue
        relative = _relative(root, path)
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=relative)
        except (OSError, UnicodeError, SyntaxError) as exc:
            issues.append(PipelineContractDiagnostic(
                "pipeline_source_invalid",
                f"Pipeline source cannot be parsed: {exc}",
                path=relative, line=int(getattr(exc, "lineno", 0) or 0),
                column=int(getattr(exc, "offset", 0) or 0),
            ))
            continue
        for node in tree.body:
            aliases = _pipeline_program_aliases_before(tree, node)
            if not isinstance(node, ast.ClassDef) or not _direct_program(node, aliases):
                continue
            declarations.append((path, tree, node))
            if reject_nested_programs and len(path.relative_to(pipeline).parts) != 1:
                issues.append(PipelineContractDiagnostic(
                    "nested_pipeline_program_forbidden",
                    "PipelineProgram must be declared in a top-level pipeline Python module",
                    path=relative, line=node.lineno, column=node.col_offset,
                ))
            if len(node.bases) != 1 or node.keywords or node.decorator_list:
                issues.append(PipelineContractDiagnostic(
                    "pipeline_program_declaration_invalid",
                    "PipelineProgram must directly inherit only PipelineProgram without decorators or class keywords",
                    path=relative, line=node.lineno, column=node.col_offset,
                ))
            if any(part.startswith("_") for part in path.relative_to(pipeline).with_suffix("").parts) or node.name.startswith("_"):
                issues.append(PipelineContractDiagnostic(
                    "pipeline_program_private",
                    "PipelineProgram module and class cannot be private",
                    path=relative, line=node.lineno, column=node.col_offset,
                ))
            run_methods = [
                item for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name == "run"
            ]
            if len(run_methods) != 1:
                issues.append(PipelineContractDiagnostic(
                    "pipeline_program_run_invalid",
                    "PipelineProgram must define exactly one synchronous run(self, ctx) method",
                    path=relative, line=node.lineno, column=node.col_offset,
                ))
            elif not _valid_run(run_methods[0]):
                issues.append(PipelineContractDiagnostic(
                    "pipeline_program_run_invalid",
                    "PipelineProgram.run must be synchronous with signature run(self, ctx) and no decorators",
                    path=relative, line=run_methods[0].lineno,
                    column=run_methods[0].col_offset,
                ))
            constructor = _method(node, "__init__")
            if constructor is not None:
                issues.append(PipelineContractDiagnostic(
                    "pipeline_program_constructor_forbidden",
                    "PipelineProgram cannot define __init__; use static declarations and run(ctx)",
                    path=relative, line=constructor.lineno,
                    column=constructor.col_offset,
                ))

    if len(declarations) != 1:
        issues.append(PipelineContractDiagnostic(
            "pipeline_program_count_invalid",
            "Pipeline package must define exactly one top-level concrete PipelineProgram; "
            f"found {len(declarations)}",
            path="pipeline",
        ))
        return PipelineInspection(None, diagnostics=tuple(issues))

    path, tree, owner = declarations[0]
    relative = _relative(root, path)
    execution_nodes = _class_assignments(owner, "execution")
    if len(execution_nodes) != 1:
        issues.append(PipelineContractDiagnostic(
            "execution_declaration_invalid",
            "Concrete PipelineProgram must assign execution exactly once",
            path=relative, line=owner.lineno, column=owner.col_offset,
            field_path="execution",
        ))
        return PipelineInspection(None, diagnostics=tuple(issues))
    execution_node = execution_nodes[0]
    raw_execution, value_issues = inspect_static_value(
        execution_node, tree, class_owner=owner, bare_name_scope="class",
        field_path="execution",
    )
    if value_issues:
        issues.append(PipelineContractDiagnostic(
            "execution_declaration_not_static",
            "PipelineProgram.execution must be a literal mapping or YAML basename: "
            + "; ".join(item.message for item in value_issues),
            path=relative, line=execution_node.lineno,
            column=execution_node.col_offset, field_path="execution",
        ))
        issues.extend(replace(item, path=item.path or relative) for item in value_issues)
        return PipelineInspection(None, diagnostics=tuple(issues))

    execution_resource = ""
    execution_label = f"{relative}:{owner.name}.execution"
    if isinstance(raw_execution, str):
        resource_result = _inspect_execution_resource(
            root, pipeline, raw_execution, relative, execution_node,
        )
        if isinstance(resource_result, PipelineContractDiagnostic):
            issues.append(resource_result)
            return PipelineInspection(None, diagnostics=tuple(issues))
        target, execution_resource = resource_result
        execution_label = execution_resource
        try:
            raw_execution = yaml.safe_load(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            issues.append(PipelineContractDiagnostic(
                "execution_resource_invalid",
                f"Execution YAML cannot be parsed: {exc}",
                path=execution_resource,
            ))
            return PipelineInspection(None, diagnostics=tuple(issues))
    execution_issues = tuple(
        replace(item, path=item.path or (execution_resource or relative))
        for item in validate_pipeline_execution(raw_execution, label=execution_label)
    )
    issues.extend(execution_issues)
    if execution_issues:
        return PipelineInspection(None, diagnostics=tuple(issues))
    module_parts = list(path.relative_to(pipeline).with_suffix("").parts)
    if module_parts[-1] == "__init__":
        module_parts = module_parts[:-1]
    definition = PipelineDefinition(
        entrypoint=f"{'.'.join(module_parts)}:{owner.name}",
        execution=parse_pipeline_execution(raw_execution, label=execution_label),
        execution_resource=execution_resource,
    )
    run = _method(owner, "run")
    declaration = ProgramDeclaration(
        source_path=relative,
        class_name=owner.name,
        run_line=int(getattr(run, "lineno", 0) or 0),
        run_column=int(getattr(run, "col_offset", 0) or 0),
    )
    return PipelineInspection(definition, declaration, tuple(issues))


def discover_pipeline_definition(bundle_root: str | Path) -> PipelineDefinition:
    inspection = inspect_pipeline_definition(bundle_root)
    if inspection.diagnostics or inspection.definition is None:
        diagnostics = inspection.diagnostics or (PipelineContractDiagnostic(
            "pipeline_definition_unavailable", "Pipeline definition is unavailable",
            path="pipeline",
        ),)
        raise PipelineContractError(diagnostics)
    return inspection.definition


def load_program(package: str | ModuleType, entrypoint: str) -> PipelineProgram:
    if not _ENTRYPOINT.fullmatch(str(entrypoint or "")):
        raise ValueError("Pipeline entrypoint must be a safe relative module:Class reference")
    module_name, class_name = entrypoint.split(":", 1)
    if any(part.startswith("_") for part in module_name.split(".")):
        raise ValueError("Pipeline entrypoint cannot reference private modules")
    package_name = package if isinstance(package, str) else package.__name__
    module = importlib.import_module(f".{module_name}", package_name)
    value = getattr(module, class_name, None)
    if not isinstance(value, type) or not issubclass(value, PipelineProgram):
        raise TypeError(f"Pipeline entrypoint {entrypoint!r} is not a PipelineProgram")
    if value is PipelineProgram or bool(getattr(value, "__abstractmethods__", ())):
        raise TypeError(f"Pipeline entrypoint {entrypoint!r} is abstract")
    return value()


def _pipeline_program_aliases_before(
    tree: ast.Module, target: ast.stmt,
) -> frozenset[str]:
    aliases: set[str] = set()
    for node in tree.body:
        if node is target:
            break
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module == "demiurge.demiflow":
            aliases.update(
                alias.asname or alias.name
                for alias in node.names if alias.name == "PipelineProgram"
            )
            continue
        aliases.difference_update(_bound_names(node))
    return frozenset(aliases)


def _bound_names(node: ast.stmt) -> set[str]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, ast.Import):
        return {alias.asname or alias.name.split(".", 1)[0] for alias in node.names}
    if isinstance(node, ast.ImportFrom):
        return {alias.asname or alias.name for alias in node.names}
    if isinstance(node, ast.Assign):
        return set().union(*(_target_names(target) for target in node.targets))
    if isinstance(node, ast.AnnAssign):
        return _target_names(node.target) if node.value is not None else set()
    if isinstance(node, ast.AugAssign):
        return _target_names(node.target)
    if isinstance(node, ast.Delete):
        return set().union(*(_target_names(target) for target in node.targets))
    return set()


def _target_names(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, (ast.Tuple, ast.List)):
        return set().union(*(_target_names(item) for item in node.elts))
    return set()

def _direct_program(node: ast.ClassDef, aliases: frozenset[str]) -> bool:
    return any(isinstance(base, ast.Name) and base.id in aliases for base in node.bases)


def _method(owner: ast.ClassDef, name: str) -> ast.AST | None:
    return next((
        item for item in owner.body
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == name
    ), None)


def _valid_run(node: ast.AST) -> bool:
    if not isinstance(node, ast.FunctionDef) or node.decorator_list:
        return False
    args = node.args
    return (
        len(args.posonlyargs) == 0
        and [item.arg for item in args.args] == ["self", "ctx"]
        and args.vararg is None
        and not args.kwonlyargs
        and args.kwarg is None
        and not args.defaults
    )


def _class_assignments(owner: ast.ClassDef, name: str) -> list[ast.AST]:
    values: list[ast.AST] = []
    for item in owner.body:
        if isinstance(item, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in item.targets
        ):
            values.append(item.value)
        elif (
            isinstance(item, ast.AnnAssign)
            and isinstance(item.target, ast.Name)
            and item.target.id == name
            and item.value is not None
        ):
            values.append(item.value)
    return values


def _inspect_execution_resource(root, pipeline, name, source_path, node):
    value = Path(name)
    if (
        value.is_absolute() or len(value.parts) != 1
        or value.name in {"", ".", ".."} or value.suffix not in _RESOURCE_SUFFIXES
    ):
        return PipelineContractDiagnostic(
            "execution_resource_path_invalid",
            "PipelineProgram.execution resource must be a top-level pipeline YAML basename",
            path=source_path, line=node.lineno, column=node.col_offset,
            field_path="execution",
        )
    candidate = pipeline / value
    relative = str(candidate.relative_to(root)).replace("\\", "/")
    if candidate.is_symlink():
        return PipelineContractDiagnostic(
            "execution_resource_symlink_forbidden",
            f"Execution resource symlinks are forbidden: {relative}",
            path=relative,
        )
    target = candidate.resolve()
    try:
        target.relative_to(pipeline)
    except ValueError as exc:
        return PipelineContractDiagnostic(
            "execution_resource_path_invalid", "Execution resource escapes pipeline package",
            path=source_path, line=node.lineno, column=node.col_offset,
        )
    if not target.is_file():
        return PipelineContractDiagnostic(
            "execution_resource_missing", f"Execution resource is unavailable: {relative}",
            path=relative,
        )
    return target, relative


def _relative(root: Path, path: Path) -> str:
    return str(path.resolve().relative_to(root)).replace("\\", "/")


__all__ = ["PipelineDefinition", "PipelineInspection", "ProgramDeclaration", "discover_pipeline_definition", "inspect_pipeline_definition", "load_program"]
