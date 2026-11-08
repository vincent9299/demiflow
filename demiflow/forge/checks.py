"""Mechanical project checks; business semantics still require tests and review."""
from __future__ import annotations

import ast
import os
from pathlib import Path
import re

from .contract import build_context, load_contract
from .definition import (
    definition_from_request, inspect_operators, load_definition, load_request,
    normalize_run_config,
)
from .io import read_json
from .manifest import build_manifest
from .schema import ForgeError
from .snapshot import revision, snapshot


_FORBIDDEN_CALLS = {
    "eval", "exec", "compile", "__import__", "open", "map_prompt",
    "map_prompt_async", "agentmap_async", "read_lance", "write_lance",
    "from_items", "from_iter", "run_pipeline", "Popen", "system", "urlopen",
}
_SECRET_NAME = re.compile(r"(api_?key|secret|token|password)", re.IGNORECASE)


def check_project(project: Path) -> dict:
    project = project.absolute()
    issues = []
    contract = load_contract()
    files = None
    try:
        files = snapshot(project, contract)
    except (ForgeError, OSError) as exc:
        issues.append(_issue("snapshot_invalid", str(exc), ""))
    if files is None:
        return {
            "schema_version": "demiflow_forge_check_v1",
            "passed": False,
            "issues": issues,
            "revision": None,
        }
    _check_layout(project, contract, issues)
    definition = request = operators = None
    try:
        definition = load_definition(project)
    except (ForgeError, OSError) as exc:
        issues.append(_issue("definition_invalid", str(exc), "pipeline.py"))
    try:
        request = load_request(project)
    except (ForgeError, OSError) as exc:
        issues.append(_issue("request_invalid", str(exc), "request.json"))
    if definition is not None and request is not None and definition.to_dict() != definition_from_request(request):
        issues.append(_issue("request_definition_drift", "PipelineDefinition no longer matches request.json", "pipeline.py"))
    try:
        operators = inspect_operators(project)
    except (ForgeError, OSError) as exc:
        issues.append(_issue("operator_invalid", str(exc), "operators"))
    _check_operator_sources(project, contract, issues)
    _check_test_sources(project, issues)
    try:
        context = build_context()
        _check_context(project, context, issues)
    except (ForgeError, OSError, ImportError) as exc:
        issues.append(_issue("platform_context_invalid", str(exc), "contract.lock.json"))
    if definition is not None and operators is not None:
        try:
            expected = build_manifest(project)
            actual = read_json(project / "manifest.json")
            if actual != expected:
                issues.append(_issue("manifest_stale", "manifest.json is missing, edited, or stale", "manifest.json"))
        except (ForgeError, OSError, KeyError) as exc:
            issues.append(_issue("manifest_invalid", str(exc), "manifest.json"))
        try:
            normalize_run_config(read_json(project / "run.example.json"), definition)
        except (ForgeError, OSError) as exc:
            issues.append(_issue("run_config_invalid", str(exc), "run.example.json"))
    return {
        "schema_version": "demiflow_forge_check_v1",
        "passed": not issues,
        "issues": issues,
        "revision": revision(files) if files is not None else None,
    }


def current_revision(project: Path) -> str:
    return revision(snapshot(project.absolute(), load_contract()))


def _check_layout(project: Path, contract: dict, issues: list) -> None:
    if not project.is_dir():
        issues.append(_issue("project_missing", "Project directory is unavailable", ""))
        return
    layout = contract["layout"]
    for name in layout["root_files"]:
        if not (project / name).is_file():
            issues.append(_issue("layout_missing", f"Missing {name}", name))
    for name in layout["source_dirs"]:
        if not (project / name).is_dir():
            issues.append(_issue("layout_missing", f"Missing {name}/", name))
    if not (project / "operators" / "__init__.py").is_file():
        issues.append(_issue("layout_missing", "Missing operators/__init__.py", "operators/__init__.py"))
    allowed_roots = set(layout["root_files"] + layout["source_dirs"] + layout["runtime_dirs"] + layout["ignored_dirs"])
    try:
        for entry in sorted(os.scandir(project), key=lambda item: item.name):
            if entry.name not in allowed_roots:
                issues.append(_issue("layout_unregistered", "Unregistered project root entry", entry.name))
        for directory in layout["source_dirs"]:
            root = project / directory
            if not root.is_dir():
                continue
            for base, dirs, files in os.walk(root):
                dirs[:] = sorted(name for name in dirs if name not in layout["ignored_dirs"])
                if directory == "operators" and Path(base) != root:
                    issues.append(_issue("layout_nested_operator", "Nested operator modules are outside contract v1", str(Path(base).relative_to(project))))
                for name in sorted(files):
                    path = Path(base) / name
                    if path.suffix != ".py":
                        issues.append(_issue("layout_file_type", "Source directories allow Python files only", str(path.relative_to(project))))
    except OSError as exc:
        issues.append(_issue("layout_unreadable", str(exc), ""))


def _check_context(project: Path, context: dict, issues: list) -> None:
    try:
        binding = read_json(project / "project.json")
        expected = {
            "schema_version": "demiflow_forge_project_v1",
            "name": load_definition(project).to_dict()["name"],
            "contract_id": context["contract"]["contract_id"],
            "contract_version": context["contract"]["version"],
            "context_digest": context["context_digest"],
        }
        if binding != expected:
            issues.append(_issue("project_binding_stale", "project.json does not match the current built-in contract", "project.json"))
        if read_json(project / "contract.lock.json") != context:
            issues.append(_issue("contract_context_stale", "contract.lock.json is edited or stale", "contract.lock.json"))
    except (ForgeError, OSError, KeyError) as exc:
        issues.append(_issue("contract_context_invalid", str(exc), "contract.lock.json"))


def _check_operator_sources(project: Path, contract: dict, issues: list) -> None:
    root = project / "operators"
    for path in sorted(root.glob("*.py")):
        try:
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text, filename=path.name)
        except (OSError, UnicodeError, SyntaxError) as exc:
            issues.append(_issue("operator_source_invalid", str(exc), str(path.relative_to(project))))
            continue
        if "TODO" in text or "NotImplementedError" in text:
            issues.append(_issue("operator_unimplemented", "Operator contains an unfinished implementation", str(path.relative_to(project))))
        if path.name == "__init__.py":
            if text:
                issues.append(_issue("operator_init_not_empty", "operators/__init__.py must be empty", str(path.relative_to(project))))
            continue
        for statement in tree.body:
            if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant) and isinstance(statement.value.value, str):
                continue
            if not isinstance(statement, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.Assign, ast.AnnAssign)):
                issues.append(_issue("operator_module_scope", "Operator module contains executable module-scope code", str(path.relative_to(project)), getattr(statement, "lineno", 0)))
            if isinstance(statement, (ast.Assign, ast.AnnAssign)):
                try:
                    ast.literal_eval(statement.value)
                except (TypeError, ValueError):
                    issues.append(_issue("operator_module_scope", "Operator module constants must be static literals", str(path.relative_to(project)), statement.lineno))
            if isinstance(statement, ast.FunctionDef):
                for decorator in statement.decorator_list:
                    if not (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name) and decorator.func.id == "operator"):
                        issues.append(_issue("operator_decorator_forbidden", "Only the built-in @operator decorator is allowed", str(path.relative_to(project)), decorator.lineno))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                modules = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
                for module in modules:
                    root_name = module.split(".")[0]
                    special = isinstance(node, ast.ImportFrom) and node.module == "demiflow.forge" and [alias.name for alias in node.names] == ["operator"]
                    if root_name not in contract["operator_imports"] and not special:
                        issues.append(_issue("contract_import_forbidden", f"Import is outside the operator contract: {module}", str(path.relative_to(project)), node.lineno))
            if isinstance(node, ast.ClassDef):
                issues.append(_issue("operator_class_forbidden", "Contract v1 operators are plain functions", str(path.relative_to(project)), node.lineno))
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in _FORBIDDEN_CALLS:
                    issues.append(_issue("contract_call_forbidden", f"Call is outside the operator contract: {node.func.id}", str(path.relative_to(project)), node.lineno))
                if isinstance(node.func, ast.Attribute) and node.func.attr in _FORBIDDEN_CALLS:
                    issues.append(_issue("contract_call_forbidden", f"Call is outside the operator contract: {node.func.attr}", str(path.relative_to(project)), node.lineno))
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str) and node.value.value:
                if any(isinstance(target, ast.Name) and _SECRET_NAME.search(target.id) for target in node.targets):
                    issues.append(_issue("contract_secret_literal", "Credential-like literal is forbidden", str(path.relative_to(project)), node.lineno))
        if "PRIVATE KEY-----" in text:
            issues.append(_issue("contract_secret_literal", "Private key content is forbidden", str(path.relative_to(project))))


def _check_test_sources(project: Path, issues: list) -> None:
    for path in sorted((project / "tests").rglob("*.py")):
        try:
            text = path.read_text(encoding="utf-8")
            ast.parse(text, filename=str(path))
        except (OSError, UnicodeError, SyntaxError) as exc:
            issues.append(_issue("test_source_invalid", str(exc), str(path.relative_to(project))))
            continue
        if "TODO" in text or "NotImplementedError" in text:
            issues.append(_issue("tests_unimplemented", "Tests contain unfinished work", str(path.relative_to(project))))


def _issue(code: str, message: str, path: str, line: int = 0) -> dict:
    return {"code": code, "message": message, "path": path, "line": line}


__all__ = ["check_project", "current_revision"]
