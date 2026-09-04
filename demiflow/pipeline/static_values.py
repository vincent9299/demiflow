"""Resolve import-pure Candidate literals without executing generated code."""
from __future__ import annotations

import ast
from typing import Any

from .core import PipelineContractDiagnostic


def inspect_static_value(
    value: ast.AST,
    tree: ast.Module,
    *,
    class_owner: ast.ClassDef | None = None,
    bare_name_scope: str = "module",
    field_path: str = "",
) -> tuple[Any, tuple[PipelineContractDiagnostic, ...]]:
    """Collect independent sibling literal errors using the strict resolver."""
    issues: list[PipelineContractDiagnostic] = []

    def inspect(node: ast.AST, path: str) -> Any:
        if isinstance(node, ast.Dict):
            output = {}
            for index, (key, item) in enumerate(zip(node.keys, node.values)):
                if key is None:
                    issues.append(_diagnostic(
                        node, "static mapping cannot contain dictionary unpacking",
                        f"{path}.<key:{index}>",
                    ))
                    continue
                try:
                    key_value = _resolve_static_value(
                        key, tree, class_owner=class_owner,
                        bare_name_scope=bare_name_scope, resolving=frozenset(),
                    )
                except ValueError as exc:
                    issues.append(_diagnostic(key, str(exc), f"{path}.<key:{index}>"))
                    inspect(item, f"{path}[{index}]")
                    continue
                try:
                    hash(key_value)
                except TypeError:
                    issues.append(_diagnostic(
                        key, "static mapping key must be hashable",
                        f"{path}.<key:{index}>",
                    ))
                    inspect(item, f"{path}[{index}]")
                    continue
                output[key_value] = inspect(item, f"{path}.{key_value}")
            return output
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            values = [inspect(item, f"{path}[{index}]") for index, item in enumerate(node.elts)]
            return tuple(values) if isinstance(node, ast.Tuple) else set(values) if isinstance(node, ast.Set) else values
        try:
            return _resolve_static_value(
                node, tree, class_owner=class_owner,
                bare_name_scope=bare_name_scope, resolving=frozenset(),
            )
        except ValueError as exc:
            issues.append(_diagnostic(node, str(exc), path))
            return None

    return inspect(value, field_path), tuple(issues)


def _diagnostic(node: ast.AST, message: str, field_path: str) -> PipelineContractDiagnostic:
    return PipelineContractDiagnostic(
        "static_value_not_literal", message,
        line=int(getattr(node, "lineno", 0) or 0),
        column=int(getattr(node, "col_offset", 0) or 0),
        field_path=field_path,
    )


def resolve_static_value(
    value: ast.AST,
    tree: ast.Module,
    *,
    class_owner: ast.ClassDef | None = None,
    bare_name_scope: str = "module",
) -> Any:
    value, diagnostics = inspect_static_value(
        value, tree, class_owner=class_owner,
        bare_name_scope=bare_name_scope,
    )
    if diagnostics:
        raise ValueError("; ".join(item.message for item in diagnostics))
    return value


def _resolve_static_value(
    value: ast.AST,
    tree: ast.Module,
    *,
    class_owner: ast.ClassDef | None,
    bare_name_scope: str,
    resolving: frozenset[tuple[str, str]],
) -> Any:
    try:
        return ast.literal_eval(value)
    except (TypeError, ValueError):
        pass
    if isinstance(value, ast.Dict):
        return {
            _resolve_static_value(
                key, tree, class_owner=class_owner,
                bare_name_scope=bare_name_scope, resolving=resolving,
            ): _resolve_static_value(
                item, tree, class_owner=class_owner,
                bare_name_scope=bare_name_scope, resolving=resolving,
            )
            for key, item in zip(value.keys, value.values)
            if key is not None
        }
    if isinstance(value, (ast.List, ast.Tuple, ast.Set)):
        items = [
            _resolve_static_value(
                item, tree, class_owner=class_owner,
                bare_name_scope=bare_name_scope, resolving=resolving,
            )
            for item in value.elts
        ]
        if isinstance(value, ast.Tuple):
            return tuple(items)
        if isinstance(value, ast.Set):
            return set(items)
        return items
    if isinstance(value, ast.Name):
        if bare_name_scope not in {"module", "class"}:
            raise ValueError(f"unsupported static name scope: {bare_name_scope}")
        assigned = None
        scope = "module"
        if bare_name_scope == "class" and class_owner is not None:
            assigned = _find_assignment(class_owner.body, value.id, before=value)
            if assigned is not None:
                scope = "class"
        if assigned is None:
            module_before = class_owner if bare_name_scope == "class" else None
            assigned = _find_assignment(tree.body, value.id, before=module_before)
        if assigned is None:
            raise ValueError(f"static constant is not assigned before use: {value.id}")
        key = (scope, value.id)
        if key in resolving:
            raise ValueError(f"static constant reference cycle: {value.id}")
        return _resolve_static_value(
            assigned, tree, class_owner=class_owner,
            bare_name_scope=bare_name_scope, resolving=resolving | {key},
        )
    if (
        isinstance(value, ast.Attribute)
        and isinstance(value.value, ast.Name)
        and value.value.id in {"self", "cls"}
        and class_owner is not None
    ):
        assigned = _find_assignment(class_owner.body, value.attr)
        if assigned is None:
            raise ValueError(f"class constant is not assigned: {value.attr}")
        key = ("class", value.attr)
        if key in resolving:
            raise ValueError(f"static constant reference cycle: {value.attr}")
        return _resolve_static_value(
            assigned, tree, class_owner=class_owner,
            bare_name_scope=bare_name_scope, resolving=resolving | {key},
        )
    raise ValueError("value is not a static literal")


def _find_assignment(
    statements: list[ast.stmt], name: str, *, before: ast.AST | None = None,
) -> ast.AST | None:
    found: ast.AST | None = None
    before_line = int(getattr(before, "lineno", 1 << 30))
    for statement in statements:
        if int(getattr(statement, "lineno", 0)) >= before_line:
            continue
        value: ast.AST | None = None
        matches = False
        if isinstance(statement, ast.Assign):
            matches = any(
                isinstance(target, ast.Name) and target.id == name
                for target in statement.targets
            )
            value = statement.value
        elif isinstance(statement, ast.AnnAssign):
            matches = (
                statement.value is not None
                and isinstance(statement.target, ast.Name)
                and statement.target.id == name
            )
            value = statement.value
        if matches:
            if found is not None:
                raise ValueError(f"static constant is assigned more than once: {name}")
            found = value
    return found


__all__ = ["inspect_static_value", "resolve_static_value"]
