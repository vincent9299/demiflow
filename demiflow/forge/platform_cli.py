"""CLI for interface facts; independent of the legacy business lifecycle."""
import json
from pathlib import Path

import click

from .platform import PUBLIC_MODULES, discover_catalog
from .platform_snapshot import build_snapshot, check_snapshot, write_snapshot, snapshot_complete, _diagnostics
from .schema import ForgeError


def output(callback, pretty):
    try:
        value = callback()
    except (ForgeError, OSError, ValueError, TypeError, RuntimeError, ImportError) as exc:
        value = {"status": "incomplete", "code": getattr(exc, 'code', 'operation_failed'),
                 "diagnostics": [{"code": "inspection_failed", "detail": str(exc)}]}
    click.echo(json.dumps(value, ensure_ascii=False, indent=2 if pretty else None))
    if value.get("status") in {"stale", "incomplete", "invalid"}:
        raise click.exceptions.Exit(1)


def _page(items, offset, limit):
    return {"total": len(items), "offset": offset, "limit": limit,
            "has_more": offset + limit < len(items)}


def register(forge):
    modules_option = click.option("--module", "modules", multiple=True, type=click.Choice(PUBLIC_MODULES))
    json_option = click.option("--json", "pretty", is_flag=True, help="Emit indented JSON.")
    limit_option = click.option("--limit", default=50, type=click.IntRange(1, 200), show_default=True)
    offset_option = click.option("--offset", default=0, type=click.IntRange(min=0), show_default=True)

    @forge.group("platform")
    def platform_group():
        """Discover native interfaces and compare platform content snapshots."""

    @platform_group.command("list")
    @modules_option
    @click.option("--name", default="", help="Substring filter on public interface IDs.")
    @offset_option
    @limit_option
    @json_option
    def list_objects(modules, name, offset, limit, pretty):
        def perform():
            catalog = discover_catalog(modules or None)
            matches = [item for item in catalog["objects"] if name in item["id"]]
            items = [{key: item[key] for key in ("id", "kind", "origin", "diagnostics")} for item in matches[offset:offset+limit]]
            return {"status": "discovered" if catalog["inspection_complete"] else "incomplete",
                    "inspection_complete": catalog["inspection_complete"], "modules": catalog["modules"],
                    "discovered_total": len(catalog["objects"]), "page": _page(matches, offset, limit),
                    "objects": items, "diagnostics": catalog["diagnostics"]}
        output(perform, pretty)

    @platform_group.command("show")
    @click.argument("identifier")
    @json_option
    def show_object(identifier, pretty):
        def perform():
            module = identifier.split(":", 1)[0]
            if module not in PUBLIC_MODULES or ":" not in identifier:
                return {"status": "invalid", "diagnostics": [{"code": "unknown_interface", "id": identifier}]}
            catalog = discover_catalog([module])
            item = next((item for item in catalog["objects"] if item["id"] == identifier), None)
            return {"status": "incomplete" if not catalog["inspection_complete"] else "discovered" if item else "invalid",
                    "inspection_complete": catalog["inspection_complete"], "object": item,
                    "diagnostics": catalog["diagnostics"] if item else [*catalog["diagnostics"], {"code": "unknown_interface", "id": identifier}]}
        output(perform, pretty)

    @platform_group.command("snapshot")
    @modules_option
    @click.option("--extra", "extras", multiple=True, help="Explicitly include a declared platform extra.")
    @click.option("--out", "path", required=True, type=click.Path(path_type=Path, dir_okay=False))
    @json_option
    def snapshot(modules, extras, path, pretty):
        def perform():
            value = build_snapshot(modules or None, extras=extras)
            if not snapshot_complete(value):
                return {"status": "incomplete", "diagnostics": _diagnostics(value)}
            write_snapshot(path, value)
            return {"status": "created", "path": str(path.absolute()), "scope": value["scope"],
                    "digests": value["digests"], "objects": len(value["catalog"]["objects"]),
                    "files": len(value["platform"]["files"]), "dependency_assurance": "metadata_only"}
        output(perform, pretty)

    @platform_group.command("check")
    @click.argument("path", type=click.Path(path_type=Path, dir_okay=False))
    @offset_option
    @limit_option
    @json_option
    def check(path, offset, limit, pretty):
        def perform():
            result = check_snapshot(path)
            changes = result["changes"]
            counts = {}
            for change in changes:
                counts[change["kind"]] = counts.get(change["kind"], 0) + 1
            return {**result, "change_counts": counts, "page": _page(changes, offset, limit),
                    "changes": changes[offset:offset+limit]}
        output(perform, pretty)
