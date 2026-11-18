"""Package resources and the finite implementation used by task bindings.

Supported layouts are a source checkout and a normally unpacked wheel install.
No materialized temporary resource path participates in a binding.
"""
from importlib.resources import files
from pathlib import Path


DEVELOPMENT_ROOT = Path(__file__).resolve().parent
PACKAGE_ROOT = DEVELOPMENT_ROOT.parents[1]
CHECKOUT_ROOT = (PACKAGE_ROOT.parent if PACKAGE_ROOT.parent.name not in {"site-packages", "dist-packages"}
                 and (PACKAGE_ROOT.parent / "pyproject.toml").is_file() else None)
TOOL_ROOT = CHECKOUT_ROOT or PACKAGE_ROOT


def resource_path(name):
    resource = files(__package__).joinpath(name)
    try:
        return Path(resource)
    except TypeError as error:
        raise ValueError("Development resources require an unpacked package installation") from error


def configured_tool_root(root, value):
    if not isinstance(value, str) or not value:
        raise ValueError("tool_root must name the loaded demiflow checkout or package root")
    target = (Path(root) / value).resolve()
    if target not in {PACKAGE_ROOT, CHECKOUT_ROOT}:
        raise ValueError("Configured tool_root differs from the loaded demiflow development implementation")
    return target


def implementation_files():
    # Stable logical names, independent of checkout/site-packages placement.
    names = (
        "__init__.py", "data/__init__.py", "cli.py", "forge/__init__.py", "forge/cli.py", "forge/definition.py",
        "forge/io.py", "forge/schema.py", "pipeline/core.py",
        *("forge/development/" + name for name in (
            "__init__.py", "__main__.py", "coverage.py", "checks.py", "cli.py", "click_entry.py",
            "history.py", "io.py", "launcher.py", "notebook.py", "project.py",
            "resources.py", "runner.py", "tasks.py", "taskstore.py", "managed.py", "skill/SKILL.md", "skill/scripts/forge.py",
        )),
    )
    return {"demiflow/" + name: PACKAGE_ROOT / name for name in names}
