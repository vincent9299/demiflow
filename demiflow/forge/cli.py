"""Administrative Forge entrypoints for platform facts."""
import click

from .platform_cli import register as register_platform


def register(main: click.Group) -> None:
    @main.group()
    def forge():
        """Inspect platform APIs and workspace facts."""

    register_platform(forge)
