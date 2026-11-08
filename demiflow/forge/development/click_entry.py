"""Public CLI adapter; all development parsing and behavior live in cli.main."""
import click


def register(forge):
    @forge.command("dev", context_settings={"ignore_unknown_options": True,
                                             "allow_extra_args": True},
                   add_help_option=False)
    @click.pass_context
    def dev(context):
        """Snapshot-bound business development (use --help for commands)."""
        from .cli import main
        raise SystemExit(main(list(context.args)))
