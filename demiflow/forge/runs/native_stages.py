"""Product adapters for verified native publications; no native fact parsing."""
from demiflow.forge.schema import ForgeError
from demiflow.execution.host.inspection import (
    admit_prepared_stage, PreparedPublicationUnresolved,
    PublicationMismatch,
)


def admit_prepared(target, journal, manifest, output):
    from .manager import _report
    try:
        return admit_prepared_stage(journal, target, parents=_report(target, manifest)['actions'],
            report_node_id=output['action_id'], uri=output['uri'], schema_identity=output['schema_identity'],
            report_digest=manifest['report_digest'])
    except PreparedPublicationUnresolved as exc:
        raise ForgeError('Native prepared publication is unresolved: ' + str(exc)) from exc
    except PublicationMismatch as exc:
        raise ForgeError(str(exc)) from exc
