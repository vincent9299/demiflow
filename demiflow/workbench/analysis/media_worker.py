"""Fixed media sandbox guard for the output owner's bounded reader.

No additional engine or host fallback. Output modules merge from rich_output;
this role remains unavailable in a wheel lacking that exact implementation.
"""
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.sandbox import _read
from demiflow.workbench.analysis.job_context import require_worker as require_job
from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current


def configuration(value):
    if type(value) is dict and value.get('operation') in {'artifact-media','probe-media'}:
        from demiflow.workbench.analysis.media_artifact_worker import configuration as fixed
        return fixed(value)
    try:
        from demiflow.workbench.analysis.output_worker import request
    except ModuleNotFoundError as exc:
        if exc.name != __package__ + '.output_worker':
            raise
        c.reject('Output reader is absent from this intermediate release; media unavailable')
    request(value)
    witness = value['witness']
    c.fields(witness, {'artifact', 'provenance', 'sources'}, {'table_handles'}, label='registered output witness')
    artifact = witness['artifact']
    c.fields(artifact, {'id', 'generation', 'digest', 'sha256', 'bytes'}, label='committed output artifact')
    object_id(artifact['id']); c.integer(artifact['generation'], 'artifact generation', 2**31-1, 1)
    c.sha(artifact['digest']); c.sha(artifact['sha256'])
    c.integer(artifact['bytes'], 'output document bytes', 16*1024*1024, 1)
    if type(witness['provenance']) is not dict or 'project' not in witness['provenance']:
        c.reject('Output witness requires its owning project')
    from demiflow.workbench.projects.registry import identity as project_identity
    project_identity(witness['provenance']['project'])
    return value


def main():
    job = require_job('media')
    value = configuration(job['config'])
    # The output owner uses this fixed file/endpoint. It is written by Runtime,
    # readonly in this namespace and must equal the admitted job configuration.
    if c.decode(_read(job_path('/job/output-request.json'), 56*1024), maximum=56*1024) != value:
        c.reject('Output request differs from its owned media job', code='conflict')
    if value['operation'] in {'artifact-media','probe-media'}:
        from demiflow.workbench.analysis.media_artifact_worker import run
        run(value)
    else:
        from demiflow.workbench.analysis.output_worker import run
        run()


if __name__ == '__main__':
    main()
