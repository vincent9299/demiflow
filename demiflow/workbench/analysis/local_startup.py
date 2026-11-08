"""One durable held/released witness for the existing local startup pipe.

Held is written before Popen. Released is durable before the sole S write;
therefore a held witness can prove no worker release after original unit exit.
Missing/changed witnesses remain unknown, including old pre-upgrade jobs.
"""
from demiflow.forge.io import atomic_write_json
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.job_context import bounded_json

FORMAT = 'demiflow_analysis_local_startup_v1'
NAME = 'startup.json'


def validate(value, lease):
    c.fields(value, {'format', 'job_id', 'context_sha256', 'controller',
                     'unit_identity', 'owner_digest', 'phase'}, label='local startup witness')
    if value['format'] != FORMAT:
        c.reject('Unsupported local startup witness', code='conflict')
    c.one_of(value['phase'], {'held', 'released'}, 'local startup phase')
    for key in ('job_id', 'context_sha256', 'controller', 'unit_identity', 'owner_digest'):
        if value[key] != lease[key]:
            c.reject('Original local startup witness differs', code='conflict')
    return value


def write(owned, lease, phase):
    value = dict(format=FORMAT, phase=phase,
                 **{key:lease[key] for key in ('job_id', 'context_sha256', 'controller',
                                              'unit_identity', 'owner_digest')})
    validate(value, lease)
    atomic_write_json(owned/NAME, value)
    return value


def load(owned, lease):
    try:
        raw = bounded_json(owned/NAME, 4096)
    except FileNotFoundError:
        return None
    return validate(c.decode(raw, maximum=4096), lease)
