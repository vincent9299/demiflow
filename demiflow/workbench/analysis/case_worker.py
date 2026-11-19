"""One fixed native-budget private Case reader; no host or kernel fallback."""
from contextlib import closing
from pathlib import Path
import socket

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import object_id
from demiflow.workbench.analysis.job_context import require_worker as require_job
from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current
from demiflow.workbench.analysis.framing import Frames
from demiflow.forge.schema import ForgeError


def configuration(value):
    c.fields(value,{'version','operation','witness','query'},label='fixed private Case read')
    if type(value['version']) is not int or value['version']!=1 or value['operation']!='case-page':
        c.reject('Unsupported fixed Case operation')
    witness=value['witness']
    c.fields(witness,{'project','artifact','source'},label='registered private Case witness')
    from demiflow.workbench.projects.registry import identity
    identity(witness['project'])
    artifact=witness['artifact'];c.fields(artifact,{'id','generation','digest'},label='registered pinned object')
    object_id(artifact['id']);c.integer(artifact['generation'],'artifact generation',2**31-1,1);c.sha(artifact['digest'])
    c.fields(witness['source'],{'kind','binding','schema','rows','case_key','retention','digest'},label='registered Case source')
    source=c.source_snapshot({key:item for key,item in witness['source'].items() if key!='digest'})
    if source!=witness['source'] or source['kind']!='pinned_object' or source['binding']!=artifact['digest'] or source['retention']!='pinned':
        c.reject('Case source differs from its registered pinned object',code='conflict')
    from demiflow.workbench.analysis.case_query import specification
    if type(value['query']) is not dict or 'scope' not in value['query']:
        c.reject('Case read requires explicit full/sample scope')
    specification(value['query'],source)
    c.encoded(value,maximum=56*1024)
    return value


def execute(value):
    configuration(value)
    # Native imports and row conversion occur only after the sandbox guard in
    # main. The integrator owns the existing private Case reader implementation.
    from demiflow.workbench.analysis import case_query
    reader=getattr(case_query,'read_private',None)
    if not callable(reader):c.reject('Private Case reader is absent from this intermediate release')
    from demiflow.execution.datafusion import DataFusionOptions
    from demiflow.execution.datafusion import DataFusionSession
    from demiflow.workbench.analysis.query import NATIVE_OPTIONS
    from demiflow.workbench.analysis.query import NATIVE_CAPACITY
    with DataFusionSession(resource_directory=job_path('/var/lib/demiflow-analysis/native-slots'),
            options=DataFusionOptions(**NATIVE_OPTIONS),max_concurrent=1,slot_capacity=NATIVE_CAPACITY,
            temp_directory=job_path('/scratch/native'),diagnostics_directory=job_path('/scratch/diagnostics'),managed_diagnostics=True) as session:
        with session.borrow_resources():
            result=reader(value['witness']['source'],job_path('/objects/'+value['witness']['artifact']['id']),value['query'])
            c.encoded(result,maximum=56*1024)
            return result


def response(value):
    message=dict(operation='case-read',artifact=value['witness']['artifact'])
    try:
        message['value']=execute(value)
    except (ForgeError,OSError) as exc:
        message['error']=dict(code=getattr(exc,'code','operation_failed'),message='Fixed Case read failed; no page was returned')
    c.encoded(message,maximum=56*1024)
    return message


def main():
    job=require_job('native')
    value=configuration(job['config'])
    Path(job_path('/scratch/native')).mkdir(mode=0o700)
    Path(job_path('/scratch/diagnostics')).mkdir(mode=0o700)
    message=response(value)
    connection=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.settimeout(5)
    with closing(connection):
        connection.connect(job_path('/broker/case-read.sock'))
        frames=Frames(connection);frames.send('json',message);frames.end_read()


if __name__=='__main__':main()
