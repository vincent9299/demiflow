"""Located, streaming local records via the native Datasource protocol."""
import gzip
import json
from pathlib import Path
from .datasource import Datasource, ReadTask, BlockMetadata


def file_snapshot(path):
    path=Path(path).resolve()
    if not path.exists():return {'path':str(path),'exists':False}
    stat=path.stat()
    return {'path':str(path),'exists':True,'size':stat.st_size,'mtime_ns':stat.st_mtime_ns,'inode':stat.st_ino}


def iter_file_records(path, *, format='jsonl', item_prefix=None, max_records=None,
                      report_path=None, missing='error'):
    """Stream located records; optionally persist scan scope, errors and file snapshot.

    Invalid lines remain rows. Broken JSON containers raise. Reports distinguish
    EOF, budget limits, missing files and interrupted/failed scans.
    """
    if format not in {'jsonl','json','text'}:raise ValueError('format must be jsonl/json/text')
    if max_records is not None and max_records<0:raise ValueError('negative max_records')
    if item_prefix is not None and format!='json':raise ValueError('item_prefix requires json')
    if missing not in {'error','empty'}:raise ValueError('missing must be error/empty')
    origin=file_snapshot(path);path=origin['path']
    report={'path':path,'snapshot':origin,'rows':0,'invalid_rows':0,'complete':False,'status':'interrupted'}
    try:
        if not origin['exists']:
            report['status']='missing'
            if missing=='error':raise FileNotFoundError(path)
            return
        opener=gzip.open if path.endswith('.gz') else open
        with opener(path,'rb') as stream:
            if format=='json':
                import ijson
                records=ijson.items(stream,item_prefix or 'item',use_float=True)
            else:records=stream
            for index,value in enumerate(records,1):
                if max_records is not None and index>max_records:
                    report['status']='budget_limited';break
                raw=None;error=None
                try:
                    if format!='json':
                        raw=value.decode('utf-8');value=raw.rstrip('\r\n') if format=='text' else json.loads(raw)
                except (ValueError,UnicodeError) as exc:
                    raw=value.decode('utf-8',errors='replace') if isinstance(value,bytes) else raw
                    value=None;error={'type':type(exc).__name__,'detail':str(exc)}
                report['rows']=index;report['invalid_rows']+=int(error is not None)
                yield {'value':value,'path':path,'row':index,'error':error,'raw':raw if error else None,'snapshot':origin}
            else:report.update(complete=True,status='read')
        if file_snapshot(path)!=origin:raise ValueError('Source changed during read')
    except Exception as error:
        report.update(complete=False,status='missing' if not origin['exists'] else 'read_error',error=str(error))
        raise
    finally:
        if report_path is not None:
            import os,uuid
            target=Path(report_path);target.parent.mkdir(parents=True,exist_ok=True)
            payload=json.dumps(report,ensure_ascii=False,sort_keys=True)
            if not target.exists() or target.read_text()!=payload:
                # Latest scan status may advance on resume; preserve the earlier
                # report, especially interruptions/failures, before replacement.
                if target.exists():os.link(target,target.with_name(target.name+'.attempt-'+uuid.uuid4().hex+'.json'))
                temp=target.with_name(target.name+'.'+uuid.uuid4().hex+'.tmp');temp.write_text(payload)
                os.replace(temp,target)


class FileRecords(Datasource):
    def __init__(self,paths,**options):self.paths=tuple(paths);self.options=options
    def get_read_tasks(self,parallelism,per_task_row_limit=None,data_context=None):
        def blocks(path):
            for row in iter_file_records(path,**self.options):yield [row]
        return [ReadTask(lambda path=path:blocks(path),BlockMetadata(input_files=(path,)),
                         per_task_row_limit=per_task_row_limit) for path in self.paths]


class UnionDatasource(Datasource):
    def __init__(self,datasets):self.datasets=tuple(datasets)
    def get_read_tasks(self,parallelism,per_task_row_limit=None,data_context=None):
        def blocks(dataset):
            for row in dataset.iter_rows():yield [row]
        return [ReadTask(lambda ds=ds:blocks(ds),per_task_row_limit=per_task_row_limit) for ds in self.datasets]
