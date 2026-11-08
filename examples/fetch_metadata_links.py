"""Run: python -B examples/fetch_metadata_links.py [--workdir NEW_DIR]."""
import json
from demiflow import data
from demiflow.collect import FetchPolicy,HttpSource
from demiflow.execution.run_context import RunContext
from demiflow.objects import LocalObjectStore,ObjectRef
from fetch_source_common import fixture,fixed_producer,request_messages,finish,RUN_LIMITS


def metadata_links(row):
    result=row['result']
    if result['status']!='ok':raise ValueError('Metadata acquisition failed')
    document=json.loads(ObjectRef(**result['raw_ref']).read(max_bytes=8192))
    if set(document)!={'items'} or not isinstance(document['items'],list) or len(document['items'])>8:
        raise ValueError('Example metadata contract exceeded')
    for item in document['items']:
        if set(item)!={'id','uri'}:raise ValueError('Unexpected metadata fields')
        yield {**item,'metadata_raw_ref':result['raw_ref']}


def main():
    with fixture() as (root,url,calls,channel,explicit):
        # Acquire and verify metadata first. Parsing/selection is a consumer fn;
        # the resulting fixed JSONL bytes, including provenance, bind publication.
        with RunContext.start(root/'metadata',limits=RUN_LIMITS):
            rows=(data.from_items([{'request':{'uri':url+'/metadata'}}])
                .fetch(store=LocalObjectStore(root/'metadata-raw'),concurrency=1,
                    policy=FetchPolicy(max_object_bytes=8192,max_attempts=1,http=HttpSource(allow_private=True)))
                .flat_map(metadata_links).take_all(limit=8))
        path,producer=fixed_producer(channel,root,rows,kind='records',source_id='metadata-links-v1')
        with RunContext.start(root/'producer',limits=RUN_LIMITS):
            data.read_json(str(path)).map(request_messages).enqueue(channel.config,producer=producer).run_stream()
        finish(channel,producer,root,calls,kind='metadata_links',explicit=explicit)
        assert calls['/metadata']==1


if __name__=='__main__':main()
