"""Run: python -B examples/fetch_manifest.py [--explicit] [--workdir NEW_DIR]."""
from demiflow import data
from demiflow.execution import RunContext
from fetch_source_common import fixture,fixed_producer,request_messages,finish,RUN_LIMITS


def main():
    with fixture() as (root,url,calls,channel,explicit):
        path,producer=fixed_producer(channel,root,[{'id':key,'uri':url+'/'+key} for key in ('a','b')],
                                     kind='manifest',source_id='fixed-manifest-v1')
        with RunContext.start(root/'producer',limits=RUN_LIMITS):
            data.read_json(str(path)).map(request_messages).enqueue(channel.config,producer=producer).run_stream()
        finish(channel,producer,root,calls,kind='fixed_manifest',explicit=explicit)


if __name__=='__main__':main()
