"""Run: python -B examples/fetch_keywords.py [--workdir NEW_DIR]."""
from demiflow import data
from demiflow.collect import SearchConfig
from demiflow.collect.session import WebSession
from demiflow.execution.run_context import RunContext
from demiflow.queue import ProducerSourceSpec
from fetch_source_common import fixture,request_messages,finish,RUN_LIMITS


def candidates(row):
    for result in row['search']:
        if result['status']!='ok':raise ValueError('Search did not return usable candidates')
        for index,candidate in enumerate(result['candidates']):
            yield {'id':row['id']+'-'+str(index),'uri':candidate['url']}


def main():
    with fixture() as (root,url,calls,channel,explicit):
        producer=channel.open('objects',producer_id='source',source=ProducerSourceSpec(kind='keywords',
            source_id='keyword-example-v1',uri='memory:fixed-keywords',version=1,max_rows=8,max_bytes=8192))
        session=WebSession(cache_path=root/'search.sqlite',search=SearchConfig(engines=({
            'name':'local-example','module':'fetch_example_engine','revision':'v1',
            'base_url':url+'/search','enable_http':True},),language='en',workers=1,request_concurrency=1,
            max_bytes=65536,max_results=2,host_interval_s=0))
        with RunContext.start(root/'producer',limits=RUN_LIMITS):
            (data.from_items([{'id':'query','queries':[{'query':'local artifacts','request_id':'q1'}]}])
                .search_web(requests='queries',output='search',session=session,max_candidates=2,
                            request_concurrency=1,concurrency=1,queue_depth=1)
                .flat_map(candidates).map(request_messages)
                .enqueue(channel.config,producer=producer).run_stream())
        # Native search response is fixed before candidate expansion/publication.
        finish(channel,producer,root,calls,kind='keywords',explicit=explicit)
        assert calls['/search']==1


if __name__=='__main__':main()
