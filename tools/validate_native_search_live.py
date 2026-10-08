"""Explicit, bounded Dataset API smoke test; no business tables or models."""
import argparse
import json
import os
from pathlib import Path
import time
from datetime import datetime, timezone

from demiflow import data
from demiflow.collect import SearchConfig, Secret
from demiflow.collect.session import WebSession
from demiflow.collect.native_search.config import baseline_id, runtime_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--proxy-env')
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    config = SearchConfig(engines=('google','wikisearch','wikipedia','mwmbl'),
        workers=4, request_concurrency=2, host_concurrency=1, host_interval_s=1,
        timeout_s=25, retries=0, proxy=Secret(args.proxy_env) if args.proxy_env else None)
    session = WebSession(cache_path=output/'requests.sqlite', object_directory=output/'objects', search=config)
    inputs = [{'id': str(i), 'requests': [{'request_id': str(i), 'query': query, 'language': language, 'bindings': []}]}
        for i,(query,language) in enumerate([('Crassula ovata','en'),('天牛','zh-CN'),('文氏图','zh-CN')])]
    rows=[]
    started=time.monotonic()
    stats=(data.from_items(inputs).search_web(requests='requests',output='search',session=session,
        max_candidates=8,request_concurrency=1,concurrency=2,queue_depth=2)
        .map(lambda row: rows.append(row) or row).run_stream())
    report={'date_utc':datetime.now(timezone.utc).isoformat(),'elapsed_s':time.monotonic()-started,
            'baseline_id':baseline_id(),'runtime_id':runtime_id(),
            'validation_scope':'Three explicit-language queries, 12 source attempts maximum before adapter-internal HTTP requests; retries=0; not a sustained load test.',
            'configuration':config.snapshot(),'rows':rows,'metrics':stats.metrics}
    (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+'\n')
    summary=[{'query':r['search'][0]['query'],'status':r['search'][0]['status'],
              'candidates':len(r['search'][0]['candidates']),
              'sources':{s['engine']:s['status'] for s in r['search'][0]['engine_receipts']}} for r in rows]
    print(json.dumps({'elapsed_s':report['elapsed_s'],'results':summary},ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
