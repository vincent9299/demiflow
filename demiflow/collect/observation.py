"""Read-only receipt summaries. No session/client construction or network I/O."""
import json
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit


def receipt_observation(path):
    """Cumulative native search HTTP and latest outcome per requested URL.

    Download attempt histories can occur in raw and parsed cache entries. These
    are explicitly deduplicated receipt observations, not socket counters.
    Exclusions and library reuse are separate from network failures.
    """
    path = Path(path)
    if not path.exists():
        return {'search_http': [], 'downloads': [], 'download_failures': [], 'download_attempt_receipts': 0,
                'download_mean_s': None, 'unique_urls': 0}
    latest = {}
    attempts = {}
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True) as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        fallback=[]
        if 'native_search_fallback_results' in tables:
            groups=db.execute('''SELECT json_extract(value,'$.primary.stop_reason'),
                json_extract(value,'$.primary.status'),json_extract(value,'$.result.status'),
                COUNT(*),MAX(observed_at) FROM native_search_fallback_results
                GROUP BY 1,2,3 LIMIT 65''').fetchall()
            if len(groups)>64:
                raise ValueError('Fallback observation exceeds 64 outcome groups')
            fallback=[dict(zip(('primary_stop_reason','primary_status','fallback_status',
                               'queries','last_selected_at'),row)) for row in groups]
        search_admission = [dict(identity=key, observed_at=stamp, **json.loads(state))
            for key,state,stamp in db.execute('SELECT identity,state_json,observed_at FROM native_search_admission ORDER BY observed_at DESC')
            ] if 'native_search_admission' in tables else []
        for state in search_admission:
            state['window_samples']=len(state.pop('samples',[]))
            failures=state.pop('failure_window',[])
            state['failure_window_samples']=len(failures)
            state['failure_window_failures']=sum(v[1] for v in failures)
        admission_events = [json.loads(r[0]) for r in db.execute(
            'SELECT event_json FROM native_search_admission_events ORDER BY id DESC LIMIT 20')
            ] if 'native_search_admission_events' in tables else []
        recovery = [dict(identity=key,observed_at=stamp,**json.loads(state))
            for key,state,stamp in db.execute('SELECT identity,state_json,observed_at FROM native_search_recovery ORDER BY observed_at DESC')
            ] if 'native_search_recovery' in tables else []
        recovery_events = [json.loads(r[0]) for r in db.execute(
            'SELECT event_json FROM native_search_recovery_events ORDER BY id DESC LIMIT 20')
            ] if 'native_search_recovery_events' in tables else []
        routes = [dict(zip(('name','cooldown_until','consecutive_failures','last_status'),r)) for r in db.execute(
            'SELECT name,until,failures,status FROM native_search_route_health')] if 'native_search_route_health' in tables else []
        session_pools=[]
        if 'native_search_session_pools' in tables:
            for identity,value,stamp in db.execute('SELECT identity,state_json,observed_at FROM native_search_session_pools ORDER BY observed_at DESC'):
                state=json.loads(value);slots=list(state.pop('slots').values());state.pop('created',None)
                session_pools.append(dict(identity=identity,observed_at=stamp,**state,
                    healthy=sum(s['healthy'] and not s['retired'] and s['expires_at']>time.time() for s in slots),
                    cold=sum(not s['healthy'] and not s['retired'] and s['expires_at']>time.time() for s in slots),
                    retired_slots=sum(bool(s['retired']) for s in slots),
                    expired_slots=sum(not s['retired'] and s['expires_at']<=time.time() for s in slots)))
        fetch_routes = [dict(zip(('domain','name','cooldown_until','consecutive_failures','last_status'),r)) for r in db.execute(
            'SELECT domain,name,until,failures,status FROM fetch_route_health')] if 'fetch_route_health' in tables else []
        fetch_route_http = [dict(zip(('domain','name','status','http_status','count','mean_s'),r)) for r in db.execute(
            'SELECT domain,name,status,http_status,COUNT(*),AVG(elapsed_s) FROM fetch_route_events GROUP BY 1,2,3,4')] if 'fetch_route_events' in tables else []
        search = list(db.execute('''SELECT json_extract(h.value,'$.host'),
            json_extract(h.value,'$.http_status'), COUNT(*), AVG(json_extract(h.value,'$.elapsed_s'))
            FROM native_search n, json_each(n.value,'$.attempts') a,
            json_each(a.value,'$.http') h GROUP BY 1,2''')) if 'native_search' in tables else []
        if 'cache' in tables:
            for url, status, reason, acquisition, raw_attempts in db.execute('''SELECT
                json_extract(value,'$.url'),json_extract(value,'$.status'),json_extract(value,'$.reason'),
                json_extract(value,'$.acquisition.kind'),json_extract(value,'$.attempts')
                FROM cache WHERE json_type(value,'$.document_ref') IS NOT NULL ORDER BY rowid'''):
                if not url:
                    continue
                latest[url] = (status, acquisition, reason)
                for attempt in json.loads(raw_attempts or '[]'):
                    key = (url, json.dumps(attempt, sort_keys=True, ensure_ascii=False))
                    attempts[key] = attempt
    domains = defaultdict(Counter)
    failures = Counter()
    for url, (status, acquisition, reason) in latest.items():
        host = (urlsplit(url).hostname or '(unknown)').lower()
        domains[host]['urls'] += 1
        domains[host][status or 'unknown'] += 1
        if acquisition:
            domains[host][acquisition] += 1
        if status not in {'ok', 'excluded'}:
            failures[(status or 'unknown', reason or '')] += 1
    elapsed = [a['elapsed_s'] for a in attempts.values() if isinstance(a.get('elapsed_s'), (float, int))]
    return {'routes':routes,'session_pools':session_pools,'fetch_routes':fetch_routes,'fetch_route_http':fetch_route_http,
            'search_fallback':fallback,
            'search_admission':search_admission,'search_admission_events':admission_events,
            'search_recovery':recovery,'search_recovery_events':recovery_events,
            'search_http': [dict(zip(('host','http_status','count','mean_s'), r)) for r in search],
            'downloads': [{'host':host, **counts} for host, counts in sorted(domains.items(), key=lambda x:-x[1]['urls'])],
            'download_failures': [{'status':status, 'reason':reason, 'urls':count}
                                  for (status,reason),count in failures.most_common()],
            'download_attempt_receipts': len(attempts),
            'download_mean_s': sum(elapsed)/len(elapsed) if elapsed else None,
            'unique_urls': len(latest)}
