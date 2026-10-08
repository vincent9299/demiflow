"""Generate source and dependency compatibility receipts without remote requests.

Run from this checkout using the interpreter with demiflow[search] installed.
An audit imports/setup-checks every configured source and unconfigured module;
network is blocked. Initialization, parser fixtures and live availability are
reported independently, never implied by a successful import.
"""
from __future__ import annotations
import ast
import asyncio
from collections import Counter
import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


async def audit():
    from demiflow.collect.native_search import NativeSearchSession, SearchConfig
    from demiflow.collect.native_search.config import catalog, VENDOR, baseline_id
    inventory = json.loads((ROOT / 'docs/native-search-inventory.json').read_text())
    configs = copy.deepcopy(catalog()['engines'])
    # Direct worker audit uses catalog flags verbatim, including inactive entries.
    # Additional modules require explicit configurations; failed setup is evidence.
    used = {s['engine'] for s in configs}
    for module in inventory['modules']:
        if module != '__init__' and module not in used:
            configs.append({'name': 'audit ' + module.replace('_', '-'), 'engine': module})
    configs.append({'name': 'wikisearch', 'engine': str(ROOT/'demiflow/services/engines/wikisearch')})
    source_results = {}
    for offset in range(0, len(configs), 24):
        group = configs[offset:offset+24]
        session = NativeSearchSession(cache_path=ROOT/'.validation/audit.sqlite',
                                      config=SearchConfig(language='en', workers=1))
        make_config = session.worker_config
        session.worker_config = lambda context: {**make_config(context), 'offline_audit': True}
        await session.initialize()
        session.resolved_sources = group
        # bootstrap has no network phase. The audit never invokes init/search.
        try:
            result = await session.call(('default', 'en'), {'op': 'inspect'})
            for cfg in group:
                name = cfg['name']
                source_results[name] = {'loaded': name.lower() in result.get('sources', {}),
                    'capabilities': result.get('sources', {}).get(name.lower()),
                    'error': result.get('errors', {}).get(name.lower()),
                    'initialization': 'not_run', 'remote_requests': 0}
        finally:
            await session.aclose()
    for source in inventory['sources']:
        source['migration'] = 'bundled_native'
        source['load_validation'] = source_results[source['name']]
    for name, module in inventory['modules'].items():
        module['migration'] = 'bundled_native'
        module['load_validations'] = [s['name'] for s in configs if s['engine'] == name and source_results[s['name']]['loaded']]
        module['fixture_validation'] = 'not_run'
        module['live_validation'] = 'not_run'
        external = {n.split('.')[0] for n in module['imports']}
        module['optional_dependencies'] = {k:v for k,v in {
            'mysql':'search-mysql', 'mariadb':'search-mariadb', 'psycopg2':'search-postgresql',
            'pymongo':'search-mongodb'}.items() if k in external}
    inventory['baseline_id'] = baseline_id()
    inventory['audit'] = {'source_count': len(inventory['sources']), 'module_count': len(inventory['modules']) - 1,
                          'load_outcomes': dict(Counter('loaded' if r['loaded'] else r['error'] for r in source_results.values())),
                          'network': 'none', 'results': source_results}
    (ROOT/'docs/native-search-inventory.json').write_text(json.dumps(inventory, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(inventory['audit']['load_outcomes']))


if __name__ == '__main__':
    asyncio.run(audit())
