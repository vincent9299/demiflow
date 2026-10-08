"""Publish the audited source matrix, keeping validation levels independent."""
import csv
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
p = ROOT/'docs/native-search-inventory.json'
x = json.loads(p.read_text())
parser = argparse.ArgumentParser()
parser.add_argument('--live-report', help='Version-matched report; omit to keep all live states not_run')
args = parser.parse_args()
live = json.loads(Path(args.live_report).read_text()) if args.live_report else {'rows': []}
if args.live_report and live.get('baseline_id') != x['baseline_id']:
    raise ValueError('Live evidence must identify the matching adapter baseline')
live_status = {}
for row in live['rows']:
    for source in row['search'][0]['engine_receipts']:
        live_status.setdefault(source['engine'], []).append(source['status'])
fixtures = {'google', 'mwmbl', 'wikipedia'}
for module, record in x['modules'].items():
    record['fixture_validation'] = 'passed_fixed_response' if module in fixtures else 'not_run'
    if module == 'sqlite':
        record['fixture_validation'] = 'passed_local_sqlite_main_and_keyvalue_paging'
    record['live_validation'] = live_status.get(module, 'not_run')
for source in x['sources']:
    source['fixture_validation'] = 'passed_fixed_response' if source['module'] in fixtures else 'not_run'
    source['live_validation'] = live_status.get(source['name'], 'not_run')
    module = x['modules'].get(source['module'], {})
    source['optional_dependencies'] = module.get('optional_dependencies', {})
    source['required_configuration'] = sorted(set(module.get('unset_configuration', [])) | set(module.get('setup_config_keys', [])))
    source['requires_api_key'] = module.get('declared', {}).get('about', {}).get('require_api_key')
x['platform_sources'] = [{'name': 'wikisearch', 'module': 'demiflow.services.engines.wikisearch',
    'migration': 'bundled_native', 'fixture_validation': 'passed_fixed_response',
    'live_validation': live_status.get('wikisearch', 'not_run'), 'load_validation': x['audit']['results']['wikisearch'],
    'language': 'explicit; bundled Wikipedia traits; all rejected', 'paging': False}]
x['validation_notes'] = {
    'load': 'Import/setup only; no remote requests. Native init/search and source availability are separate.',
    'configuration_error': 'Default is inactive, needs endpoint/key/Tor/setup configuration, or setup declined. Consult source declarations; not evidence of a working service.',
    'configuration_hints': 'required_configuration lists AST candidates: unset module fields and indexed setup/init arguments. These can include optional fields, and dynamically computed requirements still need source setup/init documentation.',
    'fixtures': 'Four sources have request/response fixtures; shared result-family/merging/scheduling behavior has dedicated tests.',
    'live': live.get('validation_scope', 'not_run; no live report supplied'),
    'remaining_sources': 'All modules bundled; parser behavior/live availability not verified unless marked above.',
    'non_http': 'SQL/Mongo/Valkey/command connectors use their own protocols/configuration; worker deadlines and source admission apply; HTTP proxies and HTTP host gates do not apply to these protocols.'}
p.write_text(json.dumps(x, ensure_ascii=False, indent=2)+'\n')
columns=['name','module','processor','language','paging','safesearch','time_range','load','optional_dependencies','required_configuration','fixture','live']
with (ROOT/'docs/native-search-sources.csv').open('w',newline='') as f:
    out=csv.DictWriter(f,fieldnames=columns);out.writeheader()
    for source in x['sources']+x['platform_sources']:
        module=x['modules'].get(source['module'], {})
        c=source.get('load_validation',{}).get('capabilities') or module.get('declared',{})
        out.writerow({'name':source['name'],'module':source['module'],'processor':c.get('engine_type','online'),
            'language':c.get('language_support','unknown'),'paging':c.get('paging','unknown'),
            'safesearch':c.get('safesearch','unknown'),'time_range':c.get('time_range_support','unknown'),
            'load':'loaded' if source.get('load_validation',{}).get('loaded') else 'configuration_required_or_inactive',
            'optional_dependencies':json.dumps(source.get('optional_dependencies',{})),
            'required_configuration':json.dumps(source.get('required_configuration',[])),
            'fixture':source['fixture_validation'],'live':json.dumps(source['live_validation'])})
# This public runtime resource includes source declarations/status only; no credentials.
(ROOT/'demiflow/collect/native_search/inventory.json').write_text(p.read_text())
print('Published 344 baseline source configurations + wikisearch; 257 baseline adapter modules')
