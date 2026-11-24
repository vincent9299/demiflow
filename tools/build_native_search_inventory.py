"""Build the native search source inventory offline: catalog sources and platform image adapters.

Usage: python tools/build_native_search_inventory.py [--check]

Loads every catalog source and every ``demiflow/services/engines/*_images.py``
adapter in a worker bootstrap with networking disabled and records its
settings, capabilities and load status. Writes
``demiflow/collect/native_search/inventory.json`` (returned by
``search_source_inventory()``), ``docs/native-search-inventory.json`` and
``docs/native-search-sources.csv``. ``--check`` only compares and exits 1 on a
difference. Live availability is not part of this inventory.
"""
import argparse
import csv
import io
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
CAPABILITY_COLUMNS = ('paging', 'max_page', 'safesearch', 'language_support', 'time_range_support', 'engine_type')


def build():
    sys.path.insert(0, str(ROOT))
    from demiflow.collect.native_search import worker
    from demiflow.collect.native_search.catalog import SOURCES
    from demiflow.collect.native_search.config import RUNTIME_VERSION, SearchConfig
    platform = sorted(p.stem for p in (ROOT / 'demiflow/services/engines').glob('*_images.py'))
    declared = [s['name'] for s in SOURCES] + [
        {'name': stem.replace('_', ' '), 'module': f'demiflow.services.engines.{stem}', 'revision': 'inventory'}
        for stem in platform]
    config = SearchConfig(engines=tuple(declared), language='en')
    settings = {'sources': config.source_configs(), 'outgoing': {}, 'timeout_s': 10, 'max_bytes': 1 << 20,
                'max_results': 100, 'max_redirects': 3, 'offline_audit': True}
    loaded, errors = worker.bootstrap(settings)
    capabilities = worker.inspect(loaded, errors)['sources']
    common = ('name', 'engine', 'categories', 'disabled', 'shortcut')
    sources = {}
    for source in SOURCES:
        name = source['name']
        categories = source.get('categories', ['general'])
        sources[name] = {'engine': source['engine'],
                         'categories': [c.strip() for c in categories.split(',')] if isinstance(categories, str) else categories,
                         'disabled_default': bool(source.get('disabled', False)), 'shortcut': source.get('shortcut'),
                         'options': {k: v for k, v in source.items() if k not in common},
                         'capabilities': capabilities.get(name), 'load_status': errors.get(name, 'ok')}
    platform_sources = {}
    for stem in platform:
        name = stem.replace('_', ' ')
        platform_sources[f'demiflow.services.engines.{stem}'] = {
            'capabilities': capabilities.get(name), 'load_status': errors.get(name, 'ok')}
    return {'runtime': RUNTIME_VERSION,
            'scope': 'Built-in native search sources (catalog) and platform image adapters; load status is an '
                     'offline check with networking disabled. Live availability is recorded separately.',
            'sources': sources, 'platform_sources': platform_sources}


def sources_csv(inventory):
    out = io.StringIO()
    writer = csv.writer(out, lineterminator='\n')
    writer.writerow(('name', 'kind', 'engine', 'categories', 'disabled_default', 'load_status') + CAPABILITY_COLUMNS)
    for name, item in inventory['sources'].items():
        caps = item['capabilities'] or {}
        writer.writerow((name, 'catalog', item['engine'], '|'.join(item['categories']), item['disabled_default'],
                         item['load_status']) + tuple(caps.get(c) for c in CAPABILITY_COLUMNS))
    for module, item in inventory['platform_sources'].items():
        caps = item['capabilities'] or {}
        writer.writerow((module, 'platform', module, '', False, item['load_status'])
                        + tuple(caps.get(c) for c in CAPABILITY_COLUMNS))
    return out.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    inventory = build()
    text = json.dumps(inventory, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
    outputs = {ROOT / 'demiflow/collect/native_search/inventory.json': text,
               ROOT / 'docs/native-search-inventory.json': text,
               ROOT / 'docs/native-search-sources.csv': sources_csv(inventory)}
    stale = [str(path.relative_to(ROOT)) for path, content in outputs.items()
             if not path.exists() or path.read_text() != content]
    if args.check:
        print('inventory up to date' if not stale else 'stale: ' + ', '.join(stale), file=sys.__stdout__)
        return 1 if stale else 0
    for path, content in outputs.items():
        path.write_text(content)
    failed = sorted(n for n, i in {**inventory['sources'], **inventory['platform_sources']}.items()
                    if i['load_status'] != 'ok')
    print(f"{len(inventory['sources'])} catalog sources, {len(inventory['platform_sources'])} platform adapters; "
          f"load failures: {failed or 'none'}", file=sys.__stdout__)
    return 0


if __name__ == '__main__':
    sys.exit(main())
