"""Model-independent Notebook assistant `data` CLI owned by Forge."""
import argparse
import json
import os
from pathlib import Path
import sys
import time


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='data',
        description='Inspect the notebook context, catalogs, and operate on the current cell.',
    )
    groups = parser.add_subparsers(dest='group', required=True)
    ctx = groups.add_parser('context', help='Show compact notebook context overview, or expand one --topic')
    ctx.add_argument('--topic', choices=['cell', 'before', 'output', 'env', 'catalog'],
                     help='Progressive-disclosure topic to expand')
    catalog = groups.add_parser('catalog').add_subparsers(dest='action', required=True)
    listing = catalog.add_parser('list')
    listing.add_argument('kind', choices=['dictionary', 'tools'])
    listing.add_argument('--query', default='', help='Filter items by keyword')
    listing.add_argument('--offset', type=int, default=0)
    detail = catalog.add_parser('show')
    detail.add_argument('kind', choices=['dictionary', 'tools'])
    detail.add_argument('id', metavar='NAME')
    detail.add_argument('--version', type=int, help='Select a dictionary table version')
    detail.add_argument('--offset', type=int, default=0)
    cell = groups.add_parser('cell').add_subparsers(dest='action', required=True)
    cell.add_parser('show')
    cell.add_parser('edit', help='Replace the bound cell with Python source from stdin')
    cell.add_parser('run', help='Run the bound cell in its attached Jupyter kernel')
    tool = groups.add_parser('tool', help='Author or update reusable semantic modules under data_tools/').add_subparsers(dest='action', required=True)
    tool_write = tool.add_parser('write', help='Write a data_tools/<name>.py module from stdin (requires non-empty docstring per public tool; __demiflow_data_tools__ is optional)')
    tool_write.add_argument('path', metavar='MODULE_PY', help='Module filename under data_tools/, e.g. pool_review.py')
    return parser


def main():
    binding, *args = sys.argv[1:]
    if binding == '--provider':
        endpoint = os.environ['DEMIFLOW_CONTROL_ENDPOINT']
        os.execv(args[0], [arg.replace('__DEMIFLOW_CELL_ENDPOINT__', endpoint) for arg in args])
    parser = build_parser()
    parsed = parser.parse_args(args)
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from demiflow.forge.control_client import call

    token, turn, epoch = json.loads(Path(binding).read_text())

    def send(method, params):
        return call('', token, turn, epoch, method, params)

    if parsed.group == 'context':
        method = 'context.show'
        params = {'topic': parsed.topic} if parsed.topic else {}
    elif parsed.group == 'catalog':
        method = 'catalog.' + parsed.action
        params = {'group': parsed.kind, **({'offset': parsed.offset} if parsed.action == 'list' else {'id': parsed.id, 'offset': parsed.offset})}
        if parsed.action == 'list' and parsed.query:
            params['query'] = parsed.query
        if parsed.action == 'show' and parsed.version is not None:
            params['version'] = parsed.version
    elif parsed.group == 'tool' and parsed.action == 'write':
        method = 'tool.write'
        raw = sys.stdin.buffer.read(65537)
        if len(raw) > 65536:
            parser.error('Tool source exceeds 65536 UTF-8 bytes')
        params = {'path': parsed.path, 'source': raw.decode()}
    elif parsed.action == 'edit':
        method = 'cell.edit'
        raw = sys.stdin.buffer.read(12001)
        if len(raw) > 12000:
            parser.error('Cell edit exceeds 12000 UTF-8 bytes')
        params = {'source': raw.decode()}
    else:
        method = 'cell.' + parsed.action
        params = {}
    key = send(method, params)
    delay = .2
    while True:
        reply = send('result', key)
        if reply['done']:
            print(json.dumps(reply.get('value', reply.get('error')), ensure_ascii=False))
            return 0 if 'value' in reply else 1
        time.sleep(delay)
        delay = min(2, delay * 1.5)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError) as exc:
        sys.exit(str(exc))
