"""Model-independent CLI client, launched with a binding for one open notebook cell."""
import argparse
import json
import os
from pathlib import Path
import sys
import time


def main():
    binding, *args = sys.argv[1:]
    if binding == '--provider':
        endpoint = os.environ['DEMIFLOW_CONTROL_ENDPOINT']
        os.execv(args[0], [arg.replace('__DEMIFLOW_CELL_ENDPOINT__', endpoint) for arg in args])
    parser = argparse.ArgumentParser(prog='data', description='Inspect the notebook context, catalogs, and operate on the current cell.')
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
    args = parser.parse_args(args)
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from demiflow.workbench.conversations.control_client import call

    token, turn, epoch = json.loads(Path(binding).read_text())
    def send(method, params):
        return call('', token, turn, epoch, method, params)
    if args.group == 'context':
        method = 'context.show'
        params = {'topic': args.topic} if args.topic else {}
    elif args.group == 'catalog':
        method = 'catalog.' + args.action
        params = {'group': args.kind, **({'offset': args.offset} if args.action == 'list' else {'id': args.id, 'offset': args.offset})}
        if args.action == 'list' and args.query:
            params['query'] = args.query
        if args.action == 'show' and args.version is not None:
            params['version'] = args.version
    elif args.action == 'edit':
        method = 'cell.edit'
        raw = sys.stdin.buffer.read(12001)
        if len(raw) > 12000:
            parser.error('Cell edit exceeds 12000 UTF-8 bytes')
        params = {'source': raw.decode()}
    else:
        method = 'cell.' + args.action
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
