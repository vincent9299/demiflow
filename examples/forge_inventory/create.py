"""Create an isolated example using only public workspace APIs; never executes it."""
import argparse
import json
from pathlib import Path
import shutil

import lance
import pyarrow as pa
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops


def create(root):
    root = Path(root)
    ws.init_workspace(root, name='inventory-reconciliation')
    for directory in ('flows', 'operators', 'tables', 'data'):
        (root / directory).mkdir()
    control = ws.read_control(root / 'workspace.json')
    expected = ws.content_digest(control)
    control['locations']['local'] = {'root': 'data', 'writable': True}
    ws.write_control(root / 'workspace.json', control, expected=expected)
    source = Path(__file__).resolve().parent
    for folder in ['flows', 'operators']:
        for path in (source / folder).glob('*.py'):
            shutil.copy2(path, root / folder / path.name)
    fixtures = {
        'orders': (pa.schema([('order_id', pa.string()), ('expected', pa.int64())]),
            [{'order_id': 'A', 'expected': 10}, {'order_id': 'B', 'expected': 5}, {'order_id': 'C', 'expected': 1}]),
        'shipments': (pa.schema([('order_id', pa.string()), ('quantity', pa.int64())]),
            [{'order_id': 'A', 'quantity': 4}, {'order_id': 'A', 'quantity': 6}, {'order_id': 'B', 'quantity': 4}]),
    }
    for name, (schema, rows) in fixtures.items():
        lance.write_dataset(pa.Table.from_pylist(rows, schema=schema), str(root / 'data' / (name + '.lance')))
        project_ops.attach_table(root, alias=name, location_id='local', relative_uri=name + '.lance', version=1,
            dataset_id=name, schema_name=name, schema_version='1')
    for name, tolerance in [('strict', 0), ('tolerant', 1)]:
        params = {'format': 'demiflow_forge_flow_parameters_v1', 'arguments': {
            'orders': {'kind':'table','alias':'orders'}, 'shipments': {'kind':'table','alias':'shipments'},
            'detail': {'kind':'output','location_id':'local','relative_uri':name + '_detail.lance'},
            'summary': {'kind':'output','location_id':'local','relative_uri':name + '_summary.lance'},
            'tolerance':tolerance}}
        path = 'flows/' + name + '.json'
        (root / path).write_text(json.dumps(params, ensure_ascii=False, indent=2) + '\n')
        ws.register_flow(root, 'flows/inventory.py:run', params=path)
    (root / 'README.md').write_text('库存交付核对：固定订单 + 多批发货，按订单汇总后给出 matched/short/excess。\n'
        '同一接线有 strict/tolerant 两份配置；详情和状态计数分别交付。所有数据是明确隔离的验收夹具，无模型或网络。\n')
    return root


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    create(parser.parse_args().root)
