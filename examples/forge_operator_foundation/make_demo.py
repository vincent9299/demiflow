"""Create a new owned workspace, then run its native Flow through Forge.

Usage: python examples/forge_operator_foundation/make_demo.py /tmp/my-forge-demo
No model, network, shared environment or production data is used.
"""
import json
from pathlib import Path
import shutil
import sys
import time


def create(root):
    import lance
    import pyarrow as pa
    from demiflow.workbench.runs import manager as runs
    from demiflow.workbench.projects import workspace
    from demiflow.workbench import project_operations as project_ops

    root = Path(root).absolute()
    workspace.init_workspace(root, name='native-operator-demo')
    for directory in ('flows', 'operators', 'tables', 'data'):
        (root / directory).mkdir()
    control = workspace.read_control(root / 'workspace.json')
    expected = workspace.content_digest(control)
    control['locations']['local'] = {'root': 'data', 'writable': True}
    workspace.write_control(root / 'workspace.json', control, expected=expected)
    example = Path(__file__).parent
    for directory in ('flows', 'operators'):
        for path in (example / directory).iterdir():
            if path.suffix in {'.py', '.json'}:
                shutil.copyfile(path, root / directory / path.name)
    shutil.copyfile(example / 'README.md', root / 'README.md')
    schema = pa.schema([pa.field('case_id', pa.string(), nullable=False), pa.field('text', pa.string(), nullable=False)])
    lance.write_dataset(pa.Table.from_pylist([
        {'case_id': 'c01', 'text': '示例 case 一'},
        {'case_id': 'c02', 'text': '示例 case 二'},
        {'case_id': 'c03', 'text': '示例 case 三'},
    ], schema=schema), str(root / 'data/cases.lance'), mode='create')
    project_ops.attach_table(root, 'cases', relative_uri='cases.lance', version=1,
                           dataset_id='cases', schema_name='case', schema_version='1')
    prepared = runs.prepare(root, 'flows/expand.py:run', params='flows/demo.json')
    run = runs.start(root, prepared['run_id'], expected=prepared['digest'])
    deadline = time.monotonic() + 120
    while run['status'] not in runs.TERMINAL:
        if time.monotonic() > deadline:
            runs.cancel(root, run['run_id'])
            raise TimeoutError('Demo did not finish within 120 seconds')
        time.sleep(0.25)
        run = runs.show(root, run['run_id'])
    if run['status'] != 'succeeded':
        raise RuntimeError(run['state'])
    rows = project_ops.sample_table(root, 'run:' + run['run_id'] + ':target')['rows']
    actual = sorted((row['case_id'], row['variant']) for row in rows)
    expected = [(identity, variant) for identity in ('c01', 'c02', 'c03') for variant in ('demo-A', 'demo-B')]
    if actual != expected:
        raise AssertionError(actual)
    return {'workspace': str(root), 'run_id': run['run_id'], 'status': run['status'],
            'rows': rows, 'observations': run['observations']}


if __name__ == '__main__':
    print(json.dumps(create(sys.argv[1]), ensure_ascii=False, indent=2))
