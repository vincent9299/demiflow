"""Third business acceptance: native two-input non-model Flow, two configurations."""
import importlib.util
from pathlib import Path
import time

import lance
import pytest
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws


def test_independent_inventory_flow_two_configs_and_fixed_outputs(tmp_path):
    spec=importlib.util.spec_from_file_location('inventory_example',Path(__file__).parents[1]/'examples/forge_inventory/create.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root=module.create(tmp_path/'inventory')
    values={}
    for name in ['strict','tolerant']:
        prepared=runs.prepare(root,'flows/inventory.py:run',params='flows/'+name+'.json')
        runs.start(root,prepared['run_id'],expected=prepared['digest'])
        deadline=time.monotonic()+90
        while time.monotonic()<deadline:
            result=runs.show(root,prepared['run_id'])
            if result['status'] in runs.TERMINAL:break
            time.sleep(.2)
        else:
            runs.cancel(root,prepared['run_id'])
            pytest.fail('Inventory fixture worker timeout')
        assert result['status']=='succeeded',result
        outputs={item['name']:item for item in result['outputs']}
        receipt=outputs['detail']['receipt']
        rows=lance.dataset(receipt['uri'],version=receipt['committed_version']).to_table().to_pylist()
        values[name]={row['order_id']:row['status'] for row in rows}
        assert len(outputs)==2 and outputs['detail']['uri']!=outputs['summary']['uri']
    assert values=={'strict':{'A':'matched','B':'short','C':'short'},'tolerant':{'A':'matched','B':'matched','C':'matched'}}
    assert lance.dataset(str(root/'data/orders.lance')).version==1
    assert lance.dataset(str(root/'data/shipments.lance')).version==1
    assert len(ws.load_workspace(root)[1]['entries']['flows/inventory.py:run'])==2
