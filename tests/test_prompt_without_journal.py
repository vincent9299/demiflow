"""Equal image requests in independent native runs are still real model calls."""
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.objects import ObjectRef
from image_http_fixture import fixture_http
from test_managed_image_execution import native_dataset,run_limits


def test_image_output_uses_run_protocol_without_request_artifacts(tmp_path,fixture_http):
    rows=[]
    for index in range(2):
        with RunContext.start(tmp_path/('run'+str(index)),limits=run_limits()):
            row,=native_dataset(tmp_path/'objects',fixture_http,1).take(1)
            rows.append(row)
            assert row['error'] is None and not row['call']['reused']
            assert ObjectRef(**row['image']).read(max_bytes=4096)==fixture_http['raw']
    assert len(fixture_http['requests'])==2
    assert rows[0]['image']['uri']!=rows[1]['image']['uri']
    assert rows[0]['image']['sha256']==rows[1]['image']['sha256']
    assert sorted(p.name for p in tmp_path.iterdir())==['objects','run0','run1']
    assert len(list(tmp_path.rglob('*.sqlite')))==2
