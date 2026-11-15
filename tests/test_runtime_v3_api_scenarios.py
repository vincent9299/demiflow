"""Independent real API groups; the full mapping never labels missing work passed."""
from types import SimpleNamespace
import os
import pytest
from runtime_v3_api_scenarios import api_matrix


@pytest.mark.parametrize('group',['sources','readers','transforms','relations','terminals','retired','writers','native','models','documents','queue','acquisition'])
def test_actual_api_behavior_group(tmp_path,group):
    fx=SimpleNamespace(root=tmp_path, evidence_dir=tmp_path/'evidence',provider=os.environ.get('DF_API_TEST_PROVIDER','local'))
    report=api_matrix(fx,group)
    assert report['api_count']==98
    assert report['cases']
    assert not report['failed'], {key:value['failed'] for key,value in report['failed'].items()}
    assert report['not_run'] and not report['complete']  # a selected subset is never the full 98-entry proof
    assert all(report['api'][key]['positive'] or report['api'][key]['disposition']=='retired_preserved_rejection' for key in report['tested'])
