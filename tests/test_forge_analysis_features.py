"""Helper reports actual optional media features; presence is not a grant."""
import pytest

from demiflow.workbench.analysis.web_service import WebService
from demiflow.forge.schema import ForgeError


def adapter(features=None):
    value=object.__new__(WebService)
    value.config=dict(helper_sha256='a'*64,profile_sha256='b'*64)
    status=dict(format='demiflow_analysis_helper_status_v1',mode='production',active=True,**value.config)
    if features is not None:status['features']=features
    value.call=lambda project,operation,payload:status
    return value


def test_old_matching_helper_does_not_advertise_unreported_media_capabilities():
    value=adapter();assert value.assert_ready() and value.features=={}
    value=adapter(dict(case_media=True,annotation_media_basis=True))
    assert value.assert_ready() and value.features==dict(case_media=True,annotation_media_basis=True)


@pytest.mark.parametrize('features',[dict(annotation_media_basis=True),dict(case_media='true'),dict(arbitrary=True)])
def test_unreported_or_malformed_media_capabilities_are_not_inferred(features):
    with pytest.raises(ForgeError):adapter(features).assert_ready()
