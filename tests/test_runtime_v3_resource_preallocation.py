"""Two actual public Dataset routes must reject before native allocation."""
from runtime_v3_resource_scenarios import arrow_worker_preallocation, lance_cast_preallocation
import pytest


@pytest.mark.parametrize('rejected', [True, False])
def test_worker_typed_conversion_is_admitted_before_arrow(tmp_path, rejected):
    arrow_worker_preallocation(tmp_path, rejected=rejected)


@pytest.mark.parametrize('case', ['conversion_negative', 'conversion_positive', 'cast_negative', 'cast_positive'])
def test_native_lance_cast_is_admitted_before_arrow(tmp_path, case):
    lance_cast_preallocation(tmp_path, case=case)


@pytest.mark.parametrize('rejected', [True, False])
def test_equal_width_cast_owns_input_and_target_at_the_same_time(tmp_path, rejected):
    from runtime_v3_resource_scenarios import equal_width_cast_residency
    equal_width_cast_residency(tmp_path, rejected=rejected)
