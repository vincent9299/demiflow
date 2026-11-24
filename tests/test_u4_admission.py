"""Resource description never acquires capacity or device reservations."""
import sys
import pytest
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.device_pool import DevicePool
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.manager import ResourceManager
from demiflow.resources.process_unit import ManagedProcessUnit
GIB=1024**3

def _capacity(cpu=4000, memory=4 * GIB):
    return {"cpu_millis": cpu, "memory_bytes": memory}


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Local managed units require Linux")
def test_plan_and_describe_are_pure_no_reservation(tmp_path):
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))
    pool = LocalCapacityPool(tmp_path / "pool", capacity=_capacity())
    devices = DevicePool(tmp_path / "devices", inventory={"devices": [
        {"device_id": "gpu-0", "type": "gpu", "memory_bytes": 16 * GIB}]})
    before_pool, before_devices = pool.snapshot(), devices.snapshot()
    assert pool.snapshot() == before_pool
    assert devices.snapshot() == before_devices
    provider = LocalResourceProvider(pool, ManagedProcessUnit(tmp_path / "units"),
                                     device_pool=devices)
    manager = ResourceManager(provider, state=None)
    described = manager.describe({"provider": "local"}, None)  # preflight probe, no reservation
    assert described["probe"]["effective"] == {"cpu_millis": 1, "memory_bytes": 1}
    assert pool.snapshot() == before_pool
    assert devices.snapshot() == before_devices
