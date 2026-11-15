# Device grant assembly and verified boundary

The unified driver now connects explicitly configured devices to the existing
`resources.DevicePool`, existing Local/Ray providers, ResourceScope accelerator
budget and physical HostDescriptor. It does not create a new device allocator.
Two runs in the same capacity domain use the same existing device ledger.

A deployment can set `DEMIFLOW_DEVICE_INVENTORY` to a JSON file (at most 64 KiB)
using the existing DevicePool schema, for example:

```json
{"devices":[{"device_id":"GPU-deployment-uuid","type":"gpu","memory_bytes":17179869184}]}
```

The UUID is an explicit deployment declaration, not automatic hardware discovery.
For Local, the actual DevicePool reservation supplies `CUDA_VISIBLE_DEVICES` to
the managed child. A CPU-only child gets an empty device mask when this explicit
inventory is configured. Device charges survive unknown exits and are returned
only by the existing confirmed-exit path. An absent inventory does not silently
turn a GPU request into a CPU execution.

Ray Core receives `num_gpus` through its existing provider and selects the actual
visible IDs. This adapter supports an explicit inventory for exactly one GPU
host, bounded by Ray's reported GPU capacity. Multiple GPU hosts are rejected:
per-host device-memory binding has not been proved. A Ray logical device ledger
label is not claimed to be a physical CUDA UUID assignment.

Nested Local workers and Ray actors cannot reopen a device pool and advertise
the parent's devices as new capacity. Nested explicit GPU admission remains
unsupported until it can subdivide the parent's original device grant.

The existing public syntax is retained, even on Local:
`map(fn, backend_options={"ray": {"worker_resources": {"gpu": 1}}})`.
The legacy namespace is parsed into the unified physical profile; it does not
invoke Ray Data. API naming cleanup is outside this change.

## Evidence and limits

`tests/test_runtime_v3_devices.py` executes real Dataset workers using a
controlled inventory and reads the child process/actor device visibility. It
checks actual original-ledger release, CPU visibility exclusion, rejection
without inventory, and nested-authority exclusion.

- `/tmp/demiflow-residency-devices6.xml`: **24 passed, 1 skipped**, covering the new
  Local proofs, 15 existing device-authority tests, and 5 resource progress tests.
- `/tmp/demiflow-residency-ray-device3.xml`: **2 passed** using `env-ray`, including
  the skipped controlled logical-GPU proof and the same finite-residency proof.
  Both use private `address='local'` clusters and `finally: ray.shutdown()`.
- `/tmp/demiflow-residency-devices4.xml`: earlier combined finite/device batch,
  **23 passed, 2 skipped** (before the final nested-worker exclusion assertion).

No CUDA library or real GPU inference ran. The Ray proof declares one controlled
logical GPU to its private cluster and verifies Ray's assigned visibility `0`.
The Local proof uses `GPU-test-controlled` solely as an observable child mask.
These are actual scheduling/grant/exit proofs, not evidence that physical GPU
kernels or accelerator-memory hard isolation were exercised. All files and
capacity domains used by tests live under `/tmp/demiflow-*`.
