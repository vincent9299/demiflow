"""Bind an HTTP service to its allocated worker and release it after confirmed exit."""
import os
import signal
import subprocess
import time

from ..resources.process_group import spawn_command
from .contracts.dataflow_errors import DataflowError


async def close_owned_service(owner):
    """Bound control wait; unknown tree ownership retains its original unit."""
    import asyncio
    from ..services.contracts import ModelServiceError
    process = owner._process
    if process is not None:
        try:
            os.kill(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + owner.declaration.shutdown_timeout_s
        while process.poll() is None and time.monotonic() < deadline:
            await asyncio.sleep(min(.01, max(0, deadline - time.monotonic())))
        if process.poll() is None:
            # Never turn failed observation into a synthetic exit by killing
            # its guardian. The guardian owns the same FD while it retries.
            raise ModelServiceError('Managed service cleanup is unconfirmed; original unit and port ownership retained')
        process.wait(timeout=0)
        owner._process = None
    if owner._log_pump is not None:
        pump, owner._log_pump = owner._log_pump, None
        await asyncio.to_thread(pump.close)
    owner._stack.close()


def spawn_owned_service(spec, *, env, resource_fds=()):
    binding = None
    if getattr(spec, '_worker_gpu_requested', False):
        check = getattr(spec, '_worker_device_check', None)
        if check is None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed GPU service has no original worker device grant')
        binding = check()
        env = {**env, 'CUDA_VISIBLE_DEVICES': ','.join(binding['device_ids']),
               'CUDA_DEVICE_ORDER': 'PCI_BUS_ID'}
    return spawn_command(spec.command, cwd=spec.root, env=env, shutdown_timeout_s=spec.shutdown_timeout_s,
                         max_rss_bytes=binding['max_rss_bytes'] if binding else None,
                         resource_fds=resource_fds, stdout=subprocess.PIPE)
