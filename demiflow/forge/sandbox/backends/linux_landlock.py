"""Linux Landlock + seccomp + user/pid/net namespace sandbox backend.

Works on bare-metal Linux AND inside nested Docker containers without
CAP_SYS_ADMIN or docker.sock access. Delegates directly to ``sandbox.py``.
"""
from __future__ import annotations


class LinuxLandlockBackend:
    name = 'linux_landlock'

    def identity(self) -> dict:
        from demiflow.forge.sandbox.sandbox import execution_identity
        return execution_identity()

    def readiness(self) -> dict:
        from demiflow.forge.sandbox.sandbox import backend_readiness
        return backend_readiness()

    def launch(self, command, *, spec, env, cwd, lease_fd=None, control_socket=None, model_socket=None):
        from demiflow.forge.sandbox.sandbox import launch_sandbox
        return launch_sandbox(
            command,
            spec=spec,
            env=env,
            cwd=cwd,
            lease_fd=lease_fd,
            control_socket=control_socket,
            model_socket=model_socket,
        )
