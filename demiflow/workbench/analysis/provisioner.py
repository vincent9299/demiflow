"""The sole Jupyter provisioner in the fixed, helper-created kernel image."""
from __future__ import annotations

import sys
import os
from pathlib import Path

from jupyter_client.provisioning.local_provisioner import LocalProvisioner

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.sandbox import require_job
from demiflow.workbench.analysis.job_context import current
from demiflow.workbench.analysis.job_context import job_path


def kernel_argv():
    context = current()
    if context is not None:
        value = context.value
        return [value['python'], '-I', '-B', str(Path(value['code_root'])/'demiflow/workbench/analysis/trusted_entry.py'),
            '--code-root', value['code_root'], '--operation', 'kernel', '--context', context.manifest_path,
            '--context-sha256', context.manifest_digest, '-f', '{connection_file}', '--HistoryManager.enabled=False']
    return [sys.executable, "-I", "-B", "-m", "ipykernel_launcher", "-f", "{connection_file}",
            "--HistoryManager.enabled=False"]


def kernel_environment():
    context = current()
    if context is not None:
        return dict(PATH=str(Path(context.value['python']).parent)+':/usr/bin:/bin', HOME=job_path('/work'),
            TMPDIR=job_path('/scratch'), MPLBACKEND='module://matplotlib_inline.backend_inline',
            MPLCONFIGDIR=job_path('/scratch/matplotlib'), IPYTHONDIR=job_path('/scratch/ipython'),
            JUPYTER_RUNTIME_DIR=job_path('/work/jupyter'), JUPYTER_CONFIG_DIR=job_path('/work/jupyter-config'),
            JUPYTER_PLATFORM_DIRS='1', PYDEVD_DISABLE_FILE_VALIDATION='1', PYTHONDONTWRITEBYTECODE='1',
            DEMIFLOW_EXECUTION_UNIT_FD=str(context.value['unit_fd']),
            TOKIO_WORKER_THREADS='1',RAYON_NUM_THREADS='1',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',
            MKL_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',LANCE_CPU_THREADS='1')
    return dict(PATH="/usr/local/bin:/usr/bin:/bin", HOME="/work", TMPDIR="/scratch",
        LD_LIBRARY_PATH="/usr/local/lib", MPLBACKEND="module://matplotlib_inline.backend_inline",
        MPLCONFIGDIR="/scratch/matplotlib", IPYTHONDIR="/scratch/ipython",
        JUPYTER_RUNTIME_DIR="/work/jupyter", JUPYTER_CONFIG_DIR="/work/jupyter-config",
        JUPYTER_PLATFORM_DIRS="1", PYDEVD_DISABLE_FILE_VALIDATION="1",
        TOKIO_WORKER_THREADS="1", RAYON_NUM_THREADS="1", OMP_NUM_THREADS="1",
        OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", NUMEXPR_NUM_THREADS="1", LANCE_CPU_THREADS="1")


class SandboxProvisioner(LocalProvisioner):
    async def pre_launch(self, **kwargs):
        require_job("kernel")
        if (self.kernel_spec.argv != kernel_argv() or self.kernel_spec.env
                or kwargs.get("cwd") != "/work" or kwargs.get("env") != kernel_environment()):
            c.reject("Kernel command/environment must match the reviewed fixed image")
        return await super().pre_launch(**kwargs)
