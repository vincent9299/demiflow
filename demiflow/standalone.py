"""demiflow 独立运行入口：零配置构造本地 Dataset API。

契约（一期 2026-09-04）：
- 直接 Dataset 用法（from_items/map/filter/.../write_*）不需要任何平台配置：
  本模块绑定 DataAPI + LocalDatasetExecutor，绕开 Candidate/LocalPipelineBackend
  路径（那条路才有 platform_requirements/wheelhouse 耦合）；
- streaming 路径：Dataset.map_async 挂 async 算子，run_stream() 触发
  （常驻 worker 协程 + 有界队列 + 无序发射，见 execution/stream.py）；
- 惰性路径：纯 sync 算子走既有 action（take_all/count/write_*），行为与原仓一致。
"""

from __future__ import annotations

from .data.api import DataAPI
from .execution.executors.local import LocalDatasetExecutor


def local_data(workers: int = 4, *, block_size: int = 256) -> DataAPI:
    """零配置本地 Dataset API（进程内线程池执行器）。

    workers/block_size 语义与 LocalDatasetExecutor 一致（惰性路径的
    线程池宽度与批大小；streaming 路径的并发由各 map_async 自己声明）。
    """
    return DataAPI(LocalDatasetExecutor(workers=workers, block_size=block_size))
