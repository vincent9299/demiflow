"""Cumulative statistics returned by Dataset.run_stream."""
from __future__ import annotations


class StreamStats:
    """流式执行计数（引擎口径；业务口径由管线在算子闭包内自持）。"""

    def __init__(self) -> None:
        self.stages: dict[str, dict[str, int]] = {}
        self.timings = {}
        self.metrics = {}
        self.stage_policies = {}
        self.outputs = {}
        self.miss: dict[str, int] = {}
        self.dead_batches: list = []      # 攒批整批失败内容（活性层，有界）
        self.dead_batches_dropped: int = 0  # 超 1000 批后的只计数不留存

    def stage(self, name: str) -> dict[str, int]:
        return self.stages.setdefault(name, {"in": 0, "emitted": 0})

    def add_miss(self, reason: str) -> None:
        self.miss[reason] = self.miss.get(reason, 0) + 1

    def observe_duration(self, name, seconds):
        from demiflow.execution.journal.run_telemetry import LatencySummary
        if name not in self.timings:
            self.timings[name] = LatencySummary()
        self.timings[name].observe(seconds)

    def timing_summary(self):
        return {name: value.summary() for name, value in self.timings.items()}

    @property
    def emitted(self) -> int:
        """末级产出行数（管线最终输出；per-stage 细账看 stage(name)/stages）。"""
        if hasattr(self, '_committed_output'):
            return self._committed_output
        if not self.stages:
            return 0
        return next(reversed(self.stages.values()))["emitted"]

    def summary(self) -> str:
        st = ", ".join(f"{k}[in={v['in']}→out={v['emitted']}]"
                      for k, v in self.stages.items())
        miss = ", ".join(f"{k}×{v}" for k, v in
                         sorted(self.miss.items(), key=lambda x: -x[1]))
        return st + (f"\nAcknowledged missing: {miss}" if miss else "")
