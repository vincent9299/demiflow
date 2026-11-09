"""Worker-host execution runtime (U2).

Lightweight exports only; importing this package never claims a run, opens a
journal, or loads Ray. The worker side consumes the frozen E2 K3 contract and
proxies the existing stream runtime under a granted fence.
"""
from .authority import WorkerAuthority, WorkerAction

__all__ = ["WorkerAuthority", "WorkerAction"]