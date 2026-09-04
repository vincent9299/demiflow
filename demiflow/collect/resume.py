"""demiflow 断点续跑原语：清单现算 done-set / 计数（消费者现场聚合，不落盘）。

自 collect_v2.op_coverage 沉淀（2026-09-04，机制归引擎）：
- 只读现算：不新增任何状态文件，续跑依据从真相清单每次重扫；
- 业务注入：row_filter（行合格判定，如质量门）与 key_of（行 → 键集，
  如归因键）；两者缺省全收/数行本身；
- 坏行容忍（与追加写清单的读端口径一致）。
"""

from __future__ import annotations

import json
import os
from typing import Callable, Optional

RowFilter = Callable[[dict], bool]
KeyOf = Callable[[dict], list]


def scan_counts(manifest: str, *,
                row_filter: Optional[RowFilter] = None,
                key_of: Optional[KeyOf] = None) -> dict:
    """扫 jsonl 清单现算 {键: 计数}；清单不存在返回空（全新存储）。

    row_filter(row) -> bool：False 的行不计（质量门口径由调用方定义）；
    key_of(row) -> Iterable[键]：一行的计数归属（如多个归因键各计一次），
    缺省按行整体计数（键为行号序）。
    """
    counts: dict = {}
    if not os.path.exists(manifest):
        return counts
    n = 0
    with open(manifest, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row_filter is not None and not row_filter(rec):
                continue
            keys = key_of(rec) if key_of is not None else (n,)
            for k in keys:
                counts[k] = counts.get(k, 0) + 1
            n += 1
    return counts
