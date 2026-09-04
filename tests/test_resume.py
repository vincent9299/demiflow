"""demiflow.collect.resume 测试：现算计数、行过滤、键提取、坏行容忍。"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

from demiflow.collect.resume import scan_counts


def test_scan_counts_basic():
    tmp = tempfile.mkdtemp()
    try:
        p = os.path.join(tmp, "m.jsonl")
        with open(p, "w", encoding="utf-8") as f:
            for row in [{"sha": "a", "owners": ["x", "y"]},
                        {"sha": "b", "owners": ["x"]},
                        {"sha": "a", "owners": ["y"]}]:
                f.write(json.dumps(row) + "\n")
        counts = scan_counts(p, key_of=lambda r: r["owners"])
        assert counts == {"x": 2, "y": 2}
        print("[PASS] 现算计数与键提取")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_row_filter_and_bad_lines():
    tmp = tempfile.mkdtemp()
    try:
        p = os.path.join(tmp, "m.jsonl")
        with open(p, "w", encoding="utf-8") as f:
            f.write(json.dumps({"sha": "a", "q": 9}) + "\n")
            f.write('{"broken\n')
            f.write(json.dumps({"sha": "b", "q": 3}) + "\n")
            f.write("\n")
        counts = scan_counts(p, row_filter=lambda r: r.get("q", 0) >= 8,
                             key_of=lambda r: [r["sha"]])
        assert counts == {"a": 1}
        print("[PASS] 行过滤与坏行容忍")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_missing_manifest_empty():
    assert scan_counts("/nonexistent/m.jsonl") == {}
    print("[PASS] 清单不存在返回空")
