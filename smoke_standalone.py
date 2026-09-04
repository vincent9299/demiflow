"""demiflow 独立化 P0 冒烟：零配置本地 Dataset API（惰性路径 + write 动作）。"""

from demiflow.standalone import local_data
import tempfile, os, json

ctx = local_data()
data = ctx  # DataAPI 即 data 入口（无 .data 间接层）
out = (ctx.from_items([{"x": i} for i in range(10)])
       .map(lambda r: {**r, "y": r["x"] * 2})
       .filter(lambda r: r["y"] > 5)
       .take_all())
assert len(out) == 7, out
print("P0 冒烟1通过: map→filter→take_all =", len(out), "行")

with tempfile.TemporaryDirectory() as td:
    p = os.path.join(td, "out.jsonl")
    ctx.from_items([{"a": i} for i in range(5)]).map(
        lambda r: {**r, "b": r["a"] + 1}).write_json(p)
    rows = [json.loads(l) for l in open(p)]
    assert len(rows) == 5, rows
print("P0 冒烟2通过: write_json =", len(rows), "行")
