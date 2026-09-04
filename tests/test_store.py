"""demiflow.collect.store 测试：幂等去重、原子写、跨进程吸收式尾扫。"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import shutil
import tempfile

import pytest

from demiflow.collect.store import AppendManifestStore


def key_of(rec: dict) -> list:
    return [(rec.get("sha"), i) for i in rec.get("owners") or [""]]


def rec_of(sha: str, owner: str, blob: str) -> dict:
    return {"sha": sha, "owners": [owner], "path": blob}


async def test_write_and_idempotent_skip():
    tmp = tempfile.mkdtemp()
    try:
        st = AppendManifestStore(manifest=os.path.join(tmp, "m.jsonl"),
                                 lock_path=os.path.join(tmp, ".lock"))
        assert st.load_index(key_of) == 0
        blob = os.path.join(tmp, "ab", "x.png")
        assert await st.write(data=b"hello", blob_path=blob,
                              key=("s1", "A"), record=rec_of("s1", "A", "ab/x.png"))
        assert os.path.exists(blob) and open(blob, "rb").read() == b"hello"
        rows = [json.loads(l) for l in open(st.manifest)]
        assert len(rows) == 1 and rows[0]["sha"] == "s1"

        # 同 key 幂等跳过；不同 key 追加新行（同 blob 不重写）
        assert not await st.write(data=b"hello", blob_path=blob,
                                  key=("s1", "A"), record=rec_of("s1", "A", "x"))
        assert await st.write(data=b"hello", blob_path=blob,
                              key=("s1", "B"), record=rec_of("s1", "B", "x"))
        rows = [json.loads(l) for l in open(st.manifest)]
        assert len(rows) == 2
        assert st.contains(("s1", "B")) and not st.contains(("s1", "C"))
        print("[PASS] 写入/幂等跳过/跨键追加")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def test_concurrent_writes_no_loss_no_dup():
    tmp = tempfile.mkdtemp()
    try:
        st = AppendManifestStore(manifest=os.path.join(tmp, "m.jsonl"),
                                 lock_path=os.path.join(tmp, ".lock"))
        st.load_index(key_of)

        async def one(i: int):
            blob = os.path.join(tmp, "sh", f"b{i}.bin")
            return await st.write(data=f"d{i}".encode(), blob_path=blob,
                                  key=(f"s{i}", "A"),
                                  record=rec_of(f"s{i}", "A", f"sh/b{i}.bin"))
        results = await asyncio.gather(*(one(i) for i in range(20)))
        assert all(results)
        rows = [json.loads(l) for l in open(st.manifest)]
        assert len(rows) == 20
        assert len({(r["sha"], tuple(r["owners"])) for r in rows}) == 20
        print("[PASS] 并发写无丢行无重复")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _mp_writer(tmp: str, shas: list):
    async def run():
        st = AppendManifestStore(manifest=os.path.join(tmp, "m.jsonl"),
                                 lock_path=os.path.join(tmp, ".lock"))
        st.load_index(key_of)
        for sha in shas:
            blob = os.path.join(tmp, sha[:2], f"{sha}.bin")
            await st.write(data=sha.encode(), blob_path=blob,
                           key=(sha, "MP"), record=rec_of(sha, "MP", f"{sha}.bin"))
    asyncio.run(run())


async def test_cross_process_absorb_and_dedup():
    """两进程并发写共享键：锁内吸收式尾扫保证同 key 只落一行。"""
    tmp = tempfile.mkdtemp()
    try:
        shared = ["sA", "sB"]
        ps = [multiprocessing.Process(target=_mp_writer,
                                      args=(tmp, spec))
              for spec in (shared + ["s1"], shared + ["s2"])]
        for p in ps:
            p.start()
        for p in ps:
            p.join()
        assert all(p.exitcode == 0 for p in ps)
        rows = [json.loads(l) for l in open(os.path.join(tmp, "m.jsonl"))]
        keys = {(r["sha"], tuple(r["owners"])) for r in rows}
        assert len(keys) == len(rows), "跨进程同 key 双写"
        assert keys == {("sA", ("MP",)), ("sB", ("MP",)),
                        ("s1", ("MP",)), ("s2", ("MP",))}
        print(f"[PASS] 跨进程吸收式去重（{len(rows)} 行无一重复）")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


async def test_index_rebuild_and_bad_line_tolerance():
    tmp = tempfile.mkdtemp()
    try:
        st = AppendManifestStore(manifest=os.path.join(tmp, "m.jsonl"),
                                 lock_path=os.path.join(tmp, ".lock"))
        st.load_index(key_of)
        blob = os.path.join(tmp, "s", "x.bin")
        await st.write(data=b"d", blob_path=blob, key=("s", "A"),
                       record=rec_of("s", "A", "s/x.bin"))
        with open(st.manifest, "a", encoding="utf-8") as f:
            f.write('{"broken json\n')
        fresh = AppendManifestStore(manifest=st.manifest,
                                    lock_path=st._lock_path)
        assert fresh.load_index(key_of) == 1
        assert fresh.contains(("s", "A"))
        print("[PASS] 索引重建与坏行容忍")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
