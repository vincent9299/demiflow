# Original tuple result: codec allocation measurement

Read-only diagnosis at root integration `2b98152`, in isolated branch `engine/gpt-tuple-codec-peak-20261117`. **No production, compiler, budget or historical test was changed.** The preceding production carrier repair `b5530d9` is preserved.

The probe reconstructs the exact original 24×2×2 join-and-reduce expected rows using the existing `oracle_join`; it retains every tuple element, wide key and null group. It invokes the original record encoder/decoder directly. The runtime's parent/source/schema sidecar and outstanding peer/kernel residency are deliberately absent from this diagnostic; they are additional costs in the real terminal. This is a codec allocation probe, not a substitute for the actual Dataset acceptance test.

## Full original decode

All values below are bytes. `Actual peak` includes the raw bytes object already alive before starting tracemalloc. Imports are warmed, `gc.collect()` runs before each measured interval, and output equality is asserted. This measures Python allocation under this interpreter, not process RSS or a universal allocator-independent upper bound.

| Original budget R | Key bytes | Group | Tuple count | Actual typed payload | Raw document | Current admission estimate | Actual peak |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 4096 | 400 | a | 32 | 2856 | 2895 | 111583 | 99435 |
| 4096 | 400 | b | 32 | 2860 | 2899 | 111595 | 99447 |
| 4096 | 400 | null | 8 | 583 | 621 | 29013 | 28383 |
| 16384 | 6144 | a | 32 | 8600 | 8639 | 123071 | 122411 |
| 16384 | 6144 | b | 32 | 8604 | 8643 | 123083 | 122423 |
| 16384 | 6144 | null | 8 | 583 | 621 | 29013 | 28383 |

The 4 KiB row limit is correctly satisfied by the actual 2856-byte typed payload. Its decoded tagged JSON contains many independent Python dictionaries, strings, lists and tuple leaves. The current 34816-byte consumer grant cannot hold that codec work; reducing the guard to fit it would admit a real overrun. For the 16 KiB case, even the isolated decoder nearly fills 126976 bytes, before the real sidecar and simultaneous retained buffers are counted.

## Phase evidence

Each row below measures the named original operation with its existing object graph counted once by identity. Phase peaks are independent and must not be summed.

| Phase | 4 KiB case | 16 KiB case |
| --- | --- | --- |
| Original JSON parse | 42713 | 59945 |
| Decode tagged values | 42584 | 54072 |
| Construct canonical second tag tree | 69037 | 80525 |
| Canonical payload JSON encoding | 83177 | 106153 |
| Freeze returned Record | 49332 | 66460 |

JSON parsing alone already exceeds the 4 KiB case's 34816-byte grant. The second canonical tree is a real allocation, but removing it cannot make the original parser fit that grant. Canonical JSON encoding also has its own temporary workspace. The current whole-function admission is close to the actual whole-function peak, particularly for the 16 KiB row, rather than a fictitious twofold charge.

Root therefore retained the codec guard and chose to adjust the **new-plan default** workspace target within original `M-control` and existing metadata limits. That change must preserve explicit worker limits, historical sealed plans and original 19/40 KiB small-data success cases. A default target does not promise that every possible dense row can fit; actual shape admission remains authoritative. No further production optimization was made in this diagnostic lane.

## Reproduce and evidence

From the isolated worktree:

```bash
PYTHONPATH="$PWD:$PWD/tests" PYTHONDONTWRITEBYTECODE=1 \
  /yzp/zhaozy/yangzepeng/0905/env/bin/python -B \
  docs/runtime-v3-review-20261115/TUPLE-CODEC-WORKSPACE-PROBE-20261117.py \
  > /tmp/demiflow-tuple-codec-peak-r1/reproduced.json
```

The script only reads source and invokes pure codecs. It does not start Dataset, Ray, RunContext, services or GPU work. `TUPLE-CODEC-WORKSPACE-20261117.json` records the interpreter, five source/fixture digests, probe digest, exact row payload digests, full and phase measurements. Early measurements at `/tmp/demiflow-tuple-codec-peak-r1/{probe,phases}.json` differ by a few hundred bytes of interpreter bookkeeping but reach the same conclusion. Main checkout and shared environments were not touched.
