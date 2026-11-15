# Independent artifact FD review: final sort consumption still blocked

Candidate `4c33cb125e0f4aec2e07c284ab65b610c5f1aa77`; isolated integrated test HEAD `845a135`. No production source was changed by this review. **The basic FD correction passed the independent selection, but final sort output is not yet composable under a viable small budget.** The owner is fixing that separately; this report preserves the exact prior failure and successful control arm.

## Passed scope

`/tmp/demiflow-fd-independent-tests.xml`: **16 passed / 16.01 s**.

The selection ran all eight new physical-FD tests, an independently added concurrent-read test, both prior interrupted-GC/generation tests, live reader and original native-journal lifetime tests, nested historical/pending roots, and the existing real typed sort and join seams. No H/Ray doubles were collected.

The extra concurrency case synchronizes two real generators against a three-slot store. Exactly one is admitted; the other returns `BUDGET_EXCEEDED` before opening another data file. `/proc/self/fd` shows three actual artifact descriptors and the same three occupied slots. Closing the generator returns to one writer slot/FD; closing the writer returns both to zero. A subsequent read succeeds in the reclaimed window.

Source review confirms shared per-process/physical-root accounting, weak lifetime ownership, minimum active view limits, native owner accounting and generator-lifetime return. Existing GC generation tests still prevent an interrupted old reclamation from refunding a new same-hash publication. This is artifact ownership admission, not a claim that this counter covers every provider socket or SQLite FD in the process.

## Confirmed consumer boundary failure

Root identified the final merge's missing downstream headroom; the independent test reproduced it with real files and the original store:

- Five real typed sort runs, keys 5 through 1, unchanged ten-file bound.
- Iterate `merge_sort_runs`; immediately publish each yielded record using the same original `RunArtifactStore.put`. Do not exhaust/list the iterator before writing.
- Current policy chooses a final fan-in of three after its intermediate pass. At the first yield, `/proc/self/fd` and live counters both show **7** artifact descriptors/slots.
- The real downstream `put` requires five additional admitted slots and fails with `BUDGET_EXCEEDED`. This is not an inherently impossible workload.
- Control arm changes only the test-local merge fan-in constant to two. It retains the exact input and ten-file budget. At the first yield there are **5** actual descriptors/slots; all five records are sorted and published successfully, preserving typed data.

`/tmp/demiflow-fd-sort-tail.xml`: **1 failed (current policy), 1 passed (two-way control), 3.46 s**. No test assertion or resource budget was weakened. The failure happens in the actual downstream store admission, not a fake consumer.

The implementation computes `final_fan_in` from all currently available reader slots, while its intermediate fan-in reserves `PUT_FILES`. A final yield leaves the readers open, so downstream output still needs a real share of the same finite window. The follow-up must either carry the actual consumer requirement or conservatively retain the necessary headroom. Merely converting the iterator to a list before output would change the capability and is not an acceptable repair.

Reproductions:

- `/tmp/demiflow-fd-independent/test_concurrent_reads.py`
- `/tmp/demiflow-fd-independent/test_sort_consumer.py`
- `/tmp/demiflow-gc-independent/test_gc_generation.py`

The paired JSON embeds those exact sources, input and XML hashes, all selected cases, and descriptor observations for both sort arms. All seven protected files remain original. The main checkout and shared environment were untouched. No full API/Ray matrix or wide performance claim is made by this review; parent coordination owns those runs.
