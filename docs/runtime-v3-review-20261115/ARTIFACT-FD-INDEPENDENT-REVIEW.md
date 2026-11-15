# Independent artifact FD review and closure

**Final scoped verdict: both independently reproduced FD composition gaps are closed by `27cf14f4447bf87f62f45e6b046abf075268fc1a`; the final independent selection passed 11 tests.** Whole-process RLIMIT and wide-data throughput remain outside this verdict. No production source was changed by this review.

Initial candidate `4c33cb125e0f4aec2e07c284ab65b610c5f1aa77` was independently tested at `845a135`. The initial basic FD correction passed, but final output consumption failed under a viable small budget. The historical evidence below is preserved unchanged; follow-up closure is recorded at the end.

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

## Follow-up closure

`40c7c18` made final merges retain the original downstream `PUT_FILES` window. At integrated `5cb8a03`, the exact original external two-arm counterexample and real typed/low-budget cases passed **10 tests / 12.79 s** (`/tmp/demiflow-fd-sort-review-fixed.xml`).

A second independent counterexample found the next precise seam: a high-budget store view and an active eleven-file view shared the same root and two writer FDs, but sorting calculated fan-in only from its own larger bounds. Five input runs failed opening readers; the test-only two-way control succeeded under the identical active eleven-file limit. `/tmp/demiflow-fd-sort-shared.xml` records **1 failed / 1 passed**, 3.44 s. This did not bypass the hard cap; it rejected work that could fit with a correct fan-in.

`27cf14f` gives `available_file_slots`, `require_file_slots` and actual acquisition one common lock-protected minimum-live-limit calculation. Sorting consumes that effective remaining window, then reserves its output headroom. The returned value is explicitly a planning snapshot, not a replacement grant; actual opens retain atomic admission.

Final integrated HEAD `4663920`: **11 passed / 13.43 s**, `/tmp/demiflow-fd-final-independent.xml`. Both original external red/control scripts pass unchanged. The same selection covers shared-view positive and before-read negative arms, original low-budget read/put/scratch refusal, native owner/view lifetime, failed-open cleanup, anti-bypass and actual concurrent read admission. The final source remained fixed. Exact final input hashes, commands, cases and both earlier follow-up reports are added to the paired JSON.

No original budget, assertion, typed payload, ordering or provenance requirement was weakened. The main checkout and shared environments remain unchanged. The parent separately owns the actual 40k throughput regressions and final frozen-candidate API matrices; this FD closure does not declare those passed.
