# Complete durable input takes priority over early-stop prefix handling

This independently verified companion to `c598c85` closes the durable branch's remaining completion race. A true final cut succeeds immediately. Otherwise, genuine completion of every stage and source requires a fresh final cut before success. Only an incomplete-input terminal stop uses the existing committed-prefix result. The original coordinator, source/consumer ACK evidence, cut identities and published receipts stay authoritative.

## Actual prior failure

On `c598c85`, `/tmp/demiflow-durable-final-stop-proof.py` ran a real one-row `Dataset.from_items(...).take_all()` with 40 KiB resident memory, 16 KiB batch and 8 KiB row limits. Those original limits require two successive real stage allocations.

The harness delayed only the terminal's original replayed `edge_eoi` control command. Its data had really arrived; parking produced a non-final cut. The harness then delivered the original EOI, waited for real worker completion and complete original source evidence, called the normal `satisfy_terminal()`, and delivered the unchanged cut request. No state, business event, contribution or receipt was fabricated or reordered.

The driver returned the complete row with `final=False`, `all_done=True`, `inputs_complete=True`, and `stopping=True`. After cleanup, resuming the same declaration unnecessarily launched another real terminal worker and added a third commit: the already completed result was not recognized as final. Original log and events: `/tmp/demiflow-durable-final-stop-proof.log` and `/tmp/demiflow-durable-final-stop-proof/proof.json`.

## Minimal change and acceptance

`DataflowExecution._advance_cut` now prioritizes:

1. Original cut already final → complete.
2. All original stages and inputs genuinely complete → original receipt, pause, real final contributions and original coordinator commit.
3. Incomplete input and terminal stop requested → preserve original prefix result.
4. Otherwise → original phase retirement.

The new durable regression has stop/no-stop arms. It requires three authentic ACKs `[False, False, True]`, only the original two allocations, unchanged complete output, and resume with **zero** endpoints/allocations and an unchanged commit list. The original two native/live completion tests remain green.

A separate real prefix test runs `data.range(10000).take(2)` under the same small budget. It requires `inputs_complete=False`, an ACKed non-final cut, `terminal_satisfied`, source cursor at most two, and no source EOF. This locks the valid incomplete-input behavior rather than forcing every stop to consume or finalize the whole source.

- Combined completion + existing durable limit recovery + original thread/process early-close: **8 passed**, 35.02 s. `/tmp/demiflow-late-final-durable.xml`.
- Final five-case completion/prefix file: **5 passed**, 17.26 s. `/tmp/demiflow-late-final-durable-prefix.xml`.

The paired JSON records exact cases, XML and original harness hashes, final source hashes, command/environment and all seven frozen-file checks. The final five-case run kept source fixed; no shared environment or main checkout was changed. This is Local real stage/commit/recovery evidence, not a new multi-host, GPU, full API or content-GC claim.
