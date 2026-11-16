# Wide native model and publication workspace candidate

Production candidate in `gpt_finite_drain_20261117`, branch `engine/gpt-wide-model-20261117`, base dcd27b2. Main/root untouched.

Native Prompt/Embedding/Agent/routed profiles share the existing codec workspace algorithm and preserve explicit worker memory/CPU/GPU plus model state. Only proven default codec slack can move within the original stream group; original R/B/M and runtime peak guards remain authoritative.

New native terminal configuration `publication_workspace={schema: demiflow.native_publication_workspace.v1, resident_bytes: N}` gives the single publication thread an explicit reservation. New physical `residency.control_reserved_bytes` means original control reservation plus this native publication reservation. The existing scheduler subtracts the full value before admitting any worker; the original final cut/ACK/lifecycle and allocator are unchanged. The driver passes that sealed physical value to the sink; a declaration without the matching original reservation is rejected. Old seals without the field retain zero new reservation and their prior behavior; no dataclass default, old identity or history is rewritten.

B remains each business batch payload ceiling. A new native sink bounds input frames, current records, manifest/index view, Python rows, Arrow constructor and retained writer buffers inside N before allocation. Existing equal-width Arrow cast input+output guard remains. Existing callers of batch-conversion admission keep the original B-bound behavior unless using this explicit native reservation. Native batch rows also intersect the original run batch_rows. Queue result terminals that only publish artifacts do not reserve Arrow conversion space.

Default M=256MiB/R=B=16MiB: control=32MiB; source/terminal/driver default minima 32/48/32MiB and targets about96/112/112MiB share the remaining224MiB; resulting about66.46/82.46/75.08MiB remain under M. The original 65,005,338B frame peak fits without granting each stage the full run.

Boundary: original64/128MiB wide join cases remain open after separate source sealing repair. Their old join B+6R grant and original JSON/typed copies may already exceed actual admitted phase capacity. This candidate does not raise memory, reduce data, change original explicit payload caps, or relabel their rejection as success. They require actual copy reduction/Arrow carrier reuse or genuinely separate finite phases.

Final production bytes are unchanged from 5e06769 through c6af2ae. R8 has9 business/assertion passes on those production bytes plus1 newly authored fixture failure; R10 corrects only that new fixture (distinct nested containers), and proves frame/index admission refuses before decoding/construction with zero constructor calls. This is scoped reused evidence, not a claim of a single all-green suite at c6af2ae.

R5 original13 passed before shared-reader growth hardening; R8 preserves original3 failure/recovery windows, exact full-payload/paid-effect replay, two adjacent9.75MB rows, wide/small multi-segment data, run batch_rows2, and Blob positive/negative. R10 proves the new actual-Python-tree/index early refusal. R2/R3 model/routed/Embedding/unknown-effect and explicit memory negative are prior scoped evidence. Original equal-width Arrow input+target cases passed in R5 and their Lance cast authority remains unchanged.

R6 old Blob rejection depended on incorrectly using single-payload B for the sum of copies. Parent ruled its14KB/RB32KiB/M128MiB combination valid. Same data now has a real success test; its negative only tightens M to384KiB and still must reach native conversion admission before constructor/publication. Both passed R8. The original real cast2.108x negative was not changed.

All formal batches used four private domains/TMPDIR, fixed code and all three pytest phases; every completed batch has zero changed inputs and no remaining children. Preserve all earlier failures including new-fixture mistakes; no retries were merged into green reports.


Commit integration: production/model c2db4ab; publication d11c95f; driver-only two-line call2be6dbd; view/queuee7991bd; common reader growth8225aca; prior-output target5e06769. Test followups e0ce470,108338b,f95b0e0,a3993ae,c6af2ae. Root-owned test migrations0e55a48,a168880,f9300b8 were cherry-picked as e5f6d4b,6137e37,2adc819; do not duplicate them.
