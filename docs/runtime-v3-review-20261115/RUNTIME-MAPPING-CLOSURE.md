# Runtime Mapping checks — narrow closure

Source: `9c2fe5007b591ab78801f0f14e74c8dbe183c731`. Only the 12 runtime `isinstance` predicates in `execution/contracts/dataflow.py` now use the underlying `collections.abc.Mapping`; public `typing.Mapping` annotations, DTO fields, validation and wire schemas are unchanged. The worker profile previously measured 624,000 typing proxy checks (1.286 s cumulative in a perturbed profile), which justified this bounded edit.

Original DTO, codec escaping/rejection, record clone, array fidelity, block and terminal collection suites: **237 passed, 1 skipped** (98.29 s). The skipped Ray block test requires the Ray interpreter; it is not counted as passing. Initial evidence observer failed when an existing codec test deliberately replaced global JSON functions; only the temporary observer was corrected and the entire same selection rerun. Production tests were not changed.

Original 20,000 → 10,000 row pipeline, four-worker upper bound, 256 MiB, 120 s: correct output, 40 original ACKs, original pool empty, no remaining children. Elapsed 15.295 s is one concurrent diagnostic, not a paired performance result. The remaining gap to the old baseline is not closed.

`RUNTIME-MAPPING-VERIFICATION.json` binds the exact receipts to this source commit (2,191 test/source and 1,050 production input digests, zero mismatches). Frozen seven files unchanged; no main checkout or shared environment writes.
