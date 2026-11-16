# Fast CPU remaining cost: fixed profile and narrow scalar fix

The 26.9828 s unprofiled Local fast run remains the valid preceding diagnostic, not a performance closure. A single subsequent profile of the same original 20,000-row/4-worker/256 MiB/120 s case used production 266aa54 (documentation HEAD e58139f), unchanged before/after. The parent concurrently ran a Local four-worker managed-object functional test. Its **39.4717 s elapsed is diagnostic only**, not a timing comparison. Output, final ACK and cleanup still passed; no budget was enlarged. Profile and exact evidence hashes are in the companion JSON.

## What batching actually changed

The driver processed 52,822 original WorkerEvents. Original business commands were still 50,297, but actual bounded sends were 3,468 (0.173 s cumulative). The 10,000 terminal data frames used 1,071 TCP connections (0.090 s in create_connection), confirming real ready batching rather than only a new method name. Per-record identity, sequence, credit and ACK have not been aggregated away.

The largest remaining driver CPU cost was original decode_wire_document: 10.652 s cumulative, including full-frame _measure_document 6.241 s; 2,197,702 string measurements used 3.392 s. Other cumulative costs: thaw_record_value 605,670 recursive calls / 3.105 s; startup activation 3.209 s; original final GC 2.985 s. select 7.189 s includes waiting and is especially affected by concurrent activity. These numbers overlap; they must not be added as disjoint runtime categories.

## Rejected small candidate

A bounded table of 244 known schema field widths passed 178 semantic tests. Five alternating 5,000-event micro-runs gave decoder medians 0.30048 s versus 0.29037 s (3.36% local reduction). The extra table/maintenance did not address the dominant intention, so the entire uncommitted change and its dependent tests were withdrawn. Raw exploratory evidence remains; it is not presented as delivered code.

The common WorkerEvent birth envelope has nine fields; four fields vary per event. A new shared wire frame would reduce repeated metadata, but would still require original per-event fence/identity/sequence/ACK validation and DTO semantics. Its estimated savings cover only part of codec time. No new wire schema, trusted-payload bypass or business-payload cache was introduced. Parent is independently evaluating safe source fusion to eliminate unnecessary cross-process source delivery; this lane does not change the planner.

## Delivered ddc1e10

The existing thaw_record_value now returns only **exact builtin** str/int/float/bool/bytes/None immediately. Every subclass, container, typed list/set marker, NumPy scalar/array and other value continues through the original unchanged path. This removes repeated Mapping/type/NumPy dispatch for already immutable ordinary scalars; it changes no validation, serialization, historical bytes, negative-zero sign, alias ownership or commit authority.

Fixed-source validation: **126 passed in 37.26 s**. Selectors: test_runtime_v3_scalar_values.py, test_runtime_v3_array_values.py, test_runtime_v3_codec_cost.py, test_runtime_v3_control_codec.py, test_runtime_v3_record_clone.py. Includes actual scalar/NumPy materialization and original committed replay, canonical-byte and closed-wire checks, array detach/copy rules, plus explicit negative-zero/bytes/subclass identity and list/tuple/set marker non-aliasing.

A separate pure-function microbenchmark used the same realistic immutable data-event payload against the exact old method extracted from e58139f and the candidate: five alternating 10,000-call runs, medians **0.20789 s → 0.02424 s (0.1166×)**. This is only payload thaw cost, not an 8.6× Dataset speedup. No full Dataset timing was repeated for this four-line change; parent will judge it in the final integrated candidate. Seven frozen files and main checkout are untouched. All owned test/performance processes have ended.
