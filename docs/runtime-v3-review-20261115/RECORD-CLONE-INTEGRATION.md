# P0: frozen source provenance and physical block composition

The integrated task-state and block-layout changes exposed a real failure: `preserve_block` used `dataclasses.replace` on a validated Record whose nested control-JSON source position was already frozen. Reconstructing the public DTO then rejected its own nested mappingproxy, so an ordinary map failed. Other Record-copy paths had the same problem.

`dataflow_records.clone_record` accepts an exact validated Record, converts only its original source-position control JSON through the wire codec's `_thaw_json`, and invokes the complete public Record validation again. Explicit replacement values keep normal validation. Raw tuple, bytes, custom objects and nested external mappingproxy remain rejected; business payload tuple/bytes types are unchanged. No public constructor is widened.

The single helper now covers block assignment/preservation/reordering, runner hydration and queue finalization, Lance key hydration, producer provenance, task provenance, and native cached forwarding. Native forwarding retains identity/block metadata but clears the obsolete input row schema. `_output_record` also copies source positions with the control-JSON helper, so frozen JSON arrays become control arrays rather than business tuples.

Final focused verification: **8 passed in 15.38s**. This includes strict public-input negatives and a real map→repartition→map→Arrow batch→Lance create→same-RunRef recovery, with exact output, version 1, original ACKs and no repeated callback events. A native-forwarding component test separately verifies schema invalidation and identity/block preservation.

The integration diagnostic had **32 passed, 3 failed, 1 Ray skip**. All 3 remaining failures are task-test assumptions exposed by the newly effective worker ceiling: physical resource reconstruction can cause extra lifecycle events without extra business calls; a saved 3-worker memory cohort correctly rejects a later 2-worker ceiling before new business execution. The physical plan and real stack confirmed RESOURCE_INFEASIBLE; an earlier tentative hang explanation was incorrect. Those tests will gain distinct feasible-recovery and tightened-ceiling rejection arms, with no weakened business assertions.

`RECORD-CLONE-INTEGRATION.json` binds the tested source and raw XML evidence. This commit excludes the separate runtime-observation work. Main repository untouched; seven protected modules still match 49b666f.
