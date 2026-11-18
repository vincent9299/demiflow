# Engine known state

What the engine tests show at one candidate, per environment, by test id. The tool is
`tests/engine_known_state.py`; it only reads receipts written by
`tests/runtime_v3_regression_inventory.py`.

| File | What it is |
|---|---|
| `74d2f99.json` | The known state at candidate `74d2f9941377`: every collected test id per lane, and for every test that is not a plain pass its phase, failure type, reason, located cause and ruling |
| `annotations.json` | The input that is not in any receipt: located causes, rulings, what was deliberately not run, results obtained without a receipt, renamed or resolved earlier records |

Lanes in `74d2f99.json`:

| Lane | Environment | Files |
|---|---|---|
| `standard` | standard Python (no Ray, no GPU) | the v3 regression set |
| `ray` | Ray environment | the files of that set that need Ray |
| `historical` | standard Python | execution tests that predate v3 |
| `historical_ray` | Ray environment | the files of that group that need Ray |

Compare a later run of one lane against it (exit code 1 when anything differs):

    python tests/engine_known_state.py compare docs/engine-known-state/74d2f99.json <lane> \
        <collection receipt> <execution directory>...

Build a document for another candidate from a specification (lanes, their receipts, the annotations):

    python tests/engine_known_state.py build <spec.json> <out.json>

A located cause is not a ruling. An entry without a ruling stays `undetermined`; nothing
here marks a test as expected to fail.
