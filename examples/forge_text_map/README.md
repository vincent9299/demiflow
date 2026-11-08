# forge-text-map

Trim text and optionally uppercase it without changing row identity.

## Contract

This project is bound to the built-in demiflow Forge contract in `contract.lock.json`.
Only `pipeline.py`, `operators/`, `tests/`, `run.example.json`, and this README are
candidate-owned. `project.json`, `request.json`, `contract.lock.json`, and
`manifest.json` are controlled bindings or generated files.

## Input and output

- Row grain: One input record per id and text pair.
- Input: one Lance table declared in `pipeline.py` and bound by RunConfig.
- Output: one local Lance table under `datasets/`.
- Completion: Every selected input row is transformed and committed to the local output Lance table.

## Implementation

`operators/normalize_text.py` trims surrounding whitespace and applies the configured
case conversion. It does not read tables, call services, or perform writes. The tests
cover uppercase, case preservation, whitespace trimming, identity preservation, and
empty text. Regenerate `manifest.json` after source changes, then run `check` and
`verify`.

## Verification and execution

Review and Freeze must match the current source revision. `forge run` executes only
frozen code and only reads or writes Lance paths below this project's `datasets/`.
Model calls, network access, and external writes are outside contract v1.
