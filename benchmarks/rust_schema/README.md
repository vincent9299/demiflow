# Schema CPU experiment

This is an opt-in benchmark prototype, not a demiflow runtime dependency.
The package manifest and public APIs do not load this extension. The authoritative
validator remains `demiflow.schema.validate_instance`.

The Rust predicate reads Python objects directly through PyO3. It compiles a
restricted schema into an owned tree (12 levels / 512 nodes), checks at most
65,536 value visits, and defers unsupported or rejected instances to Python.
The benchmark wrapper bounds schema size before copying, and verifies the
current schema against its snapshot on every call. The latter is necessary
because existing schema dictionaries can change; identity-based caching is unsafe.
It intentionally remains in the measured cost. No unchecked FFI or `unsafe` is
used in this crate.

The Python controls are in `../schema_probe.py`: the original validator, an AST
control that only evaluates the selected type check, and a compiled Python
predicate with the same snapshot protection. No copy of these controls is added
to production code. The complete error path always uses the original validator.

## Reproduce without installing into a shared environment

Use an isolated Rust toolchain/cache and extension directory. In this experiment,
Rust 1.90.0 and PyO3 0.27.2 were used. The PoC targets CPython 3.11+ via `abi3`;
it is **not** distribution support for all demiflow Python/platform versions.

```bash
export DF_EXPERIMENT=/tmp/demiflow-cpu-rust-20261101
export CARGO_HOME="$DF_EXPERIMENT/cargo"
export RUSTUP_HOME="$DF_EXPERIMENT/rustup"
export CARGO_TARGET_DIR="$DF_EXPERIMENT/target"
export PYO3_PYTHON=/yzp/zhaozy/yangzepeng/0905/env/bin/python
"$CARGO_HOME/bin/cargo" build --release --locked --jobs 2 --manifest-path benchmarks/rust_schema/Cargo.toml
```

Copy `lib_demiflow_schema_probe.so` from the release directory into a fresh
experiment-only import directory as `_demiflow_schema_probe.abi3.so`, and add that
directory and `benchmarks/` to `PYTHONPATH`. Never overwrite an extension file
mapped by a running process. Run `benchmarks/test_schema_probe.py` explicitly
with pytest before experiments. It skips when the optional extension is absent.

Entry points (all arguments are available with `--help`):

* `platform_cpu_profile.py`: owned HTTP / runtime / Dataset / journal profile.
* `deployment_context_experiment.py`: real configuration parsing with an inert
  lease, comparing the saved pre-change runtime to declaration reuse.
* `schema_cpu_experiment.py`: real read-only journal replay and microbenchmarks.
* `schema_gpu_experiment.py`: unprofiled alternating A/B in one owned model,
  followed by journal write/replay and the public Dataset path.

Use explicit empty output directories and existing model environments. GPU tests
check that GPU 0 or 1 is free and retain the platform's ownership locks. They use
two inference lanes, no batching, bounded graph memory and bounded input sizes.
The GPU A/B caps captures at 32, finishes capture warmup, then asserts no further
captures during timed phases; this experimental control does not change platform
defaults. External training/services continue running, so report variability and
repeat CPU evidence rather than treating small differences as guaranteed gains.

Design and final interpretation: [CPU/Rust design](../../docs/platform-cpu-rust-design-20261101.md)
and [review](../../docs/platform-cpu-rust-review-20261101.md).
PyO3's [build documentation](https://pyo3.rs/v0.27.2/building-and-distribution.html)
describes the isolated interpreter selection and `abi3` build used here.
