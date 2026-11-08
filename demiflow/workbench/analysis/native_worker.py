"""The fixed native analysis job dispatcher; no arbitrary commands or paths."""
from pathlib import Path

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.job_context import require_worker as require_job
from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current


def main():
    job = require_job("native")
    operation = job["config"].get("operation")
    c.one_of(operation, {"query", "materialize", "read"}, "native worker operation")
    Path(job_path("/scratch/native")).mkdir(mode=0o700)
    Path(job_path("/scratch/diagnostics")).mkdir(mode=0o700)
    if operation == "query":
        from demiflow.workbench.analysis.query import run_query
        run_query(job["config"])
    elif operation == "materialize":
        from demiflow.workbench.analysis.materialize import run_materialize
        run_materialize(job["config"])
    else:
        from demiflow.workbench.analysis.read_worker import run_read
        run_read(job["config"])


if __name__ == "__main__":
    main()
