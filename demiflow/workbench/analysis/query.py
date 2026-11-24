"""Fixed native query entrypoint. It is never a Web-process SQL executor."""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

from pathlib import Path
import re

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import schema_description
from demiflow.workbench.analysis.arrow_bridge import schema_digest
from demiflow.workbench.analysis.result_transfer import send_result
from demiflow.workbench.analysis.sandbox import require_job
from demiflow.workbench.analysis.resource_profile import profile

NATIVE_OPTIONS = profile()[0]["native_options"]
NATIVE_CAPACITY = profile()[0]["native_session"]["slot_capacity"]
_OBJECT = re.compile(r"o[0-9a-f]{32}\Z")


def query_config(value):
    c.fields(value, {"operation", "sql", "sources", "publish_id"}, label="native query configuration")
    if value["operation"] != "query":
        c.reject("Unknown native analysis operation")
    c.text(value["sql"], "analysis SQL", 16384)
    c.identity(value["publish_id"], "owned publish ID")
    sources = c.sequence(value["sources"], "native sources", 16)
    aliases = set()
    result = {}
    for source in sources:
        c.fields(source, {"alias", "object_id"}, label="native source")
        alias = c.sql_alias(source["alias"])
        if alias in aliases or type(source["object_id"]) is not str or not _OBJECT.fullmatch(source["object_id"]):
            c.reject("Duplicate alias or invalid private object ID")
        aliases.add(alias)
        # Sources are helper-owned immutable private objects only. No URI,
        # version override, provider, views, callback or host path is admitted.
        result[alias] = dict(uri=job_path("/sources/" + source["object_id"] + "/data.lance"), version=1)
    return result


def run_query(config):
    sources = query_config(config)
    # Imports remain after the role/sandbox checks performed by main().
    from demiflow.execution.hosts.datafusion import DataFusionOptions
    from demiflow.execution.hosts.datafusion import DataFusionSession
    import lance
    with DataFusionSession(resource_directory=job_path("/var/lib/demiflow-analysis/native-slots"),
            options=DataFusionOptions(**NATIVE_OPTIONS), max_concurrent=1,
            slot_capacity=NATIVE_CAPACITY, temp_directory=job_path("/scratch/native"),
            diagnostics_directory=job_path("/scratch/diagnostics"), managed_diagnostics=True) as session:
        result = session.query(config["sql"], sources=sources, label="analysis")
        output = Path(result.uri)
        if not output.is_relative_to(Path(job_path("/scratch/native"))) or result.version != 1:
            c.reject("Native query returned an unexpected result location/version", code="operation_failed")
        snapshot = lance.dataset(result.uri, version=1)
        schema = schema_description(snapshot.schema)
        rows = snapshot.count_rows()
        if rows != result.row_count or rows > 100000:
            c.reject("Native query result row quota exceeded or receipt differs", code="resource_limit")
        logical_bytes = observed_rows = 0
        batches = snapshot.scanner(batch_size=1024, batch_size_bytes=1024*1024,
            batch_readahead=1, fragment_readahead=1, io_buffer_size=1024*1024,
            scan_in_order=True).to_batches()
        try:
            for batch in batches:
                if logical_bytes + batch.nbytes > 32*1024*1024 or batch.get_total_buffer_size() > 8*1024*1024:
                    c.reject("Native result logical/decoded buffer budget exceeded", code="resource_limit")
                logical_bytes += batch.nbytes; observed_rows += batch.num_rows
        finally:
            batches.close()
        if observed_rows != rows:
            c.reject("Native result scan differs from its row receipt", code="operation_failed")
        description = dict(format="lance", version=1, rows=rows, schema=schema,
                           schema_sha256=schema_digest(snapshot.schema), logical_bytes=logical_bytes, complete=True)
        from demiflow.workbench.analysis.case_index import build_index
        build_index(snapshot, output)
        # Transfer inside the session lifetime; DataFusion's private result is
        # intentionally not retained by holding its temporary filesystem URI.
        return send_result(output, publish_id=config["publish_id"], description=description)


def main():
    job = require_job("native")
    query_config(job["config"])
    Path(job_path("/scratch/native")).mkdir(mode=0o700)
    Path(job_path("/scratch/diagnostics")).mkdir(mode=0o700)
    run_query(job["config"])


if __name__ == "__main__":
    main()
