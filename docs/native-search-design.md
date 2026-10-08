# Native search integration — implementation baseline

The supplied working snapshot contains 257 engine modules and 344 named engine
configurations. `native-search-inventory.json` records every module, imports,
static capability declarations, unset configuration, setup/init functions, and
all named configurations **before implementation**. A file is not proof of live
availability. Seven `demi_*` modules already differ from the upstream offering;
the supplied tree has no independent Git metadata. We preserve its exact bytes
and SHA256 manifest, without claiming an unknown upstream commit.

Dependency boundary: adapters depend on engine traits/data, locale matching,
network response types, engine setup/init/cache, utilities (XPath/HTML/JSON),
result types, and five processor families (online, offline, dictionary,
currency, URL search). SQL/Mongo connectors additionally need their respective
client extras and endpoints; API-key engines need explicit secret references.
The runtime extra includes curl_cffi (including TLS impersonation), Babel,
Flask/Flask-Babel compatibility types/helpers, lxml, msgspec, dateutil, isodate,
markdown-it, Pygments, Typer and Valkey. No Flask app, request context, HTTP
listener, upstream search coordinator or per-query engine threads are run.

All adapter and supporting source/data files are bundled privately. Native
execution uses a fixed-size lazy pool of demiflow subprocesses with the current
Python executable, connected by private pipes. One synchronous adapter operation
occupies each worker; processes isolate upstream globals across sessions. Hard
deadlines and cancellation kill/reap the affected worker process group. This is
an execution worker, not a hidden SearXNG service: no listening port, source path,
separate environment, service manager, browser preferences or webapp is involved.

The parent owns admission, actual HTTP/host pacing, retries, suspension, query
cache and receipts. Every child HTTP hop asks the parent for admission; child
network retries are disabled, redirects are admitted individually and response
bytes are capped. Engine setup/init use the same path. Workers reuse connections
and adapter state. Upstream result merging/scoring runs in a worker with source
order fixed by the request, preserving all result families and provenance.

Result cache identity includes runtime/baseline/config/custom-adapter identity,
all semantic parameters, limits, and a credential identity derived with a
private cache salt. In-flight requests are coalesced with reference-counted
cancellation. SQLite claims prevent two sessions using the same cache from
reissuing a reserved attempt. Interrupted claims are evidence of uncertainty,
not an invitation to repeat a possibly billed request. Failures remain failures
on replay; a deliberate new cache namespace is required to authorize a new budget.

The Dataset row remains the stream unit. Row queries and sources are consumed
by bounded workers, without a task for every query. Business relevance, fact
judgments and visual tasks remain outside demiflow. Explicit request language is
required when no session language is declared; no character-based inference.

Deployment remains isolated until acceptance and coordination with the current
business window. Historical services and evidence are retained. No public data
or business prompts/policies are modified.
