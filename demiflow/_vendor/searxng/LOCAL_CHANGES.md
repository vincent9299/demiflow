# Native integration changes

The original supplied snapshot is retained in baseline.tar.gz and BASELINE.json.
All 257 original adapter modules and all supporting source/data are retained.
Existing local demi_* adapters are part of that snapshot, not attributed as
unaltered upstream engines.

- searx/search/__init__.py: replaced Flask request-context/per-query-thread
  orchestration with a package marker. All five processor families and search
  models remain byte-for-byte available.
- searx/version_frozen.py: added deterministic content-based snapshot identity;
  do not read the installing application's Git checkout for SearXNG version.
- demiflow.collect.native_search: new bounded worker execution, scope isolation,
  durable receipts, admissions, retries, cancellation, secret references and
  runtime identity. Worker bootstrap replaces the adapter-token cache with
  bounded worker-private memory. Derived lookup tables retain the complete
  SQLite cache ABI in parent-owned private temporary directories, cleaned on
  cancellation/close. No cross-pipeline SearXNG cache is opened.
- worker_network.py: replaces Network.call_client's retries and instruments
  AsyncClient.request with parent admission, explicit redirect hops and response
  limits. Original transport client/TLS impersonation ABI remains in use.
  multi_requests retains ordered response/exception results under serial worker
  admission; origin credentials are removed on cross-origin redirects. Partial
  HTTP receipts survive timeout/cancellation without authorizing another attempt.
- demiflow.services.engines.wikisearch: preserves the existing AGPL keyword
  adapter; requires explicit language and resolves requested language sites.
- searx/engines/brave.py (2026-10-03): data extraction delegates to the
  runtime-fingerprinted `native_search.brave_data` literal parser. Handles
  Svelte scalar-substitution functions and multiple script blocks without
  executing page JavaScript; bounded bytes, depth, nodes and arguments. Original
  `properties.url` and separate thumbnails retain their adapter meaning.

Rebase workflow: acquire an identified upstream source revision; regenerate the
static inventory and exact source manifest; review all upstream changes against
this snapshot (including pre-existing local changes), replay the small native
patch, run ABI/import checks for every source plus fixed-response, runtime,
Dataset and wheel tests. Update runtime version and capability/live-validation
records. Do not edit adapter files merely to make the inventory show success.
