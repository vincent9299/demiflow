"""Declarative Case reads over a precisely labelled bounded source scope.

This is a trusted reader, not SQL or a configurable callback. A full-source
read must finish inside 1000 projected rows / 8 MiB, or refuse. Only an explicit
sample scope returns a bounded partial population. Larger sources never
acquire a fictitious whole-table result count.
Native decode stays in the existing workspace reader with its process limit.
"""
from __future__ import annotations

from functools import cmp_to_key
import hashlib
import math

from demiflow.workbench.analysis import contracts as c

PRIMITIVE = frozenset({"bool", "int64", "float64", "utf8"})


def read_private(source, object_root, query):
    """Fixed native worker only; caller has admitted its native read owner.

    The worker mounts one registered immutable object. Authenticate its sealed
    files before native decode and again after, with no latest-version fallback.
    The Web/controller never calls this function as a host-side fallback.
    """
    from pathlib import Path
    from demiflow.forge.io import no_symlinks
    from demiflow.workbench.analysis.arrow_bridge import schema_description
    from demiflow.workbench.analysis.object_files import verify_object
    from demiflow.workbench.analysis.case_index import NAME
    from demiflow.workbench.analysis.case_index import contains
    import lance
    checked=c.source_snapshot({key:value for key,value in source.items() if key!="digest"})
    if checked!=source or source["kind"]!="pinned_object" or source["retention"]!="pinned":
        c.reject("Private Case requires its registered immutable pinned source",code="conflict")
    specification(query,source)
    root=no_symlinks(Path(object_root))
    if not root.is_absolute() or root.parent.name!="objects":c.reject("Private Case root is outside the fixed object mount")
    verify_object(root.parent.parent,root.name,source["binding"])
    import os
    descriptor=os.open(root/"manifest.json",os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,"rb") as stream:raw=stream.read(65537)
    manifest=c.decode(raw)
    if (manifest["object_id"]!=root.name or manifest["retention"]!="pinned" or manifest["payload"]["format"]!="lance"
            or manifest["payload"]["schema"]!=source["schema"] or manifest["payload"]["rows"]!=source["rows"]
            or NAME not in manifest["files"]):
        c.reject("Private Case source differs from its native object",code="conflict")
    snapshot=lance.dataset(str(root/"data.lance"),version=1)
    if snapshot.count_rows()!=source["rows"] or schema_description(snapshot.schema)!=source["schema"]:
        c.reject("Private Case native metadata differs from its source receipt",code="conflict")
    result=read(snapshot,source,query)
    # The complete index hash authenticates every stable key, also on an empty
    # page. A returned key proves the scanner's Case identity convention.
    found=contains(root/"data.lance"/NAME,entry=manifest["files"][NAME],rows=source["rows"],
        schema_sha256=manifest["payload"]["schema_sha256"],row_id=result["rows"][0]["_rowid"] if result["rows"] else "0")
    if result["rows"] and not found:c.reject("Private Case row identity is outside its sealed index",code="conflict")
    verify_object(root.parent.parent,root.name,source["binding"])
    c.encoded(result,maximum=56*1024)
    return dict(result,kind="cases",source_snapshot=source["digest"])


def specification(value, source):
    c.fields(value, {"kind", "columns"}, {"filters", "grouping", "sort", "offset", "limit", "scope", "annotation_rows"}, label="Case read")
    if value["kind"] != "cases":
        c.reject("Expected a declarative Case read")
    schema = {item["name"]: item for item in source["schema"]}
    c.one_of(value.get("scope","full"),{"full","sample"},"Case query scope")
    columns = c.sequence(value["columns"], "Case read columns", 16, minimum=1)
    if any(type(name) is not str or name not in schema for name in columns) or len(set(columns)) != len(columns):
        c.reject("Case fields exceed the registered source projection", code="conflict")
    c.typed_filter(value.get("filters"), schema)
    grouping = c.sequence(value.get("grouping", []), "Case grouping", 4)
    sort = c.sequence(value.get("sort", []), "Case ordering", 4)
    required = set(columns)

    def predicate(item):
        if item is None:
            return
        if item["op"] in {"and", "or"}:
            for child in item["items"]:
                predicate(child)
        else:
            required.add(item["field"])
    predicate(value.get("filters"))
    for name in grouping:
        if type(name) is not str or name not in schema or schema[name]["type"] not in PRIMITIVE:
            c.reject("Grouping requires a registered primitive field")
        required.add(name)
    seen = set()
    for item in sort:
        c.fields(item, {"field", "direction"}, label="Case ordering")
        name = item["field"]
        if type(name) is not str or name not in schema or schema[name]["type"] not in PRIMITIVE or name in seen:
            c.reject("Ordering requires distinct registered primitive fields")
        c.one_of(item["direction"], {"asc", "desc"}, "Case order direction")
        required.add(name); seen.add(name)
    if len(set(grouping)) != len(grouping) or len(required) > 16:
        c.reject("Read/display/filter/group/order projection exceeds 16 fields", code="resource_limit")
    c.integer(value.get("offset", 0), "Case page offset", 1000)
    c.integer(value.get("limit", 20), "Case page size", 100, 1)
    if "annotation_rows" in value:
        from demiflow.workbench.analysis.case_review import rows_specification
        rows_specification(value["annotation_rows"])
    c.encoded(value)
    return [item["name"] for item in source["schema"] if item["name"] in required]


def matches(row, predicate):
    if predicate is None:
        return True
    op = predicate["op"]
    if op in {"and", "or"}:
        values = (matches(row, child) for child in predicate["items"])
        return all(values) if op == "and" else any(values)
    name=predicate["field"]
    if name not in row:c.reject("Typed Case reader omitted a projected field",code="operation_failed")
    value, expected = row[name], predicate["value"]
    if op == "is_null":
        return (value is None) == expected
    if value is None or type(value) is float and not math.isfinite(value):
        return False
    if op == "eq": return value == expected
    if op == "in": return value in expected
    if op == "contains": return expected in value
    return expected["lower"] <= value <= expected["upper"]


def read(snapshot, source, query):
    from demiflow.lance import preview
    columns = specification(query, source)
    scope=query.get("scope","full")
    annotation_rows=query.get('annotation_rows')
    selected=set(annotation_rows['keys']) if annotation_rows is not None else None
    if scope=="full" and source["rows"]>1000:
        c.reject("Full Case query exceeds this reader's 1000-row budget; explicitly choose sample scope or use the admitted native analysis path",code="resource_limit")
    scanner = None if not source["rows"] else snapshot.scanner(
        columns={name: preview._column(snapshot.schema, name) for name in columns},
        limit=min(source["rows"], 1000), batch_size=1, batch_size_bytes=65536,
        batch_readahead=1, fragment_readahead=1, io_buffer_size=1024*1024,
        with_row_id=True, scan_in_order=True, use_scalar_index=False)
    reader = iter(()) if scanner is None else scanner.to_batches()
    rows, scanned, logical, encoded, byte_limited = [], 0, 0, 0, False
    scope_hash=hashlib.sha256()
    try:
        for batch in reader:
            if batch.nbytes > 8*1024*1024 or batch.get_total_buffer_size() > 32*1024*1024:
                c.reject("Indivisible Case batch exceeds the reader policy", code="resource_limit")
            if logical + batch.nbytes > 8*1024*1024:
                byte_limited = True; break
            if scanned + batch.num_rows > 1000:
                c.reject("Case reader exceeded its admitted row count", code="resource_limit")
            # Each Python row has its own 48 KiB encoded bound. The cumulative
            # retention cap includes JSON expansion, not only Arrow buffers.
            converted = batch.to_pylist()
            for row in converted:
                if set(row)!={*columns,"_rowid"}:
                    c.reject("Typed Case reader projection differs",code="operation_failed")
                row["_rowid"] = str(row["_rowid"])
                # Evaluate predicates/order on original typed values. The
                # preview encoding of NaN/Infinity is a dict, not a number.
                display=c.encoded(preview._json_value(row),maximum=48*1024)
                charge = len(display)
                if encoded + charge > 8*1024*1024:
                    byte_limited = True; break
                scanned += 1; encoded += charge
                scope_hash.update(len(display).to_bytes(4,"big"));scope_hash.update(display)
                selected_row=(selected is None or ((row['_rowid'] in selected)==(annotation_rows['mode']=='include')))
                if selected_row and matches(row, query.get("filters")):
                    rows.append(row)
            if byte_limited:
                break
            logical += batch.nbytes
    finally:
        close = getattr(reader, "close", None)
        if close: close()
    if scope=="full" and (byte_limited or scanned!=source["rows"]):
        c.reject("Full Case query did not fit its byte/row budget; explicitly choose sample scope",code="resource_limit")

    def compare(left, right):
        for order in query.get("sort", []):
            name = order["field"]; one, two = left[name], right[name]
            # Null and preview-encoded nonfinite values sort last in either
            # direction. Row IDs are the deterministic tie break in this scope.
            a = one is None or type(one) is float and not math.isfinite(one)
            b = two is None or type(two) is float and not math.isfinite(two)
            if a != b: return 1 if a else -1
            if a: continue
            delta = (one > two) - (one < two)
            if delta: return delta if order["direction"] == "asc" else -delta
        return (int(left["_rowid"]) > int(right["_rowid"])) - (int(left["_rowid"]) < int(right["_rowid"]))
    if query.get("sort"): rows.sort(key=cmp_to_key(compare))
    grouping, groups = query.get("grouping", []), {}
    if grouping:
        for row in rows:
            values = preview._json_value({name: row[name] for name in grouping})
            key = c.encoded(values, maximum=1024).decode()
            groups[key] = groups.get(key, 0) + 1
    ranked = sorted(groups.items(), key=lambda item: (-item[1], item[0]))
    offset, limit = query.get("offset", 0), query.get("limit", 20)
    page = [preview._json_value({"_rowid": row["_rowid"], **{name: row[name] for name in query["columns"]}}) for row in rows[offset:offset+limit]]
    c.encoded(page, maximum=48*1024)
    return dict(rows=page, columns=query["columns"], offset=offset,
        **({"annotation_selection_digest":c.digest(annotation_rows)} if annotation_rows is not None else {}),
        next_offset=offset+len(page) if offset+len(page)<len(rows) else None,
        matching_in_scope=len(rows), groups=[dict(values=c.decode(key.encode()), count=count) for key,count in ranked[:32]],
        other_groups=max(0, len(ranked)-32), encoding="demiflow_preview_json_v1",
        scope_digest=scope_hash.hexdigest(),
        coverage=dict(kind="full_source" if scope=="full" else "first_projected_rows", requested_scope=scope, rows_scanned=scanned, total_rows=source["rows"],
            projected_columns=columns, complete=scanned==source["rows"], row_limit=1000,
            logical_byte_limit=8*1024*1024, logical_bytes_scanned=logical, encoded_bytes_retained=encoded,
            stopped_by="bytes" if byte_limited else "rows" if scanned<source["rows"] else None))
