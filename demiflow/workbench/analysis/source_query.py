"""Trusted fixed-source JSON samples/profiles in the existing workspace worker.

No user code, SQL, path or decoder is accepted. Rich nested fields keep the
existing preview JSON encoding. Samples refuse a wide indivisible row; profiles
report precisely how many projected rows/bytes were actually scanned. Native
allocation remains in the same bounded reader process, never the Web thread.
"""
from __future__ import annotations

import math

from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.source_export import fixed_input


def request(value, source):
    if type(value) is dict and value.get("kind")=="cases":
        from demiflow.workbench.analysis.case_query import specification
        specification(value, source)
        return value
    c.fields(value, {"kind", "columns"}, {"offset", "limit"}, label="fixed source read")
    kind = c.one_of(value["kind"], {"sample", "profile"}, "fixed source read kind")
    columns = c.sequence(value["columns"], "explicit read projection", 16, minimum=1)
    available = {field["name"] for field in source["schema"]}
    for name in columns:
        c.text(name, "source field", 256)
        if name not in available:
            c.reject("Read projection exceeds its registered fixed source", code="conflict")
    if len(set(columns)) != len(columns):
        c.reject("Source projection has repeated columns")
    if kind == "profile":
        if set(value) != {"kind", "columns"}:
            c.reject("Profile always uses its explicit first1000-row/8MiB sampling policy")
    else:
        c.integer(value.get("offset", 0), "sample offset", source["rows"])
        c.integer(value.get("limit", 20), "sample row limit", 100, 1)
    c.encoded(value)
    return value


def _profile(snapshot, source, columns):
    from demiflow.lance import preview
    scanner = None if source["rows"]==0 else snapshot.scanner(columns={name:preview._column(snapshot.schema,name) for name in columns},
        limit=min(source["rows"],1000),batch_size=1,batch_size_bytes=65536,
        batch_readahead=1,fragment_readahead=1,io_buffer_size=1024*1024,
        scan_in_order=True,use_scalar_index=False)
    reader = iter(()) if scanner is None else scanner.to_batches()
    stats = {name:dict(null=0,finite=0,nonfinite=0,complex=0,long_values=0,counts={},minimum=None,maximum=None) for name in columns}
    rows = logical = buffers = 0
    byte_limited = False
    try:
        for batch in reader:
            if batch.nbytes > 8*1024*1024 or batch.get_total_buffer_size() > 32*1024*1024:
                c.reject("An indivisible profile batch exceeds its native buffer policy",code="resource_limit")
            if logical+batch.nbytes > 8*1024*1024:
                byte_limited=True;break
            if rows+batch.num_rows > 1000:
                c.reject("Profile reader exceeded its admitted row count",code="resource_limit")
            logical+=batch.nbytes;buffers=max(buffers,batch.get_total_buffer_size())
            for row in batch.to_pylist():
                rows+=1
                for name,value in row.items():
                    item=stats[name]
                    if value is None:item["null"]+=1;continue
                    if type(value) is float and not math.isfinite(value):item["nonfinite"]+=1;continue
                    if type(value) is float or type(value) is int and -2**63<=value<2**63:
                        item["finite"]+=1
                        item["minimum"]=value if item["minimum"] is None else min(item["minimum"],value)
                        item["maximum"]=value if item["maximum"] is None else max(item["maximum"],value)
                    if type(value) not in {str,bool,int,float}:
                        item["complex"]+=1;continue
                    # Only short primitive labels are retained. At most1000
                    # labels/field ×16 fields ×128B plus bounded Python maps.
                    try:raw=c.encoded(value,maximum=128)
                    except ForgeError:
                        item["long_values"]+=1;continue
                    key=raw.decode();item["counts"][key]=item["counts"].get(key,0)+1
    finally:
        close=getattr(reader,"close",None)
        if close:close()
    fields=[]
    for name,item in stats.items():
        top=sorted(item["counts"].items(),key=lambda pair:(-pair[1],pair[0]))[:20]
        shown=sum(count for _,count in top)
        fields.append(dict(name=name,null_count=item["null"],nonfinite_count=item["nonfinite"],complex_count=item["complex"],
            long_or_unrepresentable_count=item["long_values"],minimum=item["minimum"],maximum=item["maximum"],
            finite_json_numeric_count=item["finite"],
            distinct_short_primitive_values=len(item["counts"]),enumeration=[dict(value=c.decode(key.encode()),count=count) for key,count in top],
            other_short_primitive_count=sum(item["counts"].values())-shown))
    return dict(fields=fields,coverage=dict(kind="first_projected_rows",rows_scanned=rows,total_rows=source["rows"],
        projected_columns=columns,logical_bytes_scanned=logical,maximum_native_batch_buffers=buffers,
        complete=rows==source["rows"],row_limit=1000,logical_byte_limit=8*1024*1024,
        stopped_by="bytes" if byte_limited else "rows" if rows<source["rows"] else None))


def read(root, *, source_locator, source_snapshot, query):
    c.sha(source_snapshot)
    uri,version,source=fixed_input(root,source_locator)
    if source["digest"]!=source_snapshot:c.reject("Fixed source changed before preview",code="conflict")
    query=request(query,source)
    from demiflow.lance import preview
    snapshot=preview._open(uri,version)
    columns=query["columns"]
    if query["kind"]=="profile":
        value=_profile(snapshot,source,columns)
    elif query["kind"]=="cases":
        from demiflow.workbench.analysis.case_query import read as case_read
        value=case_read(snapshot,source,query)
    else:
        offset,limit=query.get("offset",0),query.get("limit",20)
        rows=preview._rows(snapshot,columns,offset=offset,limit=limit,maximum=48*1024)
        value=dict(rows=rows,columns=columns,offset=offset,next_offset=offset+len(rows) if offset+len(rows)<source["rows"] else None,
            total_rows=source["rows"],encoding="demiflow_preview_json_v1",complete=offset==0 and len(rows)==source["rows"])
    _,_,after=fixed_input(root,source_locator)
    if after!=source:c.reject("Fixed source changed while preview was read",code="conflict")
    result=dict(source_snapshot=source_snapshot,kind=query["kind"],**value)
    c.encoded(result,maximum=56*1024)
    return result
