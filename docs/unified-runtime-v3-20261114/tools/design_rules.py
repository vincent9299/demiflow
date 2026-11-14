"""Pure checks used both by the package checker and adversarial tests."""
def manifest_errors(tasks):
    errors=[]; byid={t['id']:t for t in tasks}; owners={};symbols=set()
    if len(byid)!=len(tasks):errors.append('duplicate_id')
    for t in tasks:
        key=(t['file'],t['symbol'])
        if key in symbols:errors.append('duplicate_symbol')
        symbols.add(key);owners.setdefault(t['file'],set()).add(t['lane'])
        if not t.get('behavior') or not t.get('counterexample'):errors.append('empty_contract')
        if t['mode']=='add' and not t.get('callers') and not t.get('entrypoint'):errors.append('orphan_add')
        if t['mode'] in {'modify','retire'} and not t.get('baseline'):errors.append('unfounded_modification')
        if not t.get('test_nodeid'):errors.append('missing_acceptance')
        for c in t.get('calls',[]):
            if c not in byid:errors.append('missing_callee');continue
            if t['id'] not in byid[c].get('callers',[]):errors.append('asymmetric_edge')
        for c in t.get('callers',[]):
            if c not in byid:errors.append('missing_caller');continue
            if byid[c]['mode'] in {'retire','verify_preserve'}:errors.append('unauthorized_caller')
            if t['id'] not in byid[c].get('calls',[]):errors.append('asymmetric_edge')
    if any(len(x)!=1 for x in owners.values()):errors.append('multiple_file_owners')
    return errors
