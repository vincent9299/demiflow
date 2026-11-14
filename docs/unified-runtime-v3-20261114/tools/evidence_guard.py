"""Strict result evaluation for future implementation acceptance (stdlib only)."""
from pathlib import Path

def evaluate(report, *, required_nodeids, expected_tree, expected_digests):
    errors=[]; tree=Path(expected_tree).resolve()
    if report.get('exit_code') != 0: errors.append('nonzero pytest exit')
    if report.get('collection_errors'): errors.append('collection error')
    if report.get('deselected'): errors.append('deselected tests')
    collected=report.get('collected',[])
    if not collected or len(set(collected))!=len(collected):errors.append('empty/duplicate collection')
    if not set(required_nodeids).issubset(set(collected)):errors.append('required test not collected')
    if report.get('source_before')!=expected_digests or report.get('source_after')!=expected_digests:errors.append('source evidence mismatch/drift')
    paths=report.get('module_paths',{})
    if not paths or 'demiflow' not in paths:errors.append('missing in-process import assertion')
    for name,path in paths.items():
        if not Path(path).resolve().is_relative_to(tree):errors.append('wrong module tree: '+name)
    seen={};phases={}
    for item in report.get('results',[]):
        node=item.get('nodeid');phase=item.get('when');key=(node,phase)
        if node not in collected:errors.append('uncollected report')
        if key in seen:errors.append('duplicate/retry report: '+str(key))
        seen[key]=True
        if phase not in {'setup','call','teardown'}:errors.append('unknown phase')
        if item.get('outcome')!='passed' or item.get('wasxfail'):errors.append('nonpass/xfail: '+str(node))
        if item.get('rerun',0):errors.append('rerun: '+str(node))
        phases.setdefault(node,set()).add(phase)
    for node in collected:
        if phases.get(node)!= {'setup','call','teardown'}:errors.append('missing phase: '+node)
    return {'passed':not errors,'errors':errors,'collected':len(collected),'required':len(required_nodeids),'kind':'runtime_evidence_evaluation'}
