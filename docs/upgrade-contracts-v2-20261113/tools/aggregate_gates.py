"""Check gate dependency closure at one revision; never certify product completion."""
import argparse
import hashlib
import json
from pathlib import Path


def aggregate(gates, reports, *, required, config_sha256):
    reasons=[]; needed=set()
    def visit(name, ancestors):
        if name not in gates or name in ancestors:
            reasons.append('invalid dependency: '+name);return
        if name in needed:return
        needed.add(name)
        for dep in gates[name]['depends_on']:visit(dep,ancestors|{name})
    for name in required:visit(name,set())
    by={}
    for report in reports:
        name=report.get('gate')
        if name in by:reasons.append('duplicate gate evidence: '+str(name))
        by[name]=report
    revisions=set()
    for name in sorted(needed):
        report=by.get(name)
        if report is None:reasons.append('missing gate: '+name);continue
        gate=gates[name]
        revision=report.get('revision')
        if not isinstance(revision,str) or len(revision)!=40 or any(c not in '0123456789abcdef' for c in revision):
            reasons.append('invalid source revision: '+name)
        else:revisions.add(revision)
        if report.get('status')!='passed' or report.get('git_status')!='' or report.get('frozen_mismatches')!=[] or report.get('symbol_scope_mismatches')!=[]:
            reasons.append('gate not accepted on clean preserved source: '+name)
        if report.get('gate_config_sha256')!=config_sha256:
            reasons.append('different gate configuration: '+name)
        if report.get('depends_on')!=gate['depends_on']:
            reasons.append('dependency definition mismatch: '+name)
        rounds=report.get('rounds',[])
        if len(rounds)!=gate['repeat'] or any(r.get('status')!='passed' for r in rounds):
            reasons.append('missing successful repetitions: '+name)
    if len(revisions)!=1:reasons.append('gate evidence must share one source revision')
    return {'status':'passed' if not reasons else 'not_accepted',
            'scope':'test gates and dependencies only; not full product/engine acceptance',
            'required_gates':sorted(needed),'revision':next(iter(revisions)) if len(revisions)==1 else None,
            'reasons':reasons,'product_accepted':False}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('reports',nargs='+',type=Path)
    parser.add_argument('--gate',action='append',help='default: every configured gate')
    args=parser.parse_args()
    raw=(Path(__file__).resolve().parents[1]/'gates.json').read_bytes()
    gates=json.loads(raw)
    result=aggregate(gates,[json.loads(p.read_text()) for p in args.reports],
                     required=args.gate or sorted(gates),config_sha256=hashlib.sha256(raw).hexdigest())
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':raise SystemExit(main())
