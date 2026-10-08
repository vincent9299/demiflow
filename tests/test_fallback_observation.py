import json
import sqlite3

from demiflow.collect.observation import receipt_observation


def test_primary_stop_and_secondary_success_remain_distinct(tmp_path):
    path=tmp_path/'receipts.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE native_search_fallback_results (identity TEXT, value TEXT, observed_at REAL)')
        for i,status in enumerate(('ok','no_results','search_failed','ok')):
            value={'primary':{'stop_reason':'search_recovery_probe_limit'},'result':{'status':status}}
            db.execute('INSERT INTO native_search_fallback_results VALUES (?,?,?)',(str(i),json.dumps(value),100+i))
    result=receipt_observation(path)
    outcomes={r['fallback_status']:r for r in result['search_fallback']}
    assert outcomes['ok']['queries']==2 and outcomes['ok']['last_selected_at']==103
    assert outcomes['search_failed']['queries']==1 and outcomes['no_results']['queries']==1
    assert all(r['primary_stop_reason']=='search_recovery_probe_limit' for r in outcomes.values())
    assert result['search_http']==[] and result['download_attempt_receipts']==0
