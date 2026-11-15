"""Small read projections of native run facts; no reconciliation on GET."""

from contextlib import closing
import base64
import json
import sqlite3
import time

from demiflow.workbench.runs import manager as runs
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.execution import run_telemetry


class ObservationResetRequired(ForgeError):
    def __init__(self, reason):
        super().__init__('Observation page must be reset: '+reason,code='conflict')
        self.reset_required=True
        self.reason=reason


def _observation_revision(db,attempt):
    """Hash native identity/sequence rows in the caller's read transaction.

    reserve_nodes adds sequence=0 membership. observe_nodes only updates when
    sequence increases, and terminal records cannot change. Hashing membership
    as well as every sequence detects additions, updates, removals and restored
    older metadata; it does not rely on a control-event or execution revision.
    No new journal writer or telemetry producer is introduced.
    """
    import hashlib
    from demiflow.execution.run_journal import OBSERVATION_COUNT
    from demiflow.execution.run_journal import _node_key
    digest=hashlib.sha256();count=0;total=0;sequence_total=0
    for row in db.execute('SELECT action_id,node_id,occurrence,sequence,terminal FROM node_observations WHERE attempt=? ORDER BY action_id,node_id,occurrence',(attempt,)):
        count+=1
        if count>OBSERVATION_COUNT:
            raise ForgeError('Observation metadata count exceeds native budget',code='resource_limit')
        _node_key(*row[:3])
        if type(row[3]) is not int or not 0<=row[3]<2**63 or row[4] not in (0,1):
            raise ForgeError('Invalid native observation version')
        sequence_total+=row[3]
        raw=bounded_json(list(row),1024);total+=len(raw)
        if total>1024**2:
            raise ForgeError('Observation version scan exceeds 1MiB',code='resource_limit')
        digest.update(raw);digest.update(b'\n')
    return 'o'+digest.hexdigest(),count,sequence_total


def observations(root, run_id, *, attempt, node_id=None, action_id=None, cursor=None, limit=32):
    """One 96KiB occurrence page, pinned to an explicit observation revision."""
    from demiflow.execution.run_journal import OBSERVATION_COUNT
    from demiflow.execution.run_journal import _node_document
    if type(limit) is not int or not 1<=limit<=64 or type(attempt) is not int:
        raise ForgeError('Invalid observation page',code='invalid_request')
    for value in (node_id,action_id):
        if value is not None and (type(value) is not str or not 1<=len(value)<=64 or any(ord(c)<32 for c in value)):
            raise ForgeError('Invalid observation selector',code='invalid_request')
    decoded=None;after=None
    if cursor:
        if type(cursor) is not str or len(cursor)>2048:
            raise ForgeError('Observation cursor exceeds budget',code='invalid_request')
        try:
            decoded=json.loads(base64.b64decode(cursor,altchars=b'-_',validate=True))
            if type(decoded) is not dict or set(decoded)!={'v','binding','observation_revision','after'} or decoded['v']!=1:
                raise ValueError('Invalid cursor format')
            after=decoded['after']
            if (type(after) is not list or len(after)!=3 or type(after[0]) is not str
                    or type(after[1]) is not str or type(after[2]) is not int
                    or not 1<=len(after[0])<=64 or not 1<=len(after[1])<=64 or not 0<=after[2]<OBSERVATION_COUNT):
                raise ValueError('Invalid position')
        except (ValueError,KeyError,TypeError,RecursionError) as exc:
            raise ObservationResetRequired('cursor_invalid') from exc
    try:
        target,connection=_connect(root,run_id)
    except (OSError,ForgeError) as exc:
        if cursor:raise ObservationResetRequired('history_unavailable') from exc
        raise
    with closing(connection) as db:
        db.execute('BEGIN')
        size=db.execute('SELECT length(CAST(document AS BLOB)) FROM manifest').fetchone()
        if not size or size[0]>8*1024**2:
            raise ForgeError('Run manifest exceeds native budget',code='resource_limit')
        identity=db.execute("SELECT json_extract(document,'$.root'),json_extract(document,'$.run_id'),json_extract(document,'$.digest') FROM manifest").fetchone()
        if not identity or identity[:2]!=(str(runs.Path(root).absolute()),run_id) or type(identity[2]) is not str or len(identity[2])!=64:
            if cursor:raise ObservationResetRequired('sealed_identity_changed')
            raise ForgeError('Run observation seal belongs to a different project',code='conflict')
        control=runs.ws.read_control(runs.Path(root)/'workspace.json')
        binding=dict(workspace_id=runs.ws.workspace_id(root,control),project_root_digest=content_digest(str(runs.Path(root).absolute())),
                     run_id=run_id,attempt=attempt,seal_digest=identity[2],node_id=node_id,action_id=action_id)
        if decoded is not None and decoded['binding']!=binding:
            raise ObservationResetRequired('cursor_binding_changed')
        columns={row[1] for row in db.execute('PRAGMA table_info(node_observations)')}
        if 'attempt' not in columns:
            if cursor:raise ObservationResetRequired('historical_observation_contract')
            return dict(run_id=run_id,attempt=attempt,observations=[],availability='unsupported',
                        reason='historical_observation_contract',scope='correlated_node_observations',has_more=False)
        try:_state(db,attempt)
        except ForgeError as exc:
            if cursor:raise ObservationResetRequired('attempt_history_unavailable') from exc
            raise
        revision,count,_=_observation_revision(db,attempt)
        if decoded is not None and decoded['observation_revision']!=revision:
            raise ObservationResetRequired('observations_changed')
        from demiflow.workbench.runs import facts
        plan_scope_cache={}
        def plan_scope(action):
            """U2/W012: v3 sealed plan identity only; legacy/unreadable -> None (never backfilled)."""
            if action not in plan_scope_cache:
                sealed=facts.read_sealed_plan(target,action)
                plan_scope_cache[action]=(dict(plan_id=sealed['plan_id'],dag_id=sealed['dag_id'],
                                               plan_revision=sealed['plan_view']['plan_revision'])
                                          if sealed.get('available') and sealed.get('version')=='v3' else None)
            return plan_scope_cache[action]
        if action_id is not None:
            page_scope=plan_scope(action_id)
        else:
            # No queried action: one page scope only if the whole filtered observation
            # set belongs to a single action (bounded probe, never a guessed identity).
            scope_clauses=['attempt=?'];scope_values=[attempt]
            if node_id is not None:scope_clauses.append('node_id=?');scope_values.append(node_id)
            distinct=db.execute('SELECT DISTINCT action_id FROM node_observations WHERE '+
                                ' AND '.join(scope_clauses)+' LIMIT 2',scope_values).fetchall()
            page_scope=plan_scope(distinct[0][0]) if len(distinct)==1 else None
        result=dict(format='demiflow_forge_node_observation_page_v1',run_id=run_id,attempt=attempt,
                    node_id=node_id,action_id=action_id,binding=binding,observation_revision=revision,
                    observations=[],availability='available',scope='correlated_node_observations',
                    cursor=None,has_more=False,coverage='one_page_at_observation_revision',attempt_observation_count=count)
        if page_scope is not None:
            result.update(page_scope)
        clauses=['attempt=?'];values=[attempt]
        for key,value in [('node_id',node_id),('action_id',action_id)]:
            if value is not None:clauses.append(key+'=?');values.append(value)
        if after is not None:
            clauses.append('(action_id,node_id,occurrence)>(?,?,?)');values.extend(after)
        sql=('SELECT action_id,node_id,occurrence,sequence,length(CAST(document AS BLOB)),'
             'CASE WHEN length(CAST(document AS BLOB))<=8192 THEN document END FROM node_observations WHERE '+
             ' AND '.join(clauses)+' ORDER BY action_id,node_id,occurrence LIMIT ?')
        values.append(limit+1);rows=result['observations'];used=len(bounded_json(result,96*1024))+2064;last=after
        for action,node,occurrence,sequence,length,raw in db.execute(sql,values):
            if raw is None or length>8192:
                raise ForgeError('Observation exceeds native record budget',code='resource_limit')
            document=json.loads(raw);_node_document(document)
            if document['sequence']!=sequence:
                raise ForgeError('Observation document differs from its native sequence')
            row=dict(document,action_id=action,node_id=node,occurrence=occurrence,sequence=sequence)
            row_scope=plan_scope(action)
            if row_scope is not None:
                row.update(row_scope)
            size=len(bounded_json(row,16384))+1
            if len(rows)==limit or used+size>96*1024:
                result['has_more']=True;break
            rows.append(row);used+=size;last=[action,node,occurrence]
        if last:
            encoded=bounded_json(dict(v=1,binding=binding,observation_revision=revision,after=last),1536)
            result['cursor']=base64.urlsafe_b64encode(encoded).decode()
            if len(result['cursor'])>2048:
                raise ForgeError('Observation cursor exceeds encoding budget',code='resource_limit')
        bounded_json(result,96*1024)
        return result


def _connect(root, run_id):
    target = runs.directory(root, run_id)
    path = target / "run.sqlite"
    runs.ws.no_symlinks(path)
    if not path.is_file() or path.stat().st_size > 64 * 1024**2:
        raise ForgeError("Run journal unavailable or exceeds budget")
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    db.execute("PRAGMA cache_size=-2048")
    return target, db


def context_facts(root, run_id, *, attempt=None, deadline=None):
    """A <=4 KiB scalar projection, including historical attempts, without writes.

SQLite parses at most the existing 8 MiB manifest/64 KiB state limits. Only
bounded scalar projections cross into Python. No index refresh, reconciliation,
event replay or producer startup occurs. This is an observation, not admission.
    """
    import re
    if attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
        raise ForgeError('Invalid attempt', code='invalid_request')
    _, connection = _connect(root, run_id)
    with closing(connection) as db:
        if deadline is not None:
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        db.execute('BEGIN')
        row = db.execute("SELECT length(CAST(document AS BLOB)) FROM manifest LIMIT 1").fetchone()
        if not row or row[0] > 8 * 1024**2:
            raise ForgeError('Run manifest exceeds native budget', code='resource_limit')
        identity = db.execute("SELECT json_extract(document,'$.root')=?, json_extract(document,'$.run_id')=?, "
                              "substr(CAST(json_extract(document,'$.digest') AS BLOB),1,65), "
                              "substr(CAST(json_extract(document,'$.format') AS BLOB),1,65) FROM manifest LIMIT 1",
                              (str(runs.Path(root).absolute()), run_id)).fetchone()
        if (not identity or identity[:2] != (1, 1) or not isinstance(identity[2], bytes)
                or not re.fullmatch(rb'[0-9a-f]{64}', identity[2])):
            raise ForgeError('Run context identity differs', code='conflict')
        fields = ('attempt', 'revision', 'status', 'execution_stopped', 'cleanup', 'usage_complete', 'launch_phase', 'stop_phase')
        projection = "json_object(" + ','.join("'" + key + "',json_extract(document,'$." + key + "')" for key in fields) + ')'

        def state(table, where='', parameters=()):
            # Table names and SQL come only from these two fixed branches.
            row = db.execute('SELECT length(CAST(document AS BLOB)), substr(CAST(' + projection +
                             ' AS BLOB),1,4097) FROM ' + table + where + ' LIMIT 1', parameters).fetchone()
            if not row:
                raise ForgeError('Unknown attempt', code='not_found')
            if row[0] > 65536 or len(row[1]) > 4096:
                raise ForgeError('Run context state exceeds budget', code='resource_limit')
            return json.loads(row[1])

        current = state('execution_state')
        selected = current if attempt is None or attempt == current['attempt'] else state('attempts', ' WHERE attempt=?', (attempt,))
        result = dict(run_id=run_id, attempt=selected['attempt'], current_attempt=current['attempt'],
                      historical=selected['attempt'] != current['attempt'], seal_digest=identity[2].decode(),
                      source_format=identity[3].decode(), state=selected)
        bounded_json(result, 4096)
        return result


def _state(db, attempt):
    if attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
        raise ForgeError("Invalid attempt", code="invalid_request")
    raw = db.execute("SELECT document FROM execution_state").fetchone()
    if not raw or len(raw[0].encode()) > 65536:
        raise ForgeError("Invalid run state")
    current = json.loads(raw[0])
    if attempt is not None and attempt != current.get("attempt"):
        raw = db.execute(
            "SELECT document FROM attempts WHERE attempt=?", (attempt,)
        ).fetchone()
        if not raw or len(raw[0].encode()) > 65536:
            raise ForgeError("Unknown attempt", code="not_found")
        return json.loads(raw[0]), current.get("attempt")
    return current, current.get("attempt")


def control_state(root, run_id):
    target, connection = _connect(root, run_id)
    with closing(connection) as db:
        db.execute("BEGIN")
        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='runtime_summary'"
        ).fetchone():
            return runs._load(root, run_id, detail="identity")[0:2]
        raw = db.execute("SELECT document FROM runtime_summary").fetchone()[0]
        if len(raw) > 65536:
            raise ForgeError("Runtime projection exceeds budget")
        summary = json.loads(raw)
        if (
            summary["root"] != str(runs.Path(root).absolute())
            or summary["run_id"] != run_id
        ):
            raise ForgeError("Run projection identity differs")
        state, _ = _state(db, None)
        return target, {"state": state, "managed": True}


def snapshot(root, run_id, *, attempt=None, revision=None):
    target, connection = _connect(root, run_id)
    with closing(connection) as db:
        db.execute("BEGIN")
        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='runtime_summary'"
        ).fetchone():
            return dict(
                run_id=run_id,
                availability="unsupported",
                reason="historical_runtime_contract",
            )
        raw = db.execute("SELECT document FROM runtime_summary").fetchone()[0]
        if len(raw.encode()) > 65536:
            raise ForgeError("Runtime projection exceeds budget")
        summary = json.loads(raw)
        if (
            summary["root"] != str(runs.Path(root).absolute())
            or summary["run_id"] != run_id
        ):
            raise ForgeError("Run projection identity differs")
        state, current = _state(db, attempt)
        attempt = state["attempt"]
        action_sequence = db.execute(
            "SELECT coalesce(max(sequence),0) FROM action_events WHERE attempt=?",
            (attempt,),
        ).fetchone()[0]
        observation_revision,observation_count,node_sequence=_observation_revision(db,attempt)
        observations = []
        # Scalar projection, not full 8 KiB node/evidence documents. The full
        # graph remains explicitly available through the original detail API.
        fields = (
            "state",
            "rows_input",
            "rows_output",
            "calls",
            "errors",
            "elapsed_s",
            "timing_scope",
        )
        query = "SELECT action_id,node_id,occurrence," + ",".join(
            "json_extract(document,'$." + k + "')" for k in fields
        )
        query += " FROM node_observations WHERE attempt=? ORDER BY terminal,action_id,node_id,occurrence LIMIT 65"
        for row in db.execute(query, (attempt,)):
            observations.append(
                dict(
                    action_id=row[0],
                    node_id=row[1],
                    occurrence=row[2],
                    **dict(zip(fields, row[3:])),
                )
            )
        outputs = []
        for name, spec in summary["outputs"].items():
            # Required facts are projected separately from arbitrary action
            # values. Most recent committed attempt retains its own identity.
            facts = {}
            retained = None
            query = """SELECT attempt,kind,json_extract(document,'$.status'),json_extract(document,'$.ref')
                     FROM action_events WHERE attempt<=? AND action_id=?
                     AND kind IN ('intent','receipt','recovered','stage_commit','stage_recovered','result','recovered_result')
                     ORDER BY attempt DESC,sequence DESC LIMIT 256"""
            candidates = {}
            for a, kind, status, reference in db.execute(
                query, (attempt, spec["action_id"])
            ):
                candidates.setdefault(a, {})[kind] = (status, reference)
            facts = candidates.get(attempt, {})
            if not any(
                k in facts
                for k in ("receipt", "recovered", "stage_commit", "stage_recovered")
            ):
                for a, previous in candidates.items():
                    if a != attempt and (
                        "stage_commit" in previous
                        or "stage_recovered" in previous
                        or any(
                            previous.get(k, (None,))[0] == "committed"
                            for k in ("receipt", "recovered")
                        )
                    ):
                        facts, retained = previous, a
                        break
            committed = (
                "stage_commit" in facts
                or "stage_recovered" in facts
                or any(
                    facts.get(k, (None,))[0] == "committed"
                    for k in ("receipt", "recovered")
                )
            )
            status = (
                "committed"
                if committed
                else (
                    ("writing" if state["status"] in runs.ACTIVE else "unknown")
                    if "intent" in facts
                    else "not_started"
                )
            )
            reference = (
                facts.get("recovered_result") or facts.get("result") or (None, None)
            )[1]
            ref = json.loads(reference) if reference else None
            outputs.append(
                dict(
                    name=name,
                    **spec,
                    status=status,
                    retained_from_attempt=retained,
                    result=(
                        {"ref": ref, "location_id": spec["location_id"]}
                        if ref
                        else None
                    ),
                    result_digest=content_digest(ref) if ref else None,
                )
            )
    telemetry = run_telemetry.latest(target, attempt)
    alive = (
        runs.process_matches(state.get("supervisor"))
        if attempt == current and state["status"] in runs.ACTIVE
        else None
    )
    sample = telemetry.get("sample") or {}
    current_revision = content_digest(
        dict(
            state=state,
            actions=action_sequence,
            nodes=observation_revision,
            telemetry=[sample.get("epoch"), sample.get("sequence")],
            supervisor_alive=alive,
        )
    )
    freshness = dict(telemetry_age_s=telemetry.get("age_s"), supervisor_alive=alive)
    if revision == current_revision:
        return dict(
            run_id=run_id,
            attempt=attempt,
            revision=current_revision,
            unchanged=True,
            freshness=freshness,
        )
    result = dict(
        format="demiflow_forge_runtime_v1",
        availability="available",
        run_id=run_id,
        attempt=attempt,
        current_attempt=current,
        revision=current_revision,
        state=state,
        status=state["status"],
        entry=summary["entry"],
        digest=summary["digest"],
        limits=summary["limits"],
        outputs=outputs,
        observations=observations[:64],
        observations_truncated=len(observations) > 64,
        telemetry=telemetry,
        supervisor_alive=alive,
        freshness=freshness,
        action_sequence=action_sequence,
        node_sequence=node_sequence,
        observation_revision=observation_revision,
        observation_count=observation_count,
    )
    bounded_json(result, 128 * 1024)
    return result


def events(root, run_id, *, attempt=None, cursor=None, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ForgeError("Invalid control event page size", code="invalid_request")
    target, connection = _connect(root, run_id)
    with closing(connection) as db:
        db.execute("BEGIN")
        state, _ = _state(db, attempt)
        attempt = state.get("attempt")
        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='control_events'"
        ).fetchone():
            return dict(events=[], availability="unsupported")
        after = 0
        if cursor:
            parts = cursor.split(":")
            if (
                len(parts) != 3
                or parts[:2] != [run_id, str(attempt)]
                or not parts[2].isdigit()
                or len(parts[2]) > 20
            ):
                raise ForgeError(
                    "Control cursor belongs to a different run or attempt",
                    code="conflict",
                )
            after = int(parts[2])
        high = db.execute(
            "SELECT coalesce(max(sequence),0) FROM control_events WHERE attempt=?",
            (attempt,),
        ).fetchone()[0]
        if after > high:
            raise ForgeError(
                "Control cursor is outside retained history", code="conflict"
            )
        rows = []
        size = 0
        for sequence, raw in db.execute(
            "SELECT sequence,document FROM control_events WHERE attempt=? AND sequence>? ORDER BY sequence LIMIT ?",
            (attempt, after, limit),
        ):
            size += len(raw.encode())
            if size > 120 * 1024:
                break
            rows.append(dict(sequence=sequence, **json.loads(raw)))
        last = rows[-1]["sequence"] if rows else after
        return dict(
            run_id=run_id,
            attempt=attempt,
            events=rows,
            cursor=f"{run_id}:{attempt}:{last}",
            has_more=last < high,
            gap=False,
            scope="control_events_only",
        )


def metrics(root, run_id, *, attempt=None, points=600, resolution="raw"):
    target, connection = _connect(root, run_id)
    with closing(connection) as db:
        state, _ = _state(db, attempt)
    return run_telemetry.series(
        target, attempt=state.get("attempt"), points=points, resolution=resolution
    )
