"""Persistent local task lifecycle and snapshot-bound acceptance records."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import time
import uuid

from .io import atomic_write_json, atomic_write_text
from .launcher import SCRIPT
from .checks import structural
from .project import (ForgeError, PAGE_SIZE, REVIEW_RULES, SKIP, VERSION, PROJECT_LOCAL_STANDARDS, Project,
                      changes, digest, file_hash, no_symlinks, read_json, relative, safe_id, within)
from .runner import run


def now():
    return datetime.now(timezone.utc).isoformat()


def init(root, config_path):
    root = Path(root).absolute()
    if not root.is_dir():
        raise ForgeError("Project directory must already exist")
    no_symlinks(root)
    control = no_symlinks(root / ".demiforge")
    candidate = Project.__new__(Project)
    candidate.root, candidate.control = root, control
    candidate.config = read_json(Path(config_path).absolute())
    candidate.validate()
    control.mkdir(exist_ok=True)
    with no_symlinks(control / ".init.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        targets = [control / name for name in ("project.json", "forge", ".gitignore")]
        if any(p.exists() or p.is_symlink() for p in targets):
            raise ForgeError("Already initialized or partial bootstrap exists; refusing overwrite")
        atomic_write_json(targets[0], candidate.config)
        atomic_write_text(targets[1], SCRIPT)
        os.chmod(targets[1], 0o755)
        atomic_write_text(targets[2], "tasks/\n.init.lock\n")
    return {"project": str(root), "standard": candidate.config["standard"], "status": "initialized"}


@contextmanager
def locked(project, task_id, *, create=False, write=True):
    safe_id(task_id)
    store = getattr(project, "task_store", None)
    directory = store.directory(task_id) if store else no_symlinks(project.control / "tasks" / task_id)
    if create:
        if store:
            store.create(task_id)
        else:
            directory.mkdir(parents=True, exist_ok=False)
    elif not directory.is_dir():
        raise ForgeError(f"Unknown task: {task_id}")
    lockpath = no_symlinks(directory / ".lock")
    with lockpath.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ForgeError(f"Task is busy: {task_id}") from error
        path = no_symlinks(directory / "task.json")
        state = {} if create else read_json(path)
        yield directory, state
        if write:
            atomic_write_json(path, state)


def start(project, task_id, pipeline, goal, writes, non_goals, *, metadata=None):
    project.pipeline(pipeline)
    if not goal.strip() or not writes or not non_goals:
        raise ForgeError("Task requires a goal, explicit writable paths and non-goals")
    for name in writes:
        relative(name, modern=project.standard(pipeline) in PROJECT_LOCAL_STANDARDS)
        no_symlinks(project.root / name)
        if not any(within(name, w) for w in project.config["watch"]):
            raise ForgeError(f"Writable path is not in watched roots: {name}")
        for readonly in project.config["readonly"]:
            target = (project.root / name).resolve()
            protected = (project.root / readonly).resolve()
            if target.is_relative_to(protected) or protected.is_relative_to(target):
                raise ForgeError(f"Writable scope intersects readonly: {name}")
    binding = project.binding()
    observation = project.observe(pipeline)
    if observation.coverage is not None:
        if observation.facts.unwatched:
            raise ForgeError('Necessary local source is not watched; cannot capture its original baseline')
        unknown = [p for scope in ('tree', 'readonly')
                   for p in observation.coverage[scope]['unverified_ranges']]
        for name in writes:
            if any(within(name, p) or within(p, name) for p in unknown):
                raise ForgeError(f'Writable scope intersects unverified source coverage: {name}')
    if project.binding() != binding:
        raise ForgeError('Tool/config changed during start observation')
    initial = {"schema": "demiforge-task-v1", "id": task_id, "pipeline": pipeline,
               "goal": goal, "writes": sorted(set(writes)), "non_goals": non_goals,
               "created": now(), "binding": binding, "baseline": observation.tree,
               "readonly_baseline": observation.readonly, "sources": observation.sources,
               "reads": {}, "notes": [], "check": None, "verification": None, "review": None}
    if observation.coverage is not None:
        initial['baseline_coverage'] = observation.coverage
    if metadata:
        if set(metadata) != {'managed'}:
            raise ForgeError('Unsupported trusted task metadata')
        initial.update(metadata)
        managed = initial['managed']
        managed['bound_contract']['baseline_fingerprint'] = digest({
            key: initial.get(key) for key in ('baseline', 'readonly_baseline', 'baseline_coverage')})
        managed['contract_digest'] = digest(managed['bound_contract'])
    with locked(project, task_id, create=True) as (_, state):
        state.update(initial)
    return {"task": task_id, "status": "started", "writes": initial["writes"],
            "next": f"context --id {task_id}", "note": "Baseline includes existing dirty/untracked files"}


def current(project, state, *, with_observation=False):
    if state.get('managed', {}).get('context_binding_problem'):
        raise ForgeError('Supplementary authority source set changed; an explicit contract update is required')
    binding = project.binding()
    from .registration import binding_for
    expected_binding = binding_for(project, state) if 'managed' in state else state["binding"]
    if binding != expected_binding:
        raise ForgeError("Tool, standard or project configuration changed; use read-only history. "
                         "Continuation requires a separately reviewed plan preserving the original baseline; "
                         "do not restart this task from the current tree")
    observation = project.observe(state['pipeline'])
    tree, readonly, sources = observation.tree, observation.readonly, observation.sources
    if project.binding() != binding:
        raise ForgeError('Tool/config changed during current observation')
    basis = {"tree": tree, "readonly": readonly, "sources": sources,
                          "binding": state["binding"],
                          "task": {k: state[k] for k in ("id", "goal", "pipeline", "writes", "non_goals")},
                          "decisions": [n for n in state["notes"] if n["kind"] == "decision" or
                                        (n["kind"] == "question" and n.get("resolved"))]}
    if observation.coverage is not None:
        basis['coverage'] = observation.coverage
    if 'managed' in state:
        basis['managed_contract'] = state['managed']['contract_digest']
        basis['managed_questions'] = managed_questions(state)
        supplemental = context_baseline(project, state)
        if supplemental is not None:
            basis['managed_context'] = supplemental_context(supplemental)
    fingerprint = digest(basis)
    values = (tree, readonly, sources, fingerprint)
    return (*values, observation) if with_observation else values





def readonly_baseline(project, state):
    if 'managed' not in state: return state['readonly_baseline']
    from .registration import readonly_baseline as effective
    return effective(project, state)


def context_baseline(project, state):
    from .registration import supplemental_sources
    return supplemental_sources(project, state)


def managed_current_request(state):
    decisions = [n for n in state['notes'] if n['kind'] == 'decision' and n.get('event_id')]
    if not decisions:
        return None
    last = decisions[-1]['event_id']
    prefix, separator, suffix = last.rpartition(':')
    turn = prefix if separator and suffix.isdecimal() else last
    selected = [n for n in decisions if (n['event_id'].rpartition(':')[0] == turn and
                n['event_id'].rpartition(':')[2].isdecimal())] if turn != last else [decisions[-1]]
    return dict(turn_id=turn, text=''.join(n['text'] for n in selected),
                event_ids=[n['event_id'] for n in selected])


def managed_questions(state):
    """Question/answer facts from controller-recorded human decision events."""
    result, used = [], 0
    for index, note in enumerate(state['notes']):
        if note['kind'] != 'question':
            continue
        item = dict(note_index=index, text=note['text'], resolved=bool(note.get('resolved')),
                    answer_event_ids=[], answers=[])
        if note.get('resolved'):
            resolution = note['resolved']
            if (type(resolution) is not dict or set(resolution) != {'answer_event_ids'} or
                    type(resolution['answer_event_ids']) is not list or not resolution['answer_event_ids']):
                raise ForgeError('Managed question resolution requires recorded human decision references')
            ids = resolution['answer_event_ids']
            matches = [(i, n) for i, n in enumerate(state['notes'])
                       if n['kind'] == 'decision' and n.get('event_id') in ids]
            if ([n['event_id'] for _, n in matches] != ids or any(i <= index for i, _ in matches)):
                raise ForgeError('Managed question answer must follow the question in recorded human decisions')
            item.update(answer_event_ids=ids,
                        answers=[dict(event_id=n['event_id'], text=n['text']) for _, n in matches])
        used += len(json.dumps(item, ensure_ascii=False).encode())
        if used > 4 * 1024 * 1024:
            raise ForgeError('Managed question/answer context exceeds its bounded note budget')
        result.append(item)
    return result


def pending_questions(state):
    return [dict(note_index=i, text=n['text']) for i, n in enumerate(state['notes'])
            if n['kind'] == 'question' and not n.get('resolved')]


def supplemental_context(sources):
    """Recheck controller-delivered full files, without granting new write scope."""
    if type(sources) is not list or len(sources) > 64:
        raise ForgeError('Supplementary context requires at most 64 full-file sources')
    deadline = time.monotonic() + 5
    result = {}
    for source in sources:
        if time.monotonic() > deadline:
            raise ForgeError('Supplementary context scan exceeded five seconds')
        if type(source) is not dict or set(source) != {'canonical_path', 'sha256'}:
            raise ForgeError('Supplementary context requires canonical_path and raw sha256')
        label, expected = source['canonical_path'], source['sha256']
        if not isinstance(label, str) or len(label) > 4096:
            raise ForgeError('Invalid supplementary context path')
        path = no_symlinks(Path(label))
        if not path.is_absolute() or str(path) != label or str(path.resolve()) != label:
            raise ForgeError('Supplementary context requires a canonical absolute file path')
        if (not isinstance(expected, str) or len(expected) != 64 or
                any(char not in '0123456789abcdef' for char in expected)):
            raise ForgeError('Supplementary context requires a raw SHA-256 digest')
        actual = file_hash(path, limit=1024 * 1024, deadline=deadline)
        if actual != expected:
            raise ForgeError(f'Supplementary authoritative context changed: {label}')
        if label in result and result[label] != expected:
            raise ForgeError('Conflicting supplementary context source digests')
        result[label] = expected
    return [dict(canonical_path=path, sha256=sha) for path, sha in sorted(result.items())]


def context(project, state, source_id=None, page=1):
    _, _, sources, fingerprint, observation = current(project, state, with_observation=True)
    if source_id is None:
        return {"id": state["id"], "goal": state["goal"], "non_goals": state["non_goals"],
                "writes": state["writes"], "watch": project.config["watch"],
                "readonly": project.config["readonly"],
                "excluded_directories": sorted(SKIP - {'runs', 'datasets'} if observation.coverage is not None else SKIP),
                **({'source_coverage': observation.coverage} if observation.coverage is not None else {}),
                "sources": sources, "read_pages": state["reads"], "notes": state["notes"],
                **({"questions": managed_questions(state), "current_request": managed_current_request(state)}
                   if 'managed' in state else {}),
                "fingerprint": fingerprint,
                "evidence": {k: evidence_status(state[k], fingerprint) for k in ("check", "verification", "review")},
                "next": "Read each required source page with context --read ID --page N; excerpts are explicit"}
    if sources != state["sources"]:
        raise ForgeError("Context sources changed; refresh --reason before recording new reads")
    if source_id not in sources:
        raise ForgeError(f"Unknown source: {source_id}")
    info = sources[source_id]
    if not 1 <= page <= info["pages"]:
        raise ForgeError(f"Page must be 1..{info['pages']}")
    text = project.source(info["definition"], identities=observation.identities,
                          deadline=observation.facts.deadline if observation.facts else None)
    observation.stable()
    if digest(text) != info["sha256"]:
        raise ForgeError("Source changed during context read; retry")
    state["reads"][source_id] = sorted(set(state["reads"].get(source_id, []) + [page]))
    return f"# {source_id} | page {page}/{info['pages']} | sha256 {info['sha256']}\n" + text[(page-1)*PAGE_SIZE:page*PAGE_SIZE]


def refresh(project, state, reason):
    _, _, sources, _ = current(project, state)
    if not reason.strip():
        raise ForgeError("Context refresh requires a reason")
    changed = [key for key in sources if sources[key] != state["sources"].get(key)]
    state["reads"] = {k: v for k, v in state["reads"].items() if k not in changed}
    state["sources"] = sources
    for key in ("check", "verification", "review"):
        state[key] = None
    state["notes"].append({"kind": "context-refresh", "text": reason, "changed": changed, "time": now()})
    return {"refreshed": changed, "baseline_preserved": True}


def check(project, state):
    tree, readonly, sources, fingerprint, observation = current(project, state, with_observation=True)
    changed = changes(state["baseline"], tree)
    violations = [f"DF-01 out-of-scope change: {name}" for name in changed
                  if not any(within(name.rstrip("/"), w) for w in state["writes"])]
    violations += [f"DF-01 readonly changed: {name}" for name in changes(readonly_baseline(project, state), readonly)]
    managed = state.get('managed')
    if (managed and managed['request']['contract'].get('require_context_binding', False)
            and 'context_baseline' not in managed['bound_contract']):
        violations.append('DF-01 supplemental authority context has not been bound by the controller')
    if observation.coverage is not None and 'baseline_coverage' in state:
        for scope in ('tree', 'readonly'):
            before_unknown = set(state['baseline_coverage'][scope]['unverified_ranges'])
            after_unknown = set(observation.coverage[scope]['unverified_ranges'])
            for path in sorted(before_unknown ^ after_unknown):
                if scope == 'readonly' or not any(within(path, w) for w in state['writes']):
                    violations.append(f'DF-01 {scope} coverage boundary changed outside original scope: {path}')
    if sources != state["sources"]:
        violations.append("DF-01 context changed: refresh and read changed sources")
    for key, info in sources.items():
        if state["reads"].get(key) != list(range(1, info["pages"] + 1)):
            violations.append(f"DF-01 required context pages unread: {key}")
    violations += structural(project, state["pipeline"], observation=observation)
    state["check"] = {"fingerprint": fingerprint, "time": now(), "passed": not violations,
                      "changed": changed, "violations": violations,
                      "semantic_review_required": list(REVIEW_RULES)}
    if observation.coverage is not None:
        state['check']['source_coverage'] = observation.coverage
    return state["check"]


def verify(project, state, directory, allow_tests, *, timeout_seconds=None, launch=None):
    if not allow_tests:
        raise ForgeError("Verification runs trusted project code; explicit --allow-tests required")
    result = check(project, state)
    if not result["passed"]:
        raise ForgeError("Mechanical check failed; run check for details")
    fingerprint = result["fingerprint"]
    invocation = uuid.uuid4().hex
    evidence = no_symlinks(directory / "verification" / invocation)
    evidence.mkdir(parents=True)
    state["verification"] = {"fingerprint": fingerprint, "time": now(), "passed": False,
                             "status": "running", "invocation": invocation, "commands": []}
    atomic_write_json(directory / "task.json", state)
    commands = state["verification"]["commands"]
    deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None
    for command in project.pipeline(state["pipeline"])["verify"]:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            command = {**command, "timeout": min(command["timeout"], remaining)}
        try:
            outcome = run(command, project, evidence / (command["id"] + "-scratch"), **({'launch': launch} if launch else {}))
        except OSError as error:
            outcome = {"id": command["id"], "passed": False, "failure": str(error), "output": ""}
        output = outcome.pop("output")
        logfile = evidence / (command["id"] + ".log")
        atomic_write_text(logfile, output)
        if getattr(getattr(project, 'task_store', None), 'managed', False):
            outcome["log"] = str(logfile.relative_to(directory))
            outcome["log_base"] = "task"
        else:
            outcome["log"] = str(logfile.relative_to(project.root))
        outcome["log_sha256"] = digest(output)
        commands.append(outcome)
        if not outcome["passed"]:
            break
    _, _, _, after = current(project, state)
    result = state["verification"]
    result["source_changed_during_tests"] = after != fingerprint
    result["passed"] = after == fingerprint and all(c["passed"] for c in commands) and len(commands) == len(project.pipeline(state["pipeline"])["verify"])
    result["status"] = "passed" if result["passed"] else "failed"
    result["sandbox"] = bool(launch and commands and all(c.get('execution', {}).get('assurance') == 'enforced' for c in commands))
    return result


def review(project, state, review_path, reviewer):
    if 'managed' in state:
        raise ForgeError("Managed Review requires the trusted controller reviewer channel")
    tree, readonly, _, fingerprint = current(project, state)
    if not reviewer.strip():
        raise ForgeError("Identify the reviewer; self-review is not independent review")
    claims = read_json(Path(review_path).absolute())
    validate_review_claims(project, tree, readonly, claims)
    state["review"] = {"fingerprint": fingerprint, "time": now(), "passed": True,
                       "reviewer": reviewer, "type": "reviewer-attestation", "claims": claims}
    return state["review"]


def validate_review_claims(project, tree, readonly, claims, *, allow_failure=False):
    if not isinstance(claims, dict) or set(claims) != set(REVIEW_RULES):
        raise ForgeError(f"Review must cover exactly {', '.join(REVIEW_RULES)}")
    known = {str((project.root / p).resolve()) for p, h in {**tree, **readonly}.items() if h not in (None, "directory")}
    for rule, claim in claims.items():
        if not isinstance(claim, dict) or set(claim) != {"status", "reason", "evidence"}:
            raise ForgeError(f"{rule}: requires status, reason, evidence")
        statuses = {"pass", "not_applicable", "fail"} if allow_failure else {"pass", "not_applicable"}
        if claim["status"] not in statuses or not isinstance(claim["reason"], str) or not claim["reason"].strip():
            raise ForgeError(f"{rule}: unjustified/failed review cannot be accepted")
        if rule != "DF-05" and claim["status"] == "not_applicable":
            raise ForgeError(f"{rule} is mandatory; only prompt review permits justified N/A")
        if not isinstance(claim["evidence"], list) or not claim["evidence"]:
            raise ForgeError(f"{rule}: evidence paths required")
        for path in claim["evidence"]:
            if not isinstance(path, str) or str((project.root / path).resolve()) not in known:
                raise ForgeError(f"{rule}: evidence is not a snapshotted source: {path}")


def evidence_status(record, fingerprint):
    if record is None:
        return "missing"
    if record["fingerprint"] != fingerprint:
        return "stale"
    return "passed" if record["passed"] else "failed"


def handoff(project, state, directory, accept):
    if 'managed' in state and accept:
        raise ForgeError("Managed acceptance requires the trusted controller channel")
    result = check(project, state)
    fingerprint = result["fingerprint"]
    gates = {k: evidence_status(state[k], fingerprint) for k in ("check", "verification", "review")}
    log_problems = verification_log_problems(project, state, directory)
    if log_problems:
        gates["verification"] = "corrupt"
    unresolved = [n["text"] for n in state["notes"] if n["kind"] == "question" and not n.get("resolved")]
    ready = all(v == "passed" for v in gates.values()) and not unresolved
    report = {"task": state["id"], "time": now(), "goal": state["goal"],
              "pipeline": state["pipeline"], "writes": state["writes"], "changed": result["changed"],
              "non_goals": state["non_goals"], "fingerprint": fingerprint,
              "status": "accepted-local" if accept and ready else "incomplete",
              "gates": gates, "violations": result["violations"] + log_problems,
              "notes": state["notes"], "unresolved_questions": unresolved,
              "next_steps": [] if ready else ["Resolve violations/questions and obtain current passing evidence"],
              "watch": project.config["watch"], "readonly": project.config["readonly"],
              "excluded_directories": sorted(SKIP - {'runs', 'datasets'} if 'source_coverage' in result else SKIP),
              **({'source_coverage': result['source_coverage']} if 'source_coverage' in result else {}),
              "verification": state["verification"],
              "review": state["review"], "sources": state["sources"],
              "limits": ["Local development-task acceptance only; not whole-pipeline certification",
                         "Reviewer attestation, not automatic or independent semantic proof",
                         "Declared source roots only; concurrent changes are not attributed to an author",
                         "No OS sandbox, production run or business-quality certification"]}
    atomic_write_json(directory / "handoff.json", report)
    lines = [f"# DemiForge Handoff: {state['id']}", "", f"Status: {report['status']}",
             f"Goal: {state['goal']}", f"Fingerprint: `{fingerprint}`", "", "## Gates"]
    lines += [f"- {k}: {v}" for k, v in gates.items()]
    for heading, entries in (("Changed", result["changed"]), ("Non-goals", state["non_goals"]),
                             ("Violations", report["violations"]), ("Next Steps", report["next_steps"]),
                             ("Limits", report["limits"])):
        lines += ["", f"## {heading}", *[f"- {entry}" for entry in entries]]
    if 'source_coverage' in report:
        coverage = report['source_coverage']
        lines += ['', '## Source coverage', f"Policy: {coverage['policy']}",
                  f"Complete: {coverage['complete']}",
                  'Default pruning candidates: ' + ', '.join(coverage['default_pruning_candidates']),
                  f"Exact sources: {len(coverage['exact_sources'])}; author subtrees: {len(coverage['author_subtrees'])}",
                  f"Unverified tree ranges: {len(coverage['tree']['unverified_ranges'])}; readonly ranges: {len(coverage['readonly']['unverified_ranges'])}",
                  'Full source, exception, external-import and unverified-range lists are in handoff.json.']
    lines += ["", "## Notes", *[f"- {n['kind']}: {n['text']}" for n in state["notes"]]]
    atomic_write_text(directory / "handoff.md", "\n".join(lines) + "\n")
    return {"status": report["status"], "gates": gates, "ready": ready,
            "handoff": str(directory / "handoff.md"), "violations": report["violations"],
            "unresolved_questions": unresolved}


def verification_log_problems(project, state, directory):
    problems = []
    if state.get("verification"):
        for command in state["verification"]["commands"]:
            base = directory if command.get("log_base") == "task" else project.root
            label = command.get("log", "")
            try:
                if command.get("log_base") == "task":
                    path = Path(label)
                    if path.is_absolute() or '..' in path.parts or not label:
                        raise ForgeError("Invalid task-relative verification log")
                path = no_symlinks(base / label)
                if not path.is_file() or path.stat().st_size > 4 * 1024 * 1024 or digest(path.read_text(encoding="utf-8")) != command["log_sha256"]:
                    problems.append(f"Missing/changed verification log: {path}")
            except (OSError, ValueError, KeyError) as error:
                problems.append(f"Invalid verification log: {label}: {error}")
    return problems
