"""Small explicit CLI; no agent loop or model dependency."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from . import tasks
from .project import ForgeError, Project, read_json


def parser():
    p = argparse.ArgumentParser(prog="demiflow forge dev", description="Snapshot-bound business pipeline development")
    p.add_argument("--project", default=".", help="Project directory, default current directory")
    commands = p.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Bootstrap from a reviewed project config; never overwrite")
    init.add_argument("--config", required=True)
    for name in ("list", "history"):
        history = commands.add_parser(name, help="Read saved tasks; current evidence remains unverified")
        history.add_argument("--offset", type=int, default=0)
        history.add_argument("--limit", type=int, default=20)
        if name == "history":
            history.add_argument("--id")
            history.add_argument("--artifact", choices=("task", "handoff", "handoff-md"))
            history.add_argument("--page", type=int, default=1)
    start = commands.add_parser("start", help="Capture current dirty tree and task boundary BEFORE editing")
    start.add_argument("--id", required=True)
    start.add_argument("--pipeline", required=True)
    start.add_argument("--goal", required=True)
    start.add_argument("--write", action="append", required=True)
    start.add_argument("--non-goal", action="append", required=True)
    for name in ("context", "refresh", "check", "verify", "review", "note", "resolve", "handoff"):
        command = commands.add_parser(name)
        command.add_argument("--id", required=True)
        if name == "context":
            command.add_argument("--read", dest="source")
            command.add_argument("--page", type=int, default=1)
        elif name == "refresh":
            command.add_argument("--reason", required=True)
        elif name == "verify":
            command.add_argument("--allow-tests", action="store_true")
        elif name == "review":
            command.add_argument("--file", required=True)
            command.add_argument("--reviewer", required=True)
        elif name == "note":
            command.add_argument("--kind", choices=["decision", "progress", "question", "unverified", "next"], required=True)
            command.add_argument("--text", required=True)
        elif name == "resolve":
            command.add_argument("--question", type=int, required=True, help="Zero-based note index")
            command.add_argument("--reason", required=True)
        elif name == "handoff":
            command.add_argument("--accept", action="store_true")
    return p


def execute(args):
    if args.command == "init":
        return tasks.init(args.project, args.config), 0
    if args.command in {"list", "history"}:
        from .history import view
        return view(args.project, getattr(args, "id", None), artifact=getattr(args, "artifact", None),
                    page=getattr(args, "page", 1), offset=args.offset, limit=args.limit), 0
    project = Project(args.project)
    if args.command == "start":
        return tasks.start(project, args.id, args.pipeline, args.goal, args.write, args.non_goal), 0
    with tasks.locked(project, args.id) as (directory, state):
        if args.command == "context":
            return tasks.context(project, state, args.source, args.page), 0
        if args.command == "refresh":
            return tasks.refresh(project, state, args.reason), 0
        if args.command == "check":
            result = tasks.check(project, state)
            return result, 0 if result["passed"] else 1
        if args.command == "verify":
            result = tasks.verify(project, state, directory, args.allow_tests)
            return result, 0 if result["passed"] else 1
        if args.command == "review":
            return tasks.review(project, state, args.file, args.reviewer), 0
        if args.command == "note":
            tasks.current(project, state)
            if not args.text.strip():
                raise ForgeError("Note cannot be empty")
            state["notes"].append({"kind": args.kind, "text": args.text, "time": tasks.now()})
            return {"note_index": len(state["notes"]) - 1}, 0
        if args.command == "resolve":
            tasks.current(project, state)
            if not 0 <= args.question < len(state["notes"]) or state["notes"][args.question]["kind"] != "question" or not args.reason.strip():
                raise ForgeError("Resolve requires an existing question index and reason")
            state["notes"][args.question].update(resolved=args.reason, resolved_at=tasks.now())
            return {"resolved": args.question}, 0
        if args.command == "handoff":
            result = tasks.handoff(project, state, directory, args.accept)
            return result, 1 if args.accept and not result["ready"] else 0
    raise ForgeError("Unknown command")


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result, code = execute(args)
        print(result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, indent=2))
        return code
    except (ForgeError, OSError, UnicodeError) as error:
        print(f"demiforge: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("demiforge: interrupted; no new passing verification recorded", file=sys.stderr)
        return 130
