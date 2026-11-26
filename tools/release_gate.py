"""Validate the recorded review, staged tree and scoped test receipts before publishing main."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
from uuid import uuid4

# An installation in the shared Git hooks directory carries the same receipt reader.
sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1] / 'tests')]
import engine_known_state as known_state


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    return known_state.file_digest(path)


def read(path):
    return json.loads(Path(path).read_text())


def artifact(base, ref):
    path = base / ref['path']
    require(digest(path) == ref['sha256'], f'Evidence changed: {path}')
    return path


def ref(path):
    return {'path': str(path.resolve()), 'sha256': digest(path)}


def commit(repo, revision):
    return git(repo, 'rev-parse', '--verify', '--end-of-options', revision + '^{commit}')


def ancestor(repo, first, last):
    code = subprocess.run(['git', '-C', str(repo), 'merge-base', '--is-ancestor', first, last]).returncode
    require(code in (0, 1), 'Cannot compare Git history')
    return code == 0


def write_file(path, data, mode=0o644):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as out:
        out.write(data)
        os.fchmod(out.fileno(), mode)
        out.flush()
        os.fsync(out.fileno())
    os.replace(out.name, path)


def record_review(repo, records, revision, evidence, reviewer, resolves=None):
    head = commit(repo, revision)
    tree = git(repo, 'rev-parse', head + '^{tree}')
    value = dict(commit=head, tree=tree, reviewer=reviewer, evidence=ref(evidence))
    directory = records / 'reviews'
    if resolves:
        require(re.fullmatch('[a-f0-9]{32}', resolves), 'Invalid review ID')
        blocked = read(directory / resolves / 'block.json')
        require(ancestor(repo, blocked['commit'], head) and tree != blocked['tree'],
                'A blocking review requires a changed descendant with verified fixes')
        target = directory / resolves / (head + '.json')
        identifier = resolves
    else:
        identifier = uuid4().hex
        target = directory / identifier / 'block.json'
    write_file(target, (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode())
    return identifier


def check_reviews(repo, records, head, tree):
    for target in sorted((records / 'reviews').glob('*/block.json')):
        blocked = read(target)
        if blocked['tree'] != tree and not ancestor(repo, blocked['commit'], head):
            continue
        closed = False
        for path in target.parent.glob('*.json'):
            if path == target:
                continue
            fix = read(path)
            if (blocked['tree'] != tree and fix['tree'] != blocked['tree']
                    and ancestor(repo, blocked['commit'], fix['commit']) and ancestor(repo, fix['commit'], head)):
                artifact(path.parent, fix['evidence'])
                closed = True
                break
        require(closed, f'Open blocking review {target.parent.name}: {blocked["evidence"]["path"]}')


def execution(path, head, inputs):
    states, receipt = known_state.read_execution(path, head=head, inputs=inputs)
    require(states and receipt['source_before'] == receipt['source_after']
            and not receipt['collection_problems'] and not receipt['deselected']
            and receipt['returncode'] in (0, 1), f'Incomplete execution: {path}')
    require({row['nodeid'] for row in receipt['reports']} == set(states), f'Unmatched test reports: {path}')
    require(len(states) == len(receipt['items']), f'Duplicate test selectors: {path}')
    require(receipt['returncode'] == 0 or any(s['outcome'] != 'passed' for s in states.values()),
            f'Test process failed outside a recorded test: {path}')
    return states


def disposition(base, decision, node, state, before, head):
    reason = decision['reason']
    require(decision.get('owner') and decision.get('explanation'), f'Missing disposition for {node}')
    if reason == 'user_exception':
        artifact(base, decision['authorization'])
        return
    proofs = decision['proofs']
    if reason == 'preexisting':
        revisions = [before]
    else:
        require(reason == 'intermittent', f'Unknown disposition for {node}')
        revisions = [before] * 3 + [head] * 3
    require(len(revisions) == len(proofs), f'Missing comparison runs for {node}')
    paths = [artifact(base, proof).resolve() for proof in proofs]
    require(len(set(paths)) == len(paths), f'Reused comparison runs for {node}')
    for path, expected in zip(paths, revisions):
        inputs = known_state.digest_inputs(read(path)['source_before']['inputs'])
        sample = execution(path, expected, inputs)[node]
        if reason == 'preexisting':
            require(known_state.signature(sample) == known_state.signature(state), f'Failure changed: {node}')
        else:
            require(sample['outcome'] == 'passed', f'Intermittent comparison failed: {node}')


def check_lanes(base, publication, known, head, tested):
    differences, covered_tests = {}, set()
    seen = set()
    handled = set()
    for lane in publication['lane_receipts']:
        name = lane['lane']
        require(name not in differences, f'Duplicate lane: {name}')
        revision = lane.get('commit', tested)
        require(revision in (tested, head), 'Lane selected an unrelated commit')
        collection_path = artifact(base, lane['collection'])
        raw = read(collection_path)
        collection = known_state.read_collection(collection_path)
        require(collection['head'] == revision and raw['source_before'] == raw['source_after']
                and not raw['deselected'] and raw['returncode'] == 0 and not collection['problems'],
                'Collection does not cover the fixed candidate')
        items = {item['nodeid'] for item in raw['items']}
        states = {}
        for proof in lane['executions']:
            batch = execution(artifact(base, proof), revision, collection['inputs'])
            require(not states.keys() & batch.keys(), f'Duplicate execution in {name}')
            states.update(batch)
        require(items and items == states.keys(), f'Collected/executed scope mismatch in {name}')
        differences[name] = known_state.compare(known, name, collection['files'], states, partial=True)
        if revision == head:
            covered_tests.update(collection['files'])
        for node, state in states.items():
            if state['outcome'] == 'passed':
                continue
            key = name + '/' + node
            disposition(base, publication['exceptions'][key], node, state, publication['previous_commit'], revision)
            handled.add(key)
        seen.add(revision)
    require(handled == set(publication['exceptions']), 'Exception does not match a recorded non-pass')
    require(publication['differences'] == differences, 'Recorded differences do not match the known-state comparison')
    if publication['lane_receipts']:
        require(tested in seen, 'Missing tested-commit execution')
    return covered_tests


def check(repo, records, revision, tag=None):
    head = commit(repo, revision)
    tree = git(repo, 'rev-parse', head + '^{tree}')
    check_reviews(repo, records, head, tree)
    if tag:
        require(re.fullmatch(r'main-\d{8}-[1-9]\d*', tag), 'Invalid main release tag')
        matches = [records / tag / 'publication.json']
    else:
        matches = [p for p in records.glob('main-*/publication.json') if read(p).get('release_commit') == head]
    require(len(matches) == 1, f'Expected one release record for {head}, found {len(matches)}')
    path = matches[0]
    base = path.parent
    pub = read(path)
    require(pub['release_commit'] == head and pub['main_tag'] == base.name, 'Release identity mismatch')
    tested, previous = pub['tested_commit'], pub['previous_commit']
    require(previous != head and ancestor(repo, previous, tested) and ancestor(repo, tested, head), 'Invalid release history')
    require(pub['scope'].get('reason') and pub['scope'].get('out_of_scope') is not None, 'Missing test scope decision')
    review = pub['review']
    require(review['commit'] == head and all(review.get(k) == 'passed' for k in ('architecture', 'quality'))
            and review.get('reviewer'), 'Missing architecture or quality sign-off on the release commit')
    artifact(base, review['evidence'])
    require(pub['gate_receipts'], 'Missing merge-gate evidence')
    for evidence in pub['gate_receipts']:
        gate = read(artifact(base, evidence))
        tests = gate['tests']
        require(gate['ok'] and gate['git_tree'] == tree and gate['tree_bound'] and not gate['missing_guards']
                and not gate['frozen_differences'] and gate['guard_files'] and tests['returncode'] == 0
                and tests['passed'] > 0 and not tests['failed'] and tests['skipped'] == 0,
                'Merge gate did not pass on the release tree')
    known = json.loads(git(repo, 'show', tested + ':' + pub['known_state']))
    changed = git(repo, 'diff', '--name-only', previous, head).splitlines()
    require(changed, 'Release has no changed files')
    if not pub['lane_receipts']:
        require(pub['scope'].get('gate_only') and all(p.endswith('.md') or p.startswith('docs/') for p in changed),
                'Missing scoped test receipts for a runtime change')
    covered = check_lanes(base, pub, known, head, tested)
    if tested != head:
        delta = git(repo, 'diff', '--name-only', tested, head).splitlines()
        require(delta == pub.get('tested_to_release') and all(
            p.endswith('.md') or p.startswith('docs/') or p.startswith('tests/test_') and p in covered for p in delta),
            'Release inputs differ from the tested commit')
    return head


def protected(repo, records, target, oid, old):
    if target != 'refs/heads/main' and not target.startswith('refs/tags/main-'):
        return
    require(oid.strip('0'), 'Published refs cannot be deleted')
    tag = target.removeprefix('refs/tags/') if target.startswith('refs/tags/') else None
    if tag:
        require(not old.strip('0'), 'Published tags are immutable')
        require(git(repo, 'cat-file', '-t', oid) == 'tag', 'Main release tag must be annotated')
    elif old.strip('0'):
        require(ancestor(repo, old, oid), 'Main may only fast-forward')
    check(repo, records, oid, tag)


def install(repo, records):
    hooks = Path(git(repo, 'rev-parse', '--path-format=absolute', '--git-path', 'hooks'))
    hooks.mkdir(exist_ok=True)
    program = hooks / 'demiflow-release' / 'release_gate.py'
    commands = ('reference-transaction', 'pre-push')
    scripts = {}
    for name in commands:
        hook = hooks / name
        body = ('#!/bin/sh\n# DemiFlow release gate\nexec ' + shlex.join(
            [sys.executable, '-B', str(program), '--repo', str(repo), '--records', str(records), name]) + ' "$@"\n').encode()
        require(not hook.exists() or hook.read_bytes() == body, f'Existing hook must be preserved: {hook}')
        scripts[hook] = body
    write_file(program.with_name('engine_known_state.py'), Path(known_state.__file__).read_bytes())
    write_file(program, Path(__file__).read_bytes())
    for hook, body in scripts.items():
        write_file(hook, body, 0o755)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--records', type=Path, required=True)
    sub = parser.add_subparsers(dest='action', required=True)
    sub.add_parser('install')
    promote = sub.add_parser('check')
    promote.add_argument('revision')
    promote.add_argument('--tag')
    review = sub.add_parser('review')
    review.add_argument('revision')
    review.add_argument('--evidence', type=Path, required=True)
    review.add_argument('--reviewer', required=True)
    kind = review.add_mutually_exclusive_group(required=True)
    kind.add_argument('--block', action='store_true')
    kind.add_argument('--resolve', metavar='BLOCK_ID')
    hook = sub.add_parser('reference-transaction')
    hook.add_argument('phase')
    sub.add_parser('pre-push').add_argument('remote', nargs='*')
    args = parser.parse_args()
    repo, records = args.repo.resolve(), args.records.resolve()
    if args.action == 'install':
        install(repo, records)
    elif args.action == 'review':
        print(record_review(repo, records, args.revision, args.evidence, args.reviewer, args.resolve))
    elif args.action == 'check':
        print(check(repo, records, args.revision, args.tag))
    elif args.action == 'pre-push':
        for line in sys.stdin:
            _, oid, target, old = line.split()
            if old != oid:
                protected(repo, records, target, oid, old)
    elif args.phase == 'prepared':
        for line in sys.stdin:
            old, oid, target = line.split()
            if old != oid:
                protected(repo, records, target, oid, old)


if __name__ == '__main__':
    try:
        main()
    except (KeyError, TypeError, ValueError, OSError, subprocess.CalledProcessError) as exc:
        sys.exit(f'DemiFlow release blocked: {exc}')
