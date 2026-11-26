"""Exercise promotion through real Git refs and a local push, using private release evidence."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tools import release_gate as gate


def save(path, value):
    path.write_text(json.dumps(value))
    return gate.ref(path)


def git(repo, *args, success=True):
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    result = subprocess.run(['git', *args], cwd=repo, env=env, capture_output=True, text=True)
    if success:
        assert result.returncode == 0, result.stderr
    return result


@pytest.fixture
def checkout(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    git(repo, 'init', '-q', '-b', 'main')
    git(repo, 'config', 'user.name', 'Fixture')
    git(repo, 'config', 'user.email', 'fixture@example.org')
    known = 'docs/engine-known-state/fixture.json'
    (repo / known).parent.mkdir(parents=True)
    save(repo / known, {'lanes': {'standard': {'collection': {'files': {}}}}, 'entries': []})
    git(repo, 'add', 'docs')
    git(repo, 'commit', '-qm', 'base')
    base = gate.commit(repo, 'HEAD')
    git(repo, 'switch', '-qc', 'candidate')
    (repo / 'README.md').write_text('reviewed change\n')
    git(repo, 'add', 'README.md')
    git(repo, 'commit', '-qm', 'candidate')
    head = gate.commit(repo, 'HEAD')
    records = tmp_path / 'records'
    name = 'main-20260101-1'
    directory = records / name
    directory.mkdir(parents=True)
    report = directory / 'review.md'
    report.write_text('Reviewed the fixed candidate against the design and the failure cases.')
    merge = {'git_tree': gate.git(repo, 'rev-parse', 'HEAD^{tree}'), 'ok': True, 'tree_bound': True,
             'frozen_differences': [], 'missing_guards': [], 'guard_files': ['tests/test_release_gate.py'],
             'tests': {'passed': 2, 'returncode': 0, 'failed': [], 'skipped': 0}}
    pub = dict(main_tag=name, release_commit=head, tested_commit=head, previous_commit=base, known_state=known,
               gate_receipts=[save(directory / 'gate.json', merge)], lane_receipts=[], differences={}, exceptions={},
               review=dict(commit=head, architecture='passed', quality='passed', reviewer='owner', evidence=gate.ref(report)),
               scope=dict(reason='Documentation only', gate_only=True, out_of_scope='runtime suites'))
    return repo, records, directory, pub


def test_ordered_review_blocks_main_tag_and_push_until_changed_fix(checkout, tmp_path):
    repo, records, directory, pub = checkout
    head = pub['release_commit']
    remote = tmp_path / 'remote.git'
    git(tmp_path, 'init', '--bare', '-q', str(remote))
    git(repo, 'remote', 'add', 'origin', str(remote))
    git(repo, 'push', '-q', 'origin', 'main')
    save(directory / 'publication.json', pub)
    assert gate.check(repo, records, head) == head
    gate.install(repo, records)
    report = directory / 'review.md'
    rejected = gate.record_review(repo, records, head, report, 'reviewer')
    with pytest.raises(ValueError, match='changed descendant'):
        gate.record_review(repo, records, head, report, 'reviewer', rejected)
    for command in [('update-ref', 'refs/heads/main', head), ('tag', '-am', 'fixture', pub['main_tag'], head),
                    ('push', 'origin', 'candidate:main')]:
        result = git(repo, *command, success=False)
        assert result.returncode and 'Open blocking review' in result.stderr, result.stderr
    assert gate.commit(repo, 'main') == pub['previous_commit']
    assert gate.commit(remote, 'main') == pub['previous_commit']
    (repo / 'README.md').write_text('accepted correction\n')
    git(repo, 'commit', '-qam', 'address review')
    fixed = gate.commit(repo, 'HEAD')
    gate.record_review(repo, records, fixed, report, 'reviewer', rejected)
    with pytest.raises(ValueError, match='Open blocking review'):
        gate.check(repo, records, head)
    pub.update(tested_commit=fixed, release_commit=fixed)
    pub['review']['commit'] = fixed
    receipt = gate.read(directory / 'gate.json')
    receipt['git_tree'] = gate.git(repo, 'rev-parse', 'HEAD^{tree}')
    pub['gate_receipts'] = [save(directory / 'gate.json', receipt)]
    save(directory / 'publication.json', pub)
    git(repo, 'update-ref', 'refs/heads/main', fixed)
    git(repo, 'tag', '-am', 'accepted', pub['main_tag'], fixed)
    git(repo, 'push', '--atomic', 'origin', 'main', pub['main_tag'])
    assert gate.commit(remote, 'main') == gate.commit(remote, pub['main_tag']) == fixed


def test_green_counts_cannot_promote_another_tree(checkout):
    repo, records, directory, pub = checkout
    receipt = gate.read(directory / 'gate.json')
    del receipt['git_tree']
    pub['gate_receipts'] = [save(directory / 'gate.json', receipt)]
    save(directory / 'publication.json', pub)
    with pytest.raises((KeyError, ValueError)):
        gate.check(repo, records, pub['release_commit'])
    receipt['git_tree'] = gate.git(repo, 'rev-parse', 'main^{tree}')
    pub['gate_receipts'] = [save(directory / 'gate.json', receipt)]
    save(directory / 'publication.json', pub)
    with pytest.raises(ValueError, match='release tree'):
        gate.check(repo, records, pub['release_commit'])


def test_execution_receipts_must_cover_the_collected_scope(checkout):
    repo, records, directory, pub = checkout
    head = pub['release_commit']
    snap = {'head': head, 'inputs': {}}
    node = 'tests/test_example.py::test_works'
    record = dict(source_before=snap, source_after=snap, source_unchanged=True, returncode=0, reports=[],
                  collection_problems=[], deselected=[], args=[], environment={}, python=sys.executable,
                  items=[dict(file='tests/test_example.py', nodeid=node)])
    lane = dict(lane='standard', collection=save(directory / 'collection.json', dict(record, kind='collection_only')),
                executions=[])
    pub['lane_receipts'] = [lane]
    save(directory / 'publication.json', pub)
    with pytest.raises(ValueError, match='scope mismatch'):
        gate.check(repo, records, head)
    record.update(kind='execution', reports=[dict(nodeid=node, when=phase, outcome='passed') for phase in ('setup','call','teardown')])
    evidence = directory / 'execution.json'
    lane['executions'] = [save(evidence, record)]
    pub['differences'] = {'standard': {key: [] for key in gate.known_state.DIFFERENCES}}
    pub['differences']['standard']['collected_only_now'] = [node]
    save(directory / 'publication.json', pub)
    assert gate.check(repo, records, head) == head
    record['reports'][1]['outcome'] = 'skipped'
    lane['executions'] = [save(evidence, record)]
    save(directory / 'publication.json', pub)
    with pytest.raises((KeyError, ValueError)):
        gate.check(repo, records, head)
