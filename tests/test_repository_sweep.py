"""The sweep names each thing the branch and worktree rules ask to be tidied, and changes nothing.

One real repository is built with one case per category: a merged branch that was not deleted, a
branch whose patch is in the integration line under another id, an old unmerged branch, a recent
one, a worktree with uncommitted work, one in use, one outside the agreed directory, one whose
directory is gone, a stash entry, and a clone holding a branch the repository never received.
"""
import os
import shutil
import subprocess
import sys

import pytest

import repository_sweep as sweep_tool

NOW = 1_800_000_000
OLD = NOW - 10 * 86400
RECENT = NOW - 3600


def g(path, *arguments, stamp=RECENT):
    env = dict(os.environ, GIT_AUTHOR_NAME='t', GIT_AUTHOR_EMAIL='t@t', GIT_COMMITTER_NAME='t',
               GIT_COMMITTER_EMAIL='t@t', GIT_AUTHOR_DATE=f'{stamp} +0000', GIT_COMMITTER_DATE=f'{stamp} +0000',
               GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull)
    child = subprocess.run(['git', '-C', str(path), *arguments], capture_output=True, text=True, env=env)
    assert child.returncode == 0, (arguments, child.stderr)
    return child.stdout.strip()


def commit(path, name, text, *, stamp=RECENT):
    (path / name).write_text(text)
    g(path, 'add', name, stamp=stamp)
    g(path, 'commit', '-q', '-m', f'add {name}', stamp=stamp)


@pytest.fixture
def world(tmp_path):
    repository = tmp_path / 'home' / 'repo'
    root = tmp_path / 'home' / '_demiflow' / 'worktrees'
    repository.mkdir(parents=True)
    root.mkdir(parents=True)
    g(repository, 'init', '-q', '-b', 'main')
    commit(repository, 'base.txt', 'base\n', stamp=OLD)
    g(repository, 'branch', 'integration')
    for name in ('gpt/engine/done-20261101', 'gpt/engine/picked-20261101', 'claude/test/old-20261101',
                 'claude/test/new-20261110', 'gpt/engine/dirty-20261110', 'gpt/engine/busy-20261110',
                 'gpt/engine/elsewhere-20261110', 'gpt/engine/gone-20261110'):
        g(repository, 'branch', name, 'main')
    trees = {}
    for name in ('integration', 'gpt/engine/done-20261101', 'gpt/engine/picked-20261101', 'claude/test/old-20261101',
                 'claude/test/new-20261110', 'gpt/engine/dirty-20261110', 'gpt/engine/busy-20261110',
                 'gpt/engine/gone-20261110'):
        trees[name] = root / name
        g(repository, 'worktree', 'add', '-q', str(trees[name]), name)
    trees['gpt/engine/elsewhere-20261110'] = tmp_path / 'tmp-elsewhere'
    g(repository, 'worktree', 'add', '-q', str(trees['gpt/engine/elsewhere-20261110']), 'gpt/engine/elsewhere-20261110')
    commit(trees['gpt/engine/done-20261101'], 'done.txt', 'done\n', stamp=OLD)
    commit(trees['gpt/engine/picked-20261101'], 'picked.txt', 'picked\n', stamp=OLD)
    commit(trees['claude/test/old-20261101'], 'old.txt', 'old\n', stamp=OLD)
    commit(trees['claude/test/new-20261110'], 'new.txt', 'new\n', stamp=RECENT)
    integration = trees['integration']
    g(integration, 'merge', '-q', '--no-ff', '-m', 'Merge done', 'gpt/engine/done-20261101')
    g(integration, 'cherry-pick', 'gpt/engine/picked-20261101')
    (trees['gpt/engine/dirty-20261110'] / 'unsaved.txt').write_text('work\n')
    (repository / 'base.txt').write_text('edited, then stashed\n')
    g(repository, 'stash', '-q')
    shutil.rmtree(trees['gpt/engine/gone-20261110'])
    clone = tmp_path / 'clone'
    g(tmp_path, 'clone', '-q', str(repository), str(clone))
    g(clone, 'checkout', '-q', '-b', 'claude/test/undelivered-20261110')
    commit(clone, 'only-here.txt', 'x\n')
    busy = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], cwd=trees['gpt/engine/busy-20261110'])
    try:
        yield {'repository': repository, 'root': root, 'trees': trees, 'clone': clone}
    finally:
        busy.kill()
        busy.wait()


def names(items):
    return sorted(item.get('name') or os.path.basename(item['path']) for item in items)


def run(world, **options):
    return sweep_tool.sweep(world['repository'], clones=[world['clone']], now=NOW, worktree_root=world['root'], **options)


def test_each_category_holds_exactly_its_case(world):
    report = run(world)
    branches, trees = report['branches'], report['worktrees']
    assert report['trunk_state']['behind_integration'] == 3 and report['trunk_state']['not_in_integration'] == 0
    assert names(branches['merged_not_deleted']) == ['gpt/engine/busy-20261110', 'gpt/engine/dirty-20261110',
                                                    'gpt/engine/done-20261101', 'gpt/engine/elsewhere-20261110',
                                                    'gpt/engine/gone-20261110']
    assert names(branches['stale']) == ['claude/test/old-20261101', 'gpt/engine/picked-20261101']
    assert names(branches['active']) == ['claude/test/new-20261110']
    assert names(trees['uncommitted']) == ['dirty-20261110']
    assert names(trees['in_use']) == ['busy-20261110']
    assert names(trees['outside_root']) == ['tmp-elsewhere']
    assert names(trees['directory_missing']) == ['gone-20261110']
    # removable: merged, clean, nobody working in it. Not the dirty one, not the busy one.
    assert names(trees['removable']) == ['done-20261101', 'tmp-elsewhere']
    assert len(report['stash']) == 1
    assert report['clones'][0]['undelivered'] == [
        {'name': 'claude/test/undelivered-20261110', 'head': report['clones'][0]['undelivered'][0]['head'],
         'state': 'not in the repository'}]


def test_the_trunk_and_the_integration_line_are_never_listed(world):
    report = run(world)
    listed = [item['name'] for group in report['branches'].values() for item in group]
    assert 'main' not in listed and 'integration' not in listed
    assert 'integration' not in names(report['worktrees']['removable'])


def test_a_rebased_or_picked_branch_is_recognised_only_by_comparing_patches(world):
    assert 'gpt/engine/picked-20261101' in names(run(world)['branches']['stale'])
    report = run(world, patches=True)
    assert names(report['branches']['patches_merged_not_deleted']) == ['gpt/engine/picked-20261101']
    assert names(report['branches']['stale']) == ['claude/test/old-20261101']
    assert 'picked-20261101' in names(report['worktrees']['removable'])


def test_a_branch_fetched_but_not_merged_is_still_undelivered(world):
    g(world['repository'], 'fetch', '-q', str(world['clone']), 'claude/test/undelivered-20261110:claude/test/undelivered-20261110')
    report = run(world)
    assert [item['state'] for item in report['clones'][0]['undelivered']] == ['in the repository, not merged']
    assert 'claude/test/undelivered-20261110' in names(report['branches']['active'])


def test_the_sweep_changes_nothing(world):
    def snapshot():
        return (g(world['repository'], 'for-each-ref'), g(world['repository'], 'worktree', 'list', '--porcelain'),
                g(world['repository'], 'stash', 'list'), g(world['repository'], 'status', '--porcelain'),
                sorted(os.listdir(world['repository'] / '.git')))
    before = snapshot()
    run(world, patches=True)
    assert snapshot() == before


def test_a_missing_integration_branch_is_reported_not_guessed(world):
    report = sweep_tool.sweep(world['repository'], integration='no-such-line', now=NOW)
    assert report['error'] == 'no branch named no-such-line'
    assert 'no-such-line' in sweep_tool.describe(report)


def test_the_text_report_counts_every_category(world):
    text = sweep_tool.describe(run(world))
    for _, label in sweep_tool.LABELS:
        assert label in text
    assert 'gpt/engine/done-20261101' in text and '(0 modified, 1 untracked)' in text
