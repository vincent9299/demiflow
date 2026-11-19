"""Read-only report of what the branch and worktree rules ask to be tidied.

usage: python tests/repository_sweep.py [--repository <path>] [--trunk main] [--integration integration]
                                        [--clone <path>]... [--days 3] [--patches] [--json <out.json>]

It lists, for one repository and its worktrees:

  - how far the trunk is behind the integration line;
  - branches whose commits are all in the integration line and can be deleted;
  - branches that are not in it and have had no commit for more than --days;
  - worktrees that can be removed, worktrees with uncommitted changes, worktrees a running
    process is working in, worktrees outside the agreed directory, registered worktrees whose
    directory is gone;
  - stash entries;
  - for each --clone, the branches whose head the repository does not have or has not merged.

Nothing is changed: every git command is a query and is run without taking optional locks.
--patches also recognises a branch whose patches are all in the integration line under other
commit ids (rebased or cherry-picked work); it is slower. Exit code 0; with --fail-on-findings,
1 when anything is listed.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def git(path, *arguments, trust=False, timeout=600):
    env = dict(os.environ, GIT_OPTIONAL_LOCKS='0')
    if trust:  # a clone owned by another account: reading it needs the ownership check waived
        env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='safe.directory', GIT_CONFIG_VALUE_0='*')
    child = subprocess.run(['git', '--no-optional-locks', '-C', str(path), *arguments],
                           capture_output=True, text=True, timeout=timeout, env=env)
    return child.returncode, child.stdout.strip()


def read_worktrees(repository):
    """One record per registered worktree: path, branch or None, head, whether the directory exists,
    and the number of modified and untracked entries."""
    _, text = git(repository, 'worktree', 'list', '--porcelain')
    records, current = [], {}
    for line in text.splitlines() + ['']:
        if not line:
            if current:
                records.append(current)
                current = {}
            continue
        key, _, value = line.partition(' ')
        current[key] = value or True
    trees = []
    for record in records:
        path = record['worktree']
        tree = {'path': path, 'head': record.get('HEAD', ''), 'exists': os.path.isdir(path),
                'branch': record['branch'].removeprefix('refs/heads/') if isinstance(record.get('branch'), str) else None,
                'modified': 0, 'untracked': 0}
        if tree['exists']:
            _, status = git(path, 'status', '--porcelain')
            lines = status.splitlines()
            tree['untracked'] = sum(1 for item in lines if item.startswith('??'))
            tree['modified'] = len(lines) - tree['untracked']
        trees.append(tree)
    return trees


def read_branches(repository):
    _, text = git(repository, 'for-each-ref', '--format=%(refname:short)%09%(objectname)%09%(committerdate:unix)', 'refs/heads')
    branches = []
    for line in text.splitlines():
        name, head, stamp = line.split('\t')
        branches.append({'name': name, 'head': head, 'committed': int(stamp)})
    return branches


def working_directories():
    """The working directory of every process this account may inspect (Linux /proc)."""
    found = set()
    for entry in Path('/proc').glob('[0-9]*'):
        try:
            found.add(os.readlink(entry / 'cwd'))
        except OSError:
            continue
    return found


def is_ancestor(repository, commit, of):
    return git(repository, 'merge-base', '--is-ancestor', commit, of)[0] == 0


def patches_all_in(repository, branch, integration):
    code, text = git(repository, 'cherry', integration, branch)
    return code == 0 and not any(line.startswith('+') for line in text.splitlines())


def classify_branches(branches, merged, *, protected, now, days, patch_merged=()):
    """merged: names whose head is in the integration line. Returns the three lists the rules name."""
    result = {'merged_not_deleted': [], 'patches_merged_not_deleted': [], 'stale': [], 'active': []}
    for branch in branches:
        name = branch['name']
        if name in protected:
            continue
        age = (now - branch['committed']) / 86400
        item = {'name': name, 'head': branch['head'][:10], 'days_since_commit': round(age, 1)}
        if name in merged:
            result['merged_not_deleted'].append(item)
        elif name in patch_merged:
            result['patches_merged_not_deleted'].append(item)
        elif age > days:
            result['stale'].append(item)
        else:
            result['active'].append(item)
    return result


def classify_worktrees(trees, merged, merged_heads, *, protected, repository, worktree_root, busy=()):
    """merged: branch names in the integration line; merged_heads: detached heads that are in it;
    busy: working directories of running processes. A worktree in use is never listed as removable."""
    result = {'removable': [], 'uncommitted': [], 'in_use': [], 'outside_root': [], 'directory_missing': []}
    root = str(Path(worktree_root).resolve()) + os.sep
    for tree in trees:
        path = tree['path']
        item = {'path': path, 'branch': tree['branch'], 'head': tree['head'][:10]}
        if not tree['exists']:
            result['directory_missing'].append(item)
            continue
        if os.path.realpath(path) == os.path.realpath(repository):
            if tree['modified'] or tree['untracked']:
                result['uncommitted'].append({**item, 'modified': tree['modified'], 'untracked': tree['untracked']})
            continue
        if not (os.path.realpath(path) + os.sep).startswith(root):
            result['outside_root'].append(item)
        inside = os.path.realpath(path) + os.sep
        used = any((directory + os.sep).startswith(inside) for directory in busy)
        if used:
            result['in_use'].append(item)
        if tree['modified'] or tree['untracked']:
            result['uncommitted'].append({**item, 'modified': tree['modified'], 'untracked': tree['untracked']})
        elif not used and tree['branch'] not in protected and (
                tree['branch'] in merged if tree['branch'] else tree['head'] in merged_heads):
            result['removable'].append(item)
    return result


def read_clone(repository, clone, integration):
    """Branches of an independent clone that the repository has not merged into the integration line."""
    _, text = git(clone, 'for-each-ref', '--format=%(refname:short)%09%(objectname)', 'refs/heads', trust=True)
    undelivered = []
    for line in text.splitlines():
        name, head = line.split('\t')
        present = git(repository, 'cat-file', '-e', head + '^{commit}')[0] == 0
        if present and is_ancestor(repository, head, integration):
            continue
        undelivered.append({'name': name, 'head': head[:10],
                            'state': 'in the repository, not merged' if present else 'not in the repository'})
    _, status = git(clone, 'status', '--porcelain', trust=True)
    return {'path': str(clone), 'undelivered': undelivered, 'uncommitted': len(status.splitlines())}


def sweep(repository, *, trunk='main', integration='integration', clones=(), days=3, patches=False,
          worktree_root=None, now=None):
    repository = str(Path(repository).resolve())
    now = now or time.time()
    worktree_root = worktree_root or str(Path(repository).parent / '_demiflow' / 'worktrees')
    report = {'repository': repository, 'trunk': trunk, 'integration': integration}
    if git(repository, 'rev-parse', '-q', '--verify', f'refs/heads/{integration}')[0] != 0:
        report['error'] = f'no branch named {integration}'
        return report
    has_trunk = git(repository, 'rev-parse', '-q', '--verify', f'refs/heads/{trunk}')[0] == 0
    if has_trunk:
        behind = int(git(repository, 'rev-list', '--count', f'{trunk}..{integration}')[1] or 0)
        ahead = int(git(repository, 'rev-list', '--count', f'{integration}..{trunk}')[1] or 0)
        stamp = int(git(repository, 'log', '-1', '--format=%ct', trunk)[1] or 0)
        report['trunk_state'] = {'behind_integration': behind, 'not_in_integration': ahead,
                                 'days_since_commit': round((now - stamp) / 86400, 1)}
    else:
        report['trunk_state'] = {'missing': True}
    branches = read_branches(repository)
    protected = {trunk, integration}
    merged = {b['name'] for b in branches if is_ancestor(repository, b['head'], integration)}
    patch_merged = set()
    if patches:
        patch_merged = {b['name'] for b in branches
                        if b['name'] not in merged and patches_all_in(repository, b['name'], integration)}
    report['branches'] = classify_branches(branches, merged, protected=protected, now=now, days=days,
                                           patch_merged=patch_merged)
    trees = read_worktrees(repository)
    merged_heads = {t['head'] for t in trees if not t['branch'] and t['head'] and is_ancestor(repository, t['head'], integration)}
    report['worktrees'] = classify_worktrees(trees, merged | patch_merged, merged_heads, protected=protected,
                                             repository=repository, worktree_root=worktree_root,
                                             busy={os.path.realpath(d) for d in working_directories()})
    report['stash'] = git(repository, 'stash', 'list', '--format=%gd %ci %s')[1].splitlines()
    report['clones'] = [read_clone(repository, clone, integration) for clone in clones]
    report['counts'] = counts(report)
    return report


def counts(report):
    numbers = {f'branches.{key}': len(value) for key, value in report['branches'].items() if key != 'active'}
    numbers.update({f'worktrees.{key}': len(value) for key, value in report['worktrees'].items()})
    numbers['stash'] = len(report['stash'])
    numbers['clones.undelivered'] = sum(len(clone['undelivered']) for clone in report['clones'])
    numbers['trunk.behind_integration'] = report['trunk_state'].get('behind_integration', 0)
    return numbers


LABELS = (
    ('trunk.behind_integration', 'commits the trunk is behind the integration line'),
    ('branches.merged_not_deleted', 'branches already in the integration line, not deleted'),
    ('branches.patches_merged_not_deleted', 'branches whose patches are all in it under other ids, not deleted'),
    ('branches.stale', 'branches not merged and without a commit for longer than the limit'),
    ('worktrees.removable', 'worktrees whose content is in the integration line, clean, not removed'),
    ('worktrees.uncommitted', 'worktrees with uncommitted changes'),
    ('worktrees.in_use', 'worktrees a running process is working in'),
    ('worktrees.outside_root', 'worktrees outside the agreed directory'),
    ('worktrees.directory_missing', 'registered worktrees whose directory is gone'),
    ('stash', 'stash entries'),
    ('clones.undelivered', 'branches in independent clones that are not delivered'),
)


def describe(report, limit=12):
    if 'error' in report:
        return f"{report['repository']}: {report['error']}"
    lines = [f"{report['repository']}  trunk={report['trunk']}  integration={report['integration']}"]
    if report['trunk_state'].get('missing'):
        lines.append(f"no branch named {report['trunk']}")
    for key, label in LABELS:
        lines.append(f"{report['counts'][key]:5d}  {label}")
    for group in ('branches', 'worktrees'):
        for key, items in report[group].items():
            if key == 'active' or not items:
                continue
            lines.append(f'\n{group}.{key}:')
            for item in items[:limit]:
                lines.append('  ' + (item.get('name') or item['path'])
                             + (f"  ({item['modified']} modified, {item['untracked']} untracked)" if 'modified' in item else ''))
            if len(items) > limit:
                lines.append(f'  ... {len(items) - limit} more (see --json)')
    for clone in report['clones']:
        for item in clone['undelivered']:
            lines.append(f"\nclone {clone['path']}: {item['name']} {item['head']} {item['state']}")
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--repository', default='.')
    parser.add_argument('--trunk', default='main')
    parser.add_argument('--integration', default='integration')
    parser.add_argument('--clone', action='append', default=[])
    parser.add_argument('--days', type=float, default=3)
    parser.add_argument('--patches', action='store_true')
    parser.add_argument('--worktree-root')
    parser.add_argument('--json', metavar='OUT')
    parser.add_argument('--fail-on-findings', action='store_true')
    options = parser.parse_args(argv)
    code, top = git(options.repository, 'rev-parse', '--path-format=absolute', '--git-common-dir')
    if code:
        print(f'{options.repository} is not a git repository', file=sys.stderr)
        return 2
    repository = str(Path(top).parent) if Path(top).name == '.git' else top
    report = sweep(repository, trunk=options.trunk, integration=options.integration, clones=options.clone,
                   days=options.days, patches=options.patches, worktree_root=options.worktree_root)
    print(describe(report))
    if options.json:
        Path(options.json).write_text(json.dumps(report, indent=1, ensure_ascii=False) + '\n')
    if 'error' in report:
        return 2
    findings = {key: value for key, value in report['counts'].items() if key != 'worktrees.in_use'}
    return 1 if options.fail_on_findings and any(findings.values()) else 0


if __name__ == '__main__':
    sys.exit(main())
