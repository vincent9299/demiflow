"""Run pytest only against an asserted checkout; evidence includes exact argv."""
import hashlib, json, os, pathlib, subprocess, sys, time
def main():
    root = pathlib.Path(sys.argv[1]).resolve()
    label = sys.argv[2]
    args = sys.argv[3:]
    evidence = pathlib.Path('/tmp/demiflow-gpt-integration-review-20261114')
    if root == pathlib.Path('/yzp/zhaozy/yangzepeng/0905/demiflow').resolve():
        raise SystemExit('The live main checkout is read-only; select an isolated worktree')
    os.chdir(root)
    os.environ['PYTHONPATH'] = str(root) + os.pathsep + str(root/'tests')
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    os.environ['FORGE_WORKBENCH_EVIDENCE'] = str(evidence/(label+'-browser'))
    sys.dont_write_bytecode = True
    sys.path[:0] = [str(root), str(root/'tests')]
    import demiflow
    assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(root), demiflow.__file__
    def git(*cmd):
        return subprocess.check_output(['git', *cmd], cwd=root, text=True).strip()
    record = dict(root=str(root), module=demiflow.__file__, executable=sys.executable,
                  pid=os.getpid(), argv=sys.argv, head=git('rev-parse','HEAD'), dirty=git('status','--porcelain'),
                  started=time.time(), pytest_args=args)
    record['source_sha256'] = {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted((root/'demiflow').rglob('*.py'))}
    (evidence/(label+'-provenance.json')).write_text(json.dumps(record,indent=2))
    print('PINNED_ROOT='+str(root), flush=True)
    import pytest
    code=pytest.main(['--basetemp='+str(evidence/(label+'-tmp')), '-o',
                     'cache_dir='+str(evidence/(label+'-cache')), *args])
    record.update(exit_code=int(code), ended=time.time())
    record['imported_modules']={k:v.__file__ for k,v in list(sys.modules.items())
        if k.startswith('demiflow') and getattr(v,'__file__',None)}
    assert all(pathlib.Path(p).resolve().is_relative_to(root) for p in record['imported_modules'].values())
    (evidence/(label+'-provenance.json')).write_text(json.dumps(record,indent=2))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
