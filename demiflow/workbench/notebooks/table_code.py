"""Translate old generated Python calls using only exact catalog matches."""
import ast
import json
import re
import os
import stat
import tempfile

from demiflow.forge.io import no_symlinks


def named_source(source, names):
    if 'project_sample' not in source or 'expected=' not in source:
        return source
    try:
        code = ast.parse(source)
    except (SyntaxError, ValueError, RecursionError):
        return source
    if not any(isinstance(n, ast.ImportFrom) and n.module == 'demiflow.workbench.notebooks.data'
               and any(i.name == 'project_sample' and i.asname is None for i in n.names)
               for n in code.body):
        return source
    original = source.encode('utf-8')
    starts = [0]
    for line in original.splitlines(keepends=True):
        starts.append(starts[-1]+len(line))
    replacements = []
    for call in ast.walk(code):
        if not (isinstance(call,ast.Call) and isinstance(call.func,ast.Name) and call.func.id=='project_sample'
                and len(call.args)==1 and isinstance(call.args[0],ast.Constant)):
            continue
        key = call.args[0].value
        if not isinstance(key,str) or not re.fullmatch('[dl][a-f0-9]{24}',key):
            continue
        args = {kw.arg:kw.value for kw in call.keywords}
        if not {'expected','binding','version'} <= args.keys() or not args.keys() <= {'expected','binding','version','columns','offset','limit'}:
            continue
        if not all(isinstance(args[k],ast.Constant) and isinstance(args[k].value,str) and re.fullmatch('[a-f0-9]{64}',args[k].value)
                   for k in ('expected','binding')):
            continue
        path = names().get(key)
        if not path:
            continue
        keep = [ast.get_source_segment(source,kw) for kw in call.keywords if kw.arg not in ('expected','binding')]
        new = 'project_sample('+', '.join([json.dumps(path,ensure_ascii=False), *keep])+')'
        replacements.append((starts[call.lineno-1]+call.col_offset,starts[call.end_lineno-1]+call.end_col_offset,new.encode()))
    for start,end,new in sorted(replacements,reverse=True):
        original=original[:start]+new+original[end:]
    result = original.decode()
    # Translate the complete insert-code template to the same public call used
    # for new cards. Leave custom imports and surrounding user code alone.
    if replacements and len(code.body) == 3 and result.startswith(
            'from demiflow.workbench.notebooks.data import project_sample\n\ndf = project_sample('
    ) and result.rstrip().endswith(')\ndf'):
        result = result.replace('import project_sample\n\ndf = project_sample(', 'import sample\n\ndf = sample(', 1)
    return result


def migrate_readers(root, directory, paths):
    # Before starting this project's native server, rewrite only recognized
    # calls. A user-edited cell, every output and the execution counts survive.
    from demiflow.workbench.projects import discovery
    from demiflow.workbench.notebooks import dictionary
    cache = None
    def names():
        nonlocal cache
        if cache is None:
            rows = discovery.discover(root)['tables'] + dictionary.discover(root)['tables']
            cache = {row['id']:row['path'] for row in rows if row['status']=='available'}
        return cache
    for name in paths:
        path = no_symlinks(directory/name)
        original = path.stat()
        if original.st_size > 16*1024**2 or not os.access(path, os.W_OK) or original.st_nlink != 1:
            continue
        raw = path.read_bytes()
        if b'project_sample' not in raw or b'expected=' not in raw:
            continue
        try:
            book = json.loads(raw)
        except (ValueError, RecursionError):
            continue
        if not isinstance(book,dict) or not isinstance(book.get('cells'),list):
            continue
        changed = False
        for cell in book['cells']:
            if not isinstance(cell,dict) or cell.get('cell_type')!='code':
                continue
            value = cell.get('source','')
            if not (isinstance(value,str) or isinstance(value,list) and all(isinstance(v,str) for v in value)):
                continue
            source = ''.join(value)
            new = named_source(source,names)
            if new != source:
                cell['source'] = new.splitlines(True) if isinstance(value,list) else new
                changed = True
        if changed:
            with tempfile.NamedTemporaryFile(dir=directory, prefix='.reader-', delete=False) as out:
                temp = out.name
                try:
                    out.write((json.dumps(book,ensure_ascii=False,indent=1)+'\n').encode())
                    out.flush()
                    if (os.fstat(out.fileno()).st_uid,os.fstat(out.fileno()).st_gid) != (original.st_uid,original.st_gid):
                        os.fchown(out.fileno(),original.st_uid,original.st_gid)
                    os.fchmod(out.fileno(),stat.S_IMODE(original.st_mode))
                    os.fsync(out.fileno())
                    if os.path.samestat(original,path.stat()) and path.read_bytes()==raw:
                        os.replace(temp,path)
                finally:
                    if os.path.exists(temp):
                        os.unlink(temp)
