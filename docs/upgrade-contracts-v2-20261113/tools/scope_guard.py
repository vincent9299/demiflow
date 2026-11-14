"""Reject semantic edits outside explicitly assigned Python symbols.

Seven byte-frozen files are checked separately. This guard ignores formatting
and comments, but protects other code, declarations and module imports.
"""
import ast
import subprocess


def outside_scope_changed(before, after, allowed):
    def protected(text):
        tree=ast.parse(text)
        def visit(body, prefix=''):
            result=[]
            for node in body:
                name=getattr(node,'name',None)
                qualified=(prefix+'.' if prefix else '')+name if name else None
                targets=[t.id for t in node.targets if isinstance(t,ast.Name)] if isinstance(node,ast.Assign) else []
                if qualified in allowed and isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):
                    result.append(ast.Expr(value=ast.Constant(value='allowed:'+qualified)))
                elif len(targets)==1 and ((prefix+'.' if prefix else '')+targets[0]) in allowed:
                    result.append(ast.Expr(value=ast.Constant(value='allowed-declaration:'+prefix+'.'+targets[0])))
                else:
                    if isinstance(node,ast.ClassDef):node.body=visit(node.body,qualified)
                    result.append(node)
            return result
        tree.body=visit(tree.body)
        return ast.dump(tree,include_attributes=False)
    return protected(before)!=protected(after)


def scope_mismatches(root, guard):
    errors=[]
    for file, symbols in guard['symbol_scopes'].items():
        try:
            before=subprocess.check_output(['git','show',guard['base_commit']+':'+file],cwd=root,text=True)
            after=(root/file).read_text()
            if outside_scope_changed(before,after,set(symbols)):errors.append(file)
        except (SyntaxError, OSError, subprocess.CalledProcessError):
            errors.append(file)
    return errors
