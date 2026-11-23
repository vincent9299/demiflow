"""Recovery sdk v3 implementation."""
from __future__ import annotations

import types
import json


def code_record(code):
    """Stable executable description; excludes interpreter interning/cache state."""
    def constant(value):
        if isinstance(value,types.CodeType):return {'code':code_record(value)}
        if isinstance(value,tuple):return {'tuple':[constant(v) for v in value]}
        if isinstance(value,frozenset):return {'frozenset':sorted((constant(v) for v in value),key=lambda v:json.dumps(v,sort_keys=True))}
        return {'type':type(value).__name__,'value':repr(value)}
    return {'bytecode':code.co_code.hex(),'constants':[constant(c) for c in code.co_consts],
            'names':list(code.co_names),'varnames':list(code.co_varnames),
            'freevars':list(code.co_freevars),'cellvars':list(code.co_cellvars),
            'argcount':code.co_argcount,'posonlyargcount':code.co_posonlyargcount,
            'kwonlyargcount':code.co_kwonlyargcount,'flags':code.co_flags,
            'stacksize':code.co_stacksize,'exceptiontable':code.co_exceptiontable.hex()}
