"""Load explicitly tagged notebook code; untagged cells are never executed."""
from pathlib import Path
from demiflow.execution.artifacts import read, digest

def pipeline_cells(notebook, tag="pipeline"):
    return ["".join(c["source"]) for c in read(notebook)["cells"]
            if c["cell_type"] == "code" and tag in c.get("metadata", {}).get("tags", [])]

def load_pipeline(notebook, scope_name, name="run_pipeline"):
    scope = {"__name__": scope_name}
    for source in pipeline_cells(notebook):
        exec(compile(source, str(notebook), "exec"), scope)
    if name not in scope:
        raise ValueError(f'{notebook} has no pipeline-tagged cell defining {name}')
    return scope[name]

def graph_version(notebook):
    return digest(pipeline_cells(notebook))

def default_notebook(module_file):
    return Path(module_file).with_name("debug.ipynb")


import copy
import json

def display_json(value):
    # Only remove duplicated base64 from JSON: every real pixel is displayed below.
    value = copy.deepcopy(value)
    def redact(obj):
        if isinstance(obj, dict):
            for key, val in obj.items():
                if key == "url" and isinstance(val, str) and val.startswith("data:image/"):
                    obj[key] = "[实际像素见同案例图片；完整data URL保存在请求JSON]"
                else:
                    redact(val)
        elif isinstance(obj, list):
            for val in obj:
                redact(val)
    redact(value)
    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"

def md(text):
    from IPython.display import display, Markdown
    display(Markdown(text))

def table(headers, records):
    def cell(value):
        return str(value if value is not None else '—').replace('|', r'\|').replace('\n', '；')
    lines = ['| ' + ' | '.join(map(cell, headers)) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |']
    lines += ['| ' + ' | '.join(map(cell, row)) + ' |' for row in records]
    md('\n'.join(lines))
