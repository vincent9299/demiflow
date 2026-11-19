"""Render the P12 review packet. Standard library only; no product imports."""
import ast
import hashlib
import html
import json
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
# Shared-review copies can regenerate against the same bound source worktree.
ROOT = Path(json.loads((HERE / 'design.json').read_text())['baseline']['worktree'])


def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True).strip()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def dump(name, value):
    (HERE / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def inventory(design):
    main, integration = (design['baseline'][k] for k in ('main', 'integration'))
    selected = [
        'projects/workspace.py', 'projects/index.py', 'projects/io.py', 'projects/checks.py',
        'projects/discovery.py', 'projects/registry.py', 'projects/watch.py',
        'projects/definitions.py', 'projects/documents.py', 'projects/flow_arguments.py',
        'runs/manager.py', 'runs/limits.py', 'runs/admission.py', 'runs/native_submission.py',
        'runs/worker.py', 'runs/cli.py', 'runs/requests.py',
        'worker.py', 'reader.py', 'host_partition.py', 'api.py', 'cli.py',
        'graph/model.py', 'graph/preview.py', 'graph/parameters.py',
        'analysis/catalog_versions.py', 'analysis/source_export.py',
        'analysis/media_source_export.py', 'analysis/web_sources.py',
        'analysis/catalog_profile.py', 'notebooks/data.py', 'notebooks/service.py',
        'conversations/inspection.py',
    ]
    paths = ['demiflow/workbench/' + p for p in selected] + [
        'demiflow/execution/host/inspection.py', 'demiflow/execution/host/control.py',
        'demiflow/lance/refs.py', 'demiflow/forge/platform_snapshot.py',
        'demiflow/forge/platform_environment.py', 'docs/architecture-master-intent-v1.md',
        'AGENTS.md', 'demiflow/execution/host/_surface.py',
        'tests/test_workbench_engine_boundary.py', 'docs/workbench-engine-surface-v3.json',
    ]
    records = []
    for relative in paths:
        raw = (ROOT / relative).read_bytes()
        baseline = subprocess.check_output(['git', '-C', str(ROOT), 'show', f'{main}:{relative}'])
        merged = subprocess.check_output(['git', '-C', str(ROOT), 'show', f'{integration}:{relative}'])
        assert raw == baseline, f'Source changed: {relative}'
        record = dict(path=relative, sha256=sha(raw), integration_sha256=sha(merged),
                      same_at_integration=(raw == merged))
        if relative.endswith('.py'):
            tree = ast.parse(raw)
            record['top_level_symbols'] = [dict(name=n.name, line=n.lineno)
                for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
            record['workbench_imports'] = sorted([
                dict(line=n.lineno, module=n.module, names=[x.name for x in n.names])
                for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
                and n.module and n.module.startswith('demiflow.workbench')], key=lambda x: x['line'])
        records.append(record)
    pattern = re.compile(r'\b(?:ws|api|runs|run_manager|index)\.(?:table_binding|resolve_arguments|'
        r'_inspect_flow|_report_status|verify_run_basis|attach_result|publish|synchronize|rebuild|'
        r'workspace_changes|inspect_flow|report_status|sample_table|project_sample|attach_table|'
        r'set_table_view|bind_audit_report|begin_change_scope|audit_check|audit_observe)\b|'
        r'from demiflow\.workbench\.(?:runs\.limits|projects\.workspace) import')
    candidates = []
    for path in sorted((ROOT / 'demiflow/workbench').rglob('*.py')):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if pattern.search(line):
                candidates.append(dict(path=path.relative_to(ROOT).as_posix(), line=number, text=line.strip()))
    workspace = ast.parse((ROOT / 'demiflow/workbench/projects/workspace.py').read_text())
    proxies = [dict(name=n.name, line=n.lineno) for n in workspace.body
        if isinstance(n, ast.FunctionDef) and any(isinstance(x, ast.ImportFrom)
            and x.module == 'demiflow.workbench.worker' for x in ast.walk(n))]
    assert len(proxies) == 18
    diff = git('diff', '--name-only', main, integration).splitlines()
    assert len(diff) == 24 and not any(p.startswith('demiflow/workbench/') for p in diff)
    assert subprocess.run(['git', '-C', str(ROOT), 'merge-base', '--is-ancestor', main, integration]).returncode == 0
    result = dict(format='p12_design_source_inventory_v2', main=main, integration=integration,
        source_root=str(ROOT), integration_descends_from_main=True, integration_diff=diff,
        workbench_diff=[], sources=records, direct_workspace_worker_proxies=proxies,
        callsite_candidates=candidates,
        callsite_note='搜索候选供逐项迁移；不作为动态调用图或全仓无环的证明。AST 符号与 import 含函数内延迟导入。',
        evidence_records=[
            '/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/integration-followups-20261119/CURRENT.json',
            '/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/regression-closure-20261119/CURRENT.json',
            design['baseline']['previous_draft'],
            design['baseline'].get('supersedes', {}).get('path'),
            design.get('review_response', {}).get('review_path'),
        ])
    dump('source-inventory.json', result)
    return result


def render(design, sources):
    esc = html.escape
    css = '''
    :root{font-family:system-ui,-apple-system,"Noto Sans CJK SC",sans-serif;color:#1b2938;background:#f4f6f8;line-height:1.75}
    *{box-sizing:border-box}body{margin:0}main{max-width:1160px;margin:auto;padding:36px 28px 80px}
    header,section{background:white;border:1px solid #dce3e9;border-radius:10px;padding:24px 28px;margin-bottom:22px}
    h1{font-size:28px;line-height:1.4;margin:0 0 14px}h2{font-size:21px;margin:0 0 20px;color:#174b6b}
    p{margin:12px 0}a{color:#075b9a;text-decoration:underline;text-underline-offset:3px}
    .badge{display:inline-block;border-radius:4px;background:#fff1cc;padding:4px 10px;font-weight:600}
    nav{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:5px 20px;margin-top:20px}
    .table{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:14px;margin:18px 0}
    th,td{border:1px solid #d9e0e6;padding:12px;vertical-align:top;text-align:left;min-width:145px}
    th{background:#edf3f7}tr:nth-child(even){background:#fafbfd}td:first-child{font-weight:600;width:19%}
    pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f0f4f7;border-left:4px solid #357a9e;padding:18px;font-size:14px;line-height:1.7}
    li{margin:12px 0}.meta{font-size:14px;color:#45586b;overflow-wrap:anywhere}details{margin:16px 0}
    summary{cursor:pointer;font-weight:600}code{overflow-wrap:anywhere}footer{font-size:13px;color:#536474}
    @media(max-width:650px){main{padding:12px}header,section{padding:16px}h1{font-size:23px}nav{grid-template-columns:1fr}th,td{min-width:160px}pre{font-size:12px}}
    @media print{body{background:white}main{max-width:none;padding:0}nav{display:none}section,header{border:0;padding:10px 0}h2{break-after:avoid}tr{break-inside:avoid}.table{overflow:visible}pre{font-size:11px}}
    '''
    output = [f'<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{esc(design["title"])}</title><style>{css}</style><body><main><header>',
        f'<h1>{esc(design["title"])}</h1><div class="badge">设计待审阅 · 尚未实施</div>',
        '<p>基于最新 main，核对已交付 integration；明确改前改后、职责、输入输出及失败行为。</p>',
        '<p>v2 获 Fable 有条件通过；v2.1 已补订 begin 拒绝语义、P4 消费者计划及模块分类。设计修订不等于实现验收。</p>',
        f'<p class="meta">main: {esc(design["baseline"]["main"])}<br>integration: {esc(design["baseline"]["integration"])}</p>',
        '<p><a href="design.json">原始方案 JSON</a> · <a href="source-inventory.json">源码与差异证据</a> · <a href="verification.json">文档核验记录</a> · <a href="review-request.txt">转交说明</a></p><nav>']
    for section in design['sections']:
        output.append(f'<a href="#{section["id"]}">{esc(section["title"])}</a>')
    output.append('</nav></header>')
    for section in design['sections']:
        output.append(f'<section id="{section["id"]}"><h2>{esc(section["title"])}</h2>')
        if 'table' in section:
            t = section['table']
            output.append('<div class="table"><table><thead><tr>' + ''.join(f'<th>{esc(x)}</th>' for x in t['columns']) + '</tr></thead><tbody>')
            for row in t['rows']:
                assert len(row) == len(t['columns'])
                output.append('<tr>' + ''.join(f'<td>{esc(x)}</td>' for x in row) + '</tr>')
            output.append('</tbody></table></div>')
        for key in ('code', 'diagram'):
            if key in section:
                output.append(f'<pre>{esc(section[key])}</pre>')
        for paragraph in section.get('paragraphs', []):
            output.append(f'<p>{esc(paragraph)}</p>')
        if 'items' in section:
            output.append('<ol>' + ''.join(f'<li>{esc(x)}</li>' for x in section['items']) + '</ol>')
        output.append('</section>')
    output.append('<section id="sources"><h2>附录：可核对的源码位置</h2><p>下列路径指向本稿隔离树；完整摘要和调用位置见 source-inventory.json。内容按固定 SHA 核验，不随 main 名称前进而重解释旧证据。</p>')
    for record in sources['sources']:
        absolute = str(ROOT / record['path'])
        output.append(f'<details><summary>{esc(record["path"])}</summary><p class="meta">SHA-256: {record["sha256"]}<br>与本稿 integration 相同: {record["same_at_integration"]}</p><p>')
        output.append(f'<a href="{esc(absolute)}">打开源码</a></p><p class="meta">')
        output.append(' · '.join(f'{esc(s["name"])}:{s["line"]}' for s in record.get('top_level_symbols', [])))
        output.append('</p></details>')
    output.append('</section><footer>本页由 design.json 生成；只核对设计与源码，不能作为产品测试、Fable 通过或发布证明。</footer></main></body></html>')
    (HERE / 'design.html').write_text('\n'.join(output) + '\n')


if __name__ == '__main__':
    data = json.loads((HERE / 'design.json').read_text())
    facts = inventory(data)
    render(data, facts)
    print(json.dumps(dict(sections=len(data['sections']), sources=len(facts['sources']),
        callsite_candidates=len(facts['callsite_candidates']),
        workspace_worker_proxies=len(facts['direct_workspace_worker_proxies']),
        integration_diff=len(facts['integration_diff'])), ensure_ascii=False))
