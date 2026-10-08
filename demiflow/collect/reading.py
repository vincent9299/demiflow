"""Native document reading: verified objects, complete blocks, bounded prompt fit.

Question text and bindings are supplied by the caller. Scores only prioritize
reading; they neither reject documents nor certify semantic relevance.
"""
from dataclasses import dataclass
import re
from .documents import read_document, canonical
from .session import bounded, isolated


@dataclass(frozen=True)
class PromptContext:
    prompt: object
    budget: object
    build_inputs: object  # pure (row, reading_result) -> native template variables


def terms(text):
    values = re.findall(r'[a-zA-Z0-9_]{2,}|[\u4e00-\u9fff]+', text.lower())
    return set(v for x in values for v in ([x]+[x[i:i+2] for i in range(len(x)-1)]
               if re.search('[\u4e00-\u9fff]', x) else [x]))


def ranges(indices):
    result = []
    for i in indices:
        if result and i == result[-1][1]+1: result[-1][1] = i
        else: result.append([i, i])
    return [f'b{a:06d}..b{b:06d}' for a, b in result]


def requested_blocks(request, document):
    blocks = document['blocks']; known = {b['block_id'] for b in blocks}
    selected = [bid for bid in request['block_ids'] if bid in known]
    section = request.get('section_id')
    if section:
        start = next((i for i, b in enumerate(blocks) if b['block_id'] == section and b['kind'] == 'heading'), None)
        if start is None: return selected
        level = blocks[start].get('heading_level') or len(blocks[start]['headings'])
        for block in blocks[start:]:
            if block['block_id'] != section and block['kind'] == 'heading' and (block.get('heading_level') or len(block['headings'])) <= level:
                break
            selected.append(block['block_id'])
    return list(dict.fromkeys(selected))


def select_blocks(spec, documents, counter, *, new_limit=None):
    # Optional ranking policy changes automatic admission only. Full source
    # blocks, retained evidence and explicit locators remain byte-for-byte.
    from .reading_contract import AUTOMATIC_SELECTION
    from demiflow.schema import validate_instance
    selection = spec.get('selection', {})
    validate_instance(selection, AUTOMATIC_SELECTION, label='reading selection')
    heading_weight = selection.get('heading_weight', 1)
    neighbors = selection.get('include_neighbors', True)
    excluded = set(selection.get('excluded_kinds', []))
    min_matches = selection.get('min_matches', 0)
    fallback = selection.get('fallback_blocks', 0)
    unit = getattr(counter, 'unit', 'tokens')
    new_key, total_key = 'new_' + unit, 'total_' + unit
    if any(type(spec.get(k)) is not int or spec[k]<0 for k in (new_key,total_key)):
        raise ValueError('Material budgets must be nonnegative integers')
    available, catalogs, receipts = {}, [], []
    for record, doc in documents:
        ref = record['document_ref']
        for block in doc['blocks']:
            eid = ref['sha256']+':'+block['block_id']
            available[eid] = {'evidence_id':eid, 'document_ref':ref, 'url':doc['source']['url'],
                'title':doc['source']['title'], 'block_id':block['block_id'], 'kind':block['kind'],
                'headings':block['headings'], 'text':block['text']}
    selected = [dict(s) for s in spec.get('retained', [])]
    materials = []
    for selection in selected:
        if selection['evidence_id'] not in available:
            raise ValueError('Previously supplied block is unavailable')
        materials.append({**available[selection['evidence_id']], 'bindings':selection['bindings']})
    old_tokens = counter.text(canonical(materials)) if materials else 0
    new_budget = min(spec[new_key], new_limit) if new_limit is not None else spec[new_key]
    total_budget = spec[total_key]
    ordered = []
    locators = {}
    for request in spec.get('requests', []):
        doc = next((d for r, d in documents if r['document_ref'] == request['document_ref']), None)
        ids = requested_blocks(request, doc) if doc else []
        known = {b['block_id'] for b in doc['blocks']} if doc else set()
        valid = set(request['block_ids']) <= known and (not request.get('section_id') or bool(doc and any(
            b['block_id'] == request['section_id'] and b['kind'] == 'heading' for b in doc['blocks'])))
        eids = [request['document_ref']['sha256']+':'+bid for bid in ids]
        locators[request['request_id']] = (eids, valid)
        ordered.extend((eid, request['bindings']) for eid in eids)
    # Rank within each document, then share reading across documents. Lexical
    # overlap must not give one language every slot and starve other sources.
    per_document, strengths = {}, {}
    for di, (record, doc) in enumerate(documents):
        if not record.get('eligible', True): continue
        candidates = []
        block_terms=[terms(' '.join(b['headings'])+' '+b['text']) for b in doc['blocks']]
        heading_terms = [terms(' '.join(b['headings'])) for b in doc['blocks']] if heading_weight > 1 else []
        eligible = [b for b in doc['blocks'] if b['kind'] not in excluded]
        for question in spec['questions']:
            if record.get('bindings') and question['id'] not in record['bindings']: continue
            needle = terms(question['text'])
            ranked = sorted((-(len(needle & block_terms[b['position']]) +
                              ((heading_weight-1)*len(needle & heading_terms[b['position']]) if heading_terms else 0)),
                             b['position']) for b in eligible
                            if len(needle & block_terms[b['position']]) >= min_matches)
            if not ranked and fallback:
                ranked = [(0, b['position']) for b in eligible[:fallback]]
            strengths[di] = min(strengths.get(di, 0), ranked[0][0] if ranked else 0)
            items, seen = [], set()
            for _, position in ranked:
                positions = (position, max(0, position-1), min(len(doc['blocks'])-1, position+1)) if neighbors else (position,)
                for pos in positions:
                    if doc['blocks'][pos]['kind'] in excluded:
                        continue
                    eid = record['document_ref']['sha256']+':'+doc['blocks'][pos]['block_id']
                    if eid not in seen:
                        items.append((eid, [question['id']]))
                        seen.add(eid)
            candidates.append(items)
        per_document[di] = [c[index] for index in range(max((len(c) for c in candidates), default=0))
                            for c in candidates if index < len(c)]
    doc_order = sorted(per_document, key=lambda di: (strengths.get(di, 0), di))
    explicit = list(ordered)
    automatic = [per_document[di][index]
                 for index in range(max((len(c) for c in per_document.values()), default=0))
                 for di in doc_order if index < len(per_document[di])]
    # Merge all bindings before admission, including explicit requests. Retained
    # blocks remain byte-for-byte the same, even if new bindings were discovered.
    bindings = {}
    for eid, ids in explicit + automatic:
        bindings[eid] = list(dict.fromkeys(bindings.get(eid, [])+ids))
    per_document={di:list(dict.fromkeys(eid for eid,_ in items)) for di,items in per_document.items()}
    known = {s['evidence_id'] for s in selected}
    current_tokens = old_tokens
    costs = {}
    def admit(eid, ceiling=None):
        nonlocal current_tokens
        if eid in known: return 0
        material = {**available[eid], 'bindings':bindings[eid]}
        remaining=min(total_budget-current_tokens, new_budget-(current_tokens-old_tokens))
        if remaining<=0 or (ceiling is not None and ceiling<=0): return 0
        if eid not in costs: costs[eid]=counter.text(canonical(material))
        if costs[eid] > min(remaining,ceiling if ceiling is not None else remaining)+8:
            return 0
        tokens = counter.text(canonical(materials+[material]))
        cost = tokens-current_tokens
        if tokens > total_budget or tokens-old_tokens > new_budget or (ceiling is not None and cost>ceiling): return 0
        materials.append(material); known.add(eid); current_tokens = tokens
        selected.append({'evidence_id':eid, 'document_ref':material['document_ref'],
                         'block_id':material['block_id'], 'bindings':bindings[eid]})
        return cost
    for eid, _ in explicit: admit(eid)
    remaining = max(0, min(total_budget-current_tokens, new_budget-(current_tokens-old_tokens)))
    share = remaining // max(1, len(doc_order))
    spent = {di:0 for di in doc_order}
    # Fair first pass. Oversized complete blocks are deferred, never truncated.
    for index in range(max((len(c) for c in per_document.values()), default=0)):
        for di in doc_order:
            if index < len(per_document[di]):
                spent[di] += admit(per_document[di][index], share-spent[di])
    # Reuse unspent shares. With room for only one block, strongest hit wins.
    for eid in dict.fromkeys(eid for eid,_ in automatic): admit(eid)
    for record, doc in documents:
        sha = record['document_ref']['sha256']
        headings = [{'block_id':b['block_id'], 'text':b['text']} for b in doc['blocks'] if b['kind'] == 'heading']
        read = [b['block_id'] for b in doc['blocks'] if sha+':'+b['block_id'] in known]
        catalogs.append({'document_ref':record['document_ref'], 'url':doc['source']['url'], 'title':doc['source']['title'],
            'block_count':len(doc['blocks']), 'selected_block_ids':read,
            'unread_ranges':ranges([b['position'] for b in doc['blocks'] if b['block_id'] not in set(read)]),
            'headings':headings[:32], 'heading_total':len(headings), 'status':'ok', 'reason':''})
    for request in spec.get('requests', []):
        eids, valid = locators[request['request_id']]
        complete = bool(eids) and valid and set(eids) <= known
        receipts.append({'request_id':request['request_id'], 'status':'read' if complete else ('partially_read' if eids else 'invalid_locator'),
                         'reason':'' if complete else 'invalid block/section locator or material ' + unit + ' budget'})
    return {'selected':selected, 'materials':materials, 'readings':catalogs, 'receipts':receipts,
            'material_' + unit:current_tokens, 'status':'context_budget' if old_tokens>total_budget else 'ok',
            'reason':'Retained material exceeds total material budget' if old_tokens>total_budget else ''}


def select_context(row, spec, documents, failed, context):
    unit = getattr(context.budget.counter, 'unit', 'tokens')
    metric = 'prompt_' + unit
    original = spec['new_' + unit]
    def attempt(allowance):
        out = select_blocks(spec, documents, context.budget.counter, new_limit=allowance)
        out['readings'] += failed
        if out['status']!='ok': return out
        if allowance < original:
            for reading in out['readings']:
                if reading['status'] == 'ok':
                    reading['reason'] = f'New material allowance reduced from {original} to {allowance} {unit} to fit full prompt; earlier material retained; other blocks remain unread.'
        out[metric] = context.budget.counter.prompt(context.prompt, context.build_inputs(row, out))
        return out
    full=attempt(original)
    if full['status']!='ok' or full[metric]<=context.budget.max_input: return full
    # Always test mandatory context separately. A change in payload projection
    # means reducing serialized material by N tokens need not reduce input by N.
    best=attempt(0)
    if best['status']!='ok': return best
    if best[metric]>context.budget.max_input:
        return {**best,'status':'context_budget',
                'reason':f"Complete input {best[metric]} exceeds {context.budget.max_input}; mandatory context retained"}
    low,high=1,original-1
    for _ in range(6):
        if low>high: break
        allowance=(low+high)//2
        out=attempt(allowance)
        if out['status']=='ok' and out[metric]<=context.budget.max_input:
            best=out;low=allowance+1
        else: high=allowance-1
    return best


class ReadDocuments:
    concurrency = 1
    def __init__(self, request, output, context, when, document_concurrency, timeout_s, max_bytes,
                 reuse_workers=0, reuse_worker_max_tasks=128):
        self.request, self.output, self.context, self.when = request, output, context, when
        self.document_concurrency, self.timeout_s, self.max_bytes = document_concurrency, timeout_s, max_bytes
        self.reuse_workers = reuse_workers
        self.reuse_worker_max_tasks = reuse_worker_max_tasks
        self.pool = None
        self.resources = [self] if reuse_workers else []

    async def astart(self):
        if self.reuse_workers:
            from functools import partial
            from ..execution.isolated_pool import IsolatedWorkerPool
            self.pool = IsolatedWorkerPool({
                'read': partial(read_document, max_bytes=self.max_bytes),
                'select': partial(select_context, context=self.context),
            }, workers=self.reuse_workers, max_tasks=self.reuse_worker_max_tasks)

    def snapshot_metrics(self):
        return self.pool.snapshot_metrics() if self.pool else {}

    async def aclose(self):
        if self.pool:
            await self.pool.aclose()

    async def __call__(self, row):
        if self.when is not None and not self.when(row): return row
        if self.pool is None:
            result = await read_documents(row[self.request], context=self.context, row=row,
                document_concurrency=self.document_concurrency, timeout_s=self.timeout_s, max_bytes=self.max_bytes)
        else:
            async def execute(name, *args):
                return await self.pool.run(name, *args, timeout_s=self.timeout_s)
            result = await _read_with_executor(row[self.request], row, self.document_concurrency, execute)
        return {**row, self.output:result}


async def read_documents(request, *, context, row=None, document_concurrency=2,
                         timeout_s=30, max_bytes=8*1024*1024):
    """Public single-row execution of Dataset.read_documents's exact contract.

    request is the value in the Dataset request column; result is the value
    written to its output column. No Dataset, actor graph or model is started.
    context and resource limits are supplied by the owning node/environment.
    CharacterBudget uses new_chars/total_chars and reports material_chars/
    prompt_chars. TokenBudget retains the existing token fields and behavior.
    The JSON argument contract is declared in reading_contract.py; agent callers
    pass the same request value, without an alternative ID/query interface.
    """
    async def execute(name, *args):
        if name == 'read':
            return await isolated(read_document, *args, max_bytes=max_bytes, timeout_s=timeout_s)
        return await isolated(select_context, *args, context=context, timeout_s=timeout_s)
    return await _read_with_executor(request, row or {}, document_concurrency, execute)


async def _read_with_executor(request, row, document_concurrency, execute):
    """Same reading/selection contract for one-shot and reusable isolation."""
    async def one(record):
        try:
            return record, await execute('read', record['document_ref'])
        except (ValueError, OSError, TimeoutError) as exc:
            return record, exc
    outcomes = await bounded(request['documents'], one, document_concurrency)
    readable = [(r, d) for r, d in outcomes if not isinstance(d, Exception)]
    failed = [{'document_ref':r['document_ref'], 'url':r['url'], 'title':'', 'block_count':0,
        'selected_block_ids':[], 'unread_ranges':[], 'headings':[], 'heading_total':0, 'status':'read_failed', 'reason':str(d)}
        for r, d in outcomes if isinstance(d, Exception)]
    try:
        return await execute('select', row, request, readable, failed)
    except (ValueError, TimeoutError) as exc:
        return {'status':'read_failed', 'reason':str(exc)}
