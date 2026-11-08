"""Saved annotation predicates from the one existing event ledger.

The controller derives a bounded ID selection before native Case pagination.
Only server-owned principal/source/target/definition/cut facts enter that set.
"""
import sqlite3

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.records import Records

FORMAT = 'demiflow_case_annotation_rows_v1'
SQL_TICKS = 2048  # One tick is 1000 SQLite VM instructions.


def validate(value, source, descriptor):
    c.fields(value, {'variant','field','definitions','match'}, {'option'}, label='saved annotation filter')
    if source['case_key']['kind']!='row_id':
        c.reject('Saved annotation filtering requires this reader\'s fixed row-ID source',code='operation_rejected')
    Records._target(source, dict(source=source['digest'],case_key='0',variant=value['variant'],field=value['field']))
    if value['variant'] is not None and value['field'] is not None:
        c.reject('A registered scheme filter cannot use a field scope', code='conflict')
    definitions=c.sequence(value['definitions'],'annotation filter definitions',16,minimum=1)
    for definition in definitions:c.sha(definition,'annotation filter question')
    known={c.digest(item):item for item in descriptor['questions']}
    if len(set(definitions))!=len(definitions) or not set(definitions)<=set(known):
        c.reject('Annotation filter must use distinct current-view question definitions',code='conflict')
    c.one_of(value['match'],{'unanswered','answered_any','answered_all','option'},'annotation filter match')
    if value['match']=='option':
        option=value.get('option');c.fields(option,{'definition','value'},label='annotation option filter')
        c.sha(option['definition']);c.identity(option['value'],'question option')
        question=known.get(option['definition'])
        if (option['definition'] not in definitions or question is None or question['type'] not in {'choice','multi'}
                or option['value'] not in {item['id'] for item in question['options']}):
            c.reject('Annotation filter option is outside its exact question definition',code='conflict')
    elif 'option' in value:c.reject('Only an option filter accepts an option')
    c.encoded(value,maximum=4096)
    return value


def principal_scope(access):
    return c.digest(dict(project=access.project,principal=access.principal))


def rows_specification(value):
    c.fields(value,{'format','mode','keys','predicate_digest','annotation_watermark'},label='server annotation row selection')
    if value['format']!=FORMAT:c.reject('Unknown annotation selection format')
    c.one_of(value['mode'],{'include','exclude'},'annotation row selection mode')
    c.sha(value['predicate_digest']);c.integer(value['annotation_watermark'],'annotation selection cut',2**63-1)
    keys=c.sequence(value['keys'],'annotation row selection',1000)
    for key in keys:
        c.text(key,'annotation Case row key',20)
        if not key.isascii() or not key.isdecimal() or str(int(key))!=key or int(key)>=2**64:
            c.reject('Annotation selection must contain exact canonical uint64 keys')
    if keys!=sorted(set(keys),key=lambda item:(len(item),item)):
        c.reject('Annotation selection keys must be distinct and canonical')
    c.encoded(value,maximum=24*1024)
    return value


def selection(records,access,source,value,descriptor,*,watermark=None):
    value=validate(value,source,descriptor);access.check()
    with records.database.locked() as db:
        records._checked_target(db,access,source,dict(source=source['digest'],case_key='0',variant=value['variant'],field=value['field']))
        current=db.execute('SELECT coalesce(max(sequence),0) FROM annotation_events WHERE project=?',(access.project,)).fetchone()[0]
        if watermark is None:watermark=current
        c.integer(watermark,'annotation filter watermark',current)
        definitions=value['definitions'];placeholders=','.join('?' for _ in definitions)
        params=[access.project,source['digest'],value['variant'] or '',value['field'] or '',access.principal,watermark,*definitions]
        where="e.state='answered'";having=''
        if value['match']=='answered_all':having=' HAVING count(*)=?';params.append(len(definitions))
        elif value['match']=='option':
            option=value['option'];question=next(item for item in descriptor['questions'] if c.digest(item)==option['definition'])
            where+=' AND e.definition=?';params.append(option['definition'])
            if question['type']=='choice':where+=' AND CAST(e.value AS TEXT)=?';params.append(c.encoded(option['value']).decode())
            else:
                where+=" AND EXISTS(SELECT 1 FROM json_each(CAST(e.value AS TEXT)) j WHERE j.type='text' AND j.value=?)"
                params.append(option['value'])
        remaining=SQL_TICKS
        def budget():
            nonlocal remaining
            remaining-=1
            return int(remaining<=0)
        db.set_progress_handler(budget,1000)
        try:
            rows=db.execute('''WITH latest AS (
                SELECT case_key,definition,max(sequence) AS sequence FROM annotation_events
                WHERE project=? AND source=? AND variant=? AND field=? AND principal=? AND sequence<=?
                AND definition IN ('''+placeholders+''') GROUP BY case_key,definition)
                SELECT e.case_key FROM latest h JOIN annotation_events e ON e.sequence=h.sequence
                WHERE '''+where+' GROUP BY e.case_key'+having+' ORDER BY length(e.case_key),e.case_key LIMIT 1001',params).fetchall()
        except sqlite3.OperationalError:
            if remaining<=0:c.reject('Annotation filter exceeds its bounded SQL work budget',code='resource_limit')
            raise
        finally:db.set_progress_handler(None,0)
        if len(rows)>1000:c.reject('Annotation filter exceeds its 1000-key selection budget',code='resource_limit')
        keys=[row[0] for row in rows]
        scope=principal_scope(access)
        result=dict(format=FORMAT,mode='exclude' if value['match']=='unanswered' else 'include',keys=keys,
            predicate_digest=c.digest(dict(source=source['digest'],principal=scope,filter=value)),annotation_watermark=watermark)
        rows_specification(result);access.check()
        return result,dict(filter=value,principal_scope=scope,annotation_watermark=watermark,selection_digest=c.digest(result))
