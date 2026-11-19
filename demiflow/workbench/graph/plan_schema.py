"""Render native schema facts without interpreting kernel configurations."""
import json
from .plan_details import material, safe_json, unavailable


def _schema(value):
    result = dict(value)
    if value['status'] == 'declared':
        result['basis'] = '引擎声明及类型传播（未读取数据）'
    elif value['status'] == 'fields_only':
        result['basis'] = '读取调用声明的字段；字段类型和可空性尚未取得'
    else:
        result['reason'] = '编译阶段没有完整的行 Schema；未执行数据，也不推测 Python 函数的返回字段。'
    return result


def build_schema_details(projected):
    result = {}
    for key, node in projected.items():
        schema = node.schema
        if schema.availability != 'available':
            result[key] = unavailable('Schema 材料不可用：' + str(schema.reason))
            continue
        try:
            value = dict(inputs=[dict(port=edge['port'], producer=edge['producer'], schema=_schema(edge['schema']))
                                 for edge in schema.inputs],
                         output=_schema(schema.output), bindings=safe_json(schema.bindings),
                         note='这里是 Dataset 行的 Schema；模型响应 Schema 在 Prompt 中单列，不等于整行输出。')
            if node.kind == 'terminal':
                value['note'] += ' 终结动作显示数据通道的行结构，Python 返回值或提交回执另属 API 返回契约。'
            result[key] = material(json.dumps(value, ensure_ascii=False, indent=2), language='json', metadata=value)
        except ValueError as exc:
            result[key] = unavailable(str(exc))
    return result
