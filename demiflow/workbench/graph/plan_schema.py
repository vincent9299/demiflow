"""Read-only schema presentation; never scan data or infer a UDF's return."""
import json
from types import SimpleNamespace
from .plan_details import material, safe_json


def build_schema_details(compiled, *, bounds):
    from demiflow.execution.dataflow_schema import terminal_schema
    graph = compiled.graph
    incoming = {node.occurrence_id: [] for node in graph.nodes}
    for edge in graph.edges:
        incoming[edge.consumer].append(edge)
    schemas = {}
    # Reuse the engine's bounded metadata interpreter, not a new type inference
    # system. Preview has no physical source snapshots: unknown stays unknown.
    physical = SimpleNamespace(sources=())
    for node in graph.nodes:
        schema = terminal_schema(graph, physical, terminal=node.occurrence_id, bounds=bounds)
        if schema is not None:
            fields = [{'name': field.name, 'type': str(field.type)[:1024], 'nullable': field.nullable}
                      for field in schema][:256]
            schemas[node.occurrence_id] = dict(status='declared', basis='引擎声明及类型传播（未读取数据）',
                fields=fields, truncated=len(schema) > 256)
        else:
            schemas[node.occurrence_id] = dict(status='unknown', fields=[],
                reason='编译阶段没有完整的行 Schema；未执行数据，也不推测 Python 函数的返回字段。')
            # A projected column list is a name fact, not a type/schema proof.
            query = node.kernel.configuration.get('query', {})
            columns = query.get('fields', {}).get('columns') if hasattr(query, 'get') else None
            if type(columns) in (tuple, list) and all(type(c) is str for c in columns):
                schemas[node.occurrence_id] = dict(status='fields_only',
                    basis='读取调用声明的字段；字段类型和可空性尚未取得',
                    fields=[dict(name=c, type=None, nullable=None) for c in columns[:256]], truncated=len(columns) > 256)
    result = {}
    for node in graph.nodes:
        value = dict(inputs=[dict(port=edge.input_port, producer=edge.producer,
                                 schema=schemas[edge.producer]) for edge in incoming[node.occurrence_id]],
                     output=schemas[node.occurrence_id],
                     bindings=safe_json({k: node.kernel.configuration[k] for k in ('inputs', 'output', 'outputs')
                                         if k in node.kernel.configuration}),
                     note='这里是 Dataset 行的 Schema；模型响应 Schema 在 Prompt 中单列，不等于整行输出。')
        if node.kind == 'terminal':
            value['note'] += ' 终结动作显示数据通道的行结构，Python 返回值或提交回执另属 API 返回契约。'
        result[node.occurrence_id] = material(json.dumps(value, ensure_ascii=False, indent=2), language='json', metadata=value)
    return result
