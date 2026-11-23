"""Pin the compile <-> runtime contract as it exists today.

The facts about one operator kind are spread over the registry, the compiler's
trait tables and branches, the runtime's kind sets and the SDK entry points.
``compile_contract_inventory`` reads them from source into one view. These
tests keep that view true and name the places where the tables disagree, so a
change to any of them is a visible, deliberate edit rather than drift.

Nothing here changes behaviour, and no table is duplicated: the inventory is
derived from the engine's own source every time.
"""
import ast

import compile_contract_inventory as inventory

TABLES = inventory.tables()
PREDICATES = inventory.predicates(TABLES)
DIFFERENCES = inventory.differences(TABLES)
ENTRIES = inventory.entries()


def test_snapshot_is_current():
    stored = inventory.SNAPSHOT.read_text(encoding='utf-8')
    assert inventory.render(inventory.inventory()) == stored, (
        'The compile/runtime contract changed. If that is intended, regenerate the snapshot with '
        '`python tests/compile_contract_inventory.py --write` and review its diff.')


def test_native_dispatch_includes_only_called_type_predicates(monkeypatch):
    original_source = inventory.source
    def without_search_call(relative):
        source = original_source(relative)
        if relative == inventory.NATIVE_DECLARATION:
            return source.replace('if is_native_search_actor(actor):', 'if False:')
        return source

    key = 'execution.native_model_declaration'
    assert 'SearchWeb' in inventory.class_dispatch()[key]
    monkeypatch.setattr(inventory, 'source', without_search_call)
    assert 'SearchWeb' not in inventory.class_dispatch()[key]


def test_prompt_dispatch_is_derived_from_each_use_of_the_domain_predicate(monkeypatch):
    original_source = inventory.source
    def without_prompt_check(relative):
        text = original_source(relative)
        if relative in (inventory.NATIVE_DECLARATION, inventory.PLAN_FACTS):
            return text.replace('is_native_prompt_actor(', 'unrelated_check(')
        return text

    owners = ('execution.native_model_declaration', 'execution.plan_facts')
    before = inventory.class_dispatch()
    assert all('PromptActor' in before[owner] for owner in owners)
    monkeypatch.setattr(inventory, 'source', without_prompt_check)
    after = inventory.class_dispatch()
    assert all('PromptActor' not in after[owner] for owner in owners)


def test_recorded_runtime_decisions_still_match_their_use_sites():
    assert inventory.missing_predicate_snippets() == [], (
        'A runtime use site changed. Revisit the decision recorded in compile_contract_inventory.PREDICATES.')


def test_every_registered_operator_declares_traits_and_ports():
    assert DIFFERENCES['registered_operators_without_traits'] == set()
    assert DIFFERENCES['traits_for_unregistered_operators'] == set()
    assert all(definition['inputs'] and definition['inputs'][0] == 'main'
               for definition in TABLES['registry.operators'].values())
    assert set(TABLES['compiler.traits']).isdisjoint(TABLES['compiler.native_traits'])


def test_registry_and_runtime_disagree_only_in_these_known_ways():
    # Registered as operators, yet the runtime kernel factory does not admit them.
    # These are the names a direct NativeOperation would use; the SDK reaches the
    # same features through typed operations instead (see the entries test).
    assert DIFFERENCES['registered_operators_not_admitted_by_runtime'] == {
        'PredictOp',  # lowered to one model-bound MapBatchesOp per selected step
        'decode_document', 'deduplicate', 'encode_image', 'group_batches', 'lance_lookup',
        'pack_prompt_inputs', 'parse_wikitext', 'queue_ack', 'queue_produce', 'raw_fetch',
        'read_documents', 'read_local_object', 'register_documents', 'register_documents_batch',
        'save_lance', 'search_vectors', 'search_web', 'stream_prepend'}
    # Kernel kinds that only a model lowering produces; no registry carries them.
    assert DIFFERENCES['runtime_kinds_not_registered_anywhere'] == {'document', 'fetch'}
    # Relational kernels are registered as sources, the aggregate kernel as a terminal.
    assert DIFFERENCES['runtime_kinds_registered_as_sources'] == {'exclude_keys', 'groups', 'join', 'reduce', 'union'}
    assert DIFFERENCES['runtime_kinds_registered_as_terminals'] == {'aggregate'}
    # The runtime sets are not a partition: these kinds are batch-shaped and service-bound.
    assert DIFFERENCES['runtime_kinds_in_several_sets'] == {
        'EmbeddingMapOp': ['batch', 'model'], 'RegisterDocumentsBatchOp': ['batch', 'model'],
        'SaveLanceOp': ['batch', 'model']}
    # Same name, different meaning: global input ordering (compiler) vs. the ordering kernel (runtime).
    assert DIFFERENCES['ordering_kinds_runtime_minus_compiler'] == {'RepartitionOp'}


def test_each_admitted_kind_is_pushed_in_at_most_one_way():
    admitted = [kind for kind, row in PREDICATES.items() if row['admitted']]
    assert len(admitted) == 40
    for kind in admitted:
        row = PREDICATES[kind]
        assert row['row_call'] + row['batch_call'] + row['stream_group'] <= 1, kind
    # Finite-only kernels never take an incremental row or batch call.
    assert sorted(kind for kind in admitted if not (
        PREDICATES[kind]['row_call'] or PREDICATES[kind]['batch_call'] or PREDICATES[kind]['stream_group'])) == [
        'RandomShuffleOp', 'RandomizeBlockOrderOp', 'RepartitionOp', 'SortOp', 'aggregate',
        'exclude_keys', 'groups', 'join', 'reduce', 'union']
    # A batch call always has an instance; task scope only applies to row calls.
    assert all(PREDICATES[kind]['needs_instance'] for kind in admitted if PREDICATES[kind]['batch_call'])
    assert all(PREDICATES[kind]['row_call'] for kind in admitted if PREDICATES[kind]['task_scoped'])


def test_unknown_kinds_satisfy_no_decision():
    for kind in inventory.UNKNOWN_KINDS:
        assert not any(PREDICATES[kind].values()), kind


def test_sdk_native_entries_only_preflight_their_declaration():
    # Every SDK entry builds a declaration to validate its arguments and then
    # discards it; the plan carries a typed operation. Using the result would be
    # a new entry path with its own capability, not a refactor.
    assert ENTRIES['preflight_result_used'] == []
    assert len(ENTRIES['preflight_result_discarded']) == 23
    registered = set(TABLES['registry.operators'])
    for method, name in ENTRIES['preflight_result_discarded']:
        assert ENTRIES['aliases'].get(name, name) in registered or name in ENTRIES['names_exempt_from_registry'], (
            method, name)
    assert set(ENTRIES['aliases'].values()) <= registered


def test_compilation_reads_nothing_from_its_surroundings():
    functions = sorted({item.split(': ', 1)[0] for item in inventory.implicit_inputs()})
    assert functions == []
    # A new planning module must not gain run-state reads outside the inventoried stages.
    planning = inventory.PACKAGE / 'planning'
    forbidden = {'journal', 'execution_commits', 'source_binding'}
    violations = [(path.name, node.lineno, node.attr)
                  for path in sorted(planning.rglob('*.py'))
                  for node in ast.walk(ast.parse(path.read_text(encoding='utf-8')))
                  if isinstance(node, ast.Attribute) and node.attr in forbidden]
    assert violations == []


def test_one_function_resolves_the_compiler_inputs():
    """Executor defaults and report bindings are read in one module and handed over explicitly."""
    reads = inventory.implicit_inputs((inventory.INPUT_RESOLVER,))
    assert sorted({item.split(': ', 1)[0] for item in reads}) == [
        'execution/compiler_support/dataflow_compile_inputs.py::compile_inputs',       # the action's executor, the report context
        'execution/compiler_support/dataflow_compile_inputs.py::executor_defaults',    # the three executor attributes
    ]
    compiler = inventory.source(inventory.COMPILER)
    assert compiler.count('compile_inputs(request)') == 1             # resolved once, at the capture entry


def test_source_tables_equal_the_imported_objects():
    """The source reader must agree with what the engine actually loads."""
    from demiflow.execution.kernels import batch_membership as dataflow_batch_membership
    from demiflow.execution.kernels import dataflow as kernels, operator
    from demiflow.execution.contracts.dataflow_kinds import TASK_KINDS
    from demiflow.planning import dataflow_compile as compiler, dataflow_registry as registry

    assert set(registry.operator_registry()) == set(TABLES['registry.operators'])
    assert set(registry.source_registry()) == set(TABLES['registry.sources'])
    assert set(registry.terminal_registry()) == set(TABLES['registry.terminals'])
    for kind, definition in registry.operator_registry().items():
        assert (definition.compiler_symbol, definition.kernel_symbol, tuple(definition.inputs)) == (
            TABLES['registry.operators'][kind]['compiler_symbol'],
            TABLES['registry.operators'][kind]['kernel_symbol'],
            tuple(TABLES['registry.operators'][kind]['inputs']))
    assert dict(compiler._TRAITS) == TABLES['compiler.traits']
    assert dict(compiler._NATIVE_TRAITS) == TABLES['compiler.native_traits']
    for name, actual in (
            ('compiler.finite_complete_kinds', compiler._FINITE_COMPLETE_KINDS),
            ('compiler.ordering_kinds', compiler._ORDERING_KINDS),
            ('runtime.kernel_kinds', kernels.KERNEL_KINDS), ('runtime.unary_kinds', kernels._UNARY_KINDS),
            ('runtime.batch_kinds', kernels._BATCH_KINDS), ('runtime.model_kinds', kernels._MODEL_KINDS),
            ('runtime.relational_kinds', kernels._RELATIONAL_KINDS), ('runtime.ordering_kinds', kernels._ORDERING_KINDS),
            ('runtime.aggregate_kinds', kernels._AGGREGATE_KINDS),
            ('runtime.stream_group_kinds', kernels._STREAM_GROUP_KINDS),
            ('runtime.instance_kinds', operator._INSTANCE_KINDS),
            ('runtime.batch_instance_kinds', operator._BATCH_INSTANCE_KINDS),
            ('runtime.task_kinds', TASK_KINDS),
            ('runtime.batch_membership_kinds', dataflow_batch_membership.KINDS)):
        assert set(actual) == set(TABLES[name]), name
