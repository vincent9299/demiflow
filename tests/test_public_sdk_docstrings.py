"""Merge-gate ratchet enforcing docstrings on public DemiFlow SDK operators and contracts.

Every public symbol exported by ``demiflow.forge.api_reference.MODULES`` must carry an
explicit docstring explaining its contract (never empty and never Python's auto-generated
``ClassName(field=...)`` dataclass signature placeholder). Existing undocumented legacy
symbols are frozen in ``LEGACY_MISSING_DOCSTRINGS`` on a shrink-only basis so new public
APIs can never merge without documentation.
"""
from demiflow.forge.api_reference import api_markdown, build_reference, select_apis


# Shrink-only baseline of pre-existing public symbols without explicit docstrings.
# Never add new entries here; write a real docstring on the symbol instead.
# When you backfill a docstring on a listed symbol, delete it from this set.
LEGACY_MISSING_DOCSTRINGS = frozenset({
    'demiflow.data:BlockMetadata',
    'demiflow.data:ReadTask',
    'demiflow.data:WriteResult',
    'demiflow.data:add_lance_columns',
    'demiflow.data:ensure_lance_vector_index',
    'demiflow.execution:BundlePackages',
    'demiflow.execution:PipelineBackend.run',
    'demiflow.execution:PipelineBundleRef',
    'demiflow.execution:PipelineBundleRef.load',
    'demiflow.execution:PipelineBundleRef.verify',
    'demiflow.execution:PipelineExecutionEnvironment',
    'demiflow.execution:PipelineExecutionEnvironment.declaration_id',
    'demiflow.execution:PipelineExecutionEnvironment.verify_artifacts',
    'demiflow.execution:PipelineExecutionTarget',
    'demiflow.execution:PipelineExecutionTarget.from_mapping',
    'demiflow.execution:PipelineExecutionTarget.to_dict',
    'demiflow.execution:PipelineRunObservation',
    'demiflow.execution:PipelineRunObservation.to_dict',
    'demiflow.execution:PipelineRunReadiness',
    'demiflow.execution:PipelineRunRequest',
    'demiflow.execution:PipelineRunResult',
    'demiflow.execution:PlatformRuntimeIdentity',
    'demiflow.execution:PlatformRuntimeIdentity.from_mapping',
    'demiflow.execution:PlatformRuntimeIdentity.to_dict',
    'demiflow.execution:RunContext.action',
    'demiflow.execution:RunContext.allocate_artifact',
    'demiflow.execution:RunContext.check_stop',
    'demiflow.execution:RunContext.quarantine_worker',
    'demiflow.execution:RunContext.release_temporaries',
    'demiflow.execution:RunContext.reserve',
    'demiflow.execution:RunContext.reserve_artifact_bytes',
    'demiflow.execution:RunContext.resume',
    'demiflow.execution:RunContext.start',
    'demiflow.execution:RunRef',
    'demiflow.execution:SourcePosition.to_dict',
    'demiflow.execution:backend_for',
    'demiflow.execution:current_run',
    'demiflow.execution:inspect_pipeline_run_readiness',
    'demiflow.execution:load_bundle_packages',
    'demiflow.objects:LocalObjectStore.put',
    'demiflow.objects:LocalObjectStore.recovery_identity',
    'demiflow.objects:LocalObjectStore.reference',
    'demiflow.objects:ObjectRef',
    'demiflow.objects:ObjectRef.to_dict',
    'demiflow.objects:ReadLocalObject.restore_state',
    'demiflow.objects:ReadLocalObject.snapshot_state',
    'demiflow.operator_llm:CompiledTemplate',
    'demiflow.operator_llm:ImagePart',
    'demiflow.operator_llm:ImageValue',
    'demiflow.operator_llm:OperatorLLMRequest',
    'demiflow.operator_llm:OperatorLLMRequestUsage',
    'demiflow.operator_llm:OperatorLLMRequestUsage.from_value',
    'demiflow.operator_llm:OperatorLLMRequestUsage.to_dict',
    'demiflow.operator_llm:OperatorLLMResponse',
    'demiflow.operator_llm:OperatorLLMUsage',
    'demiflow.operator_llm:OperatorLLMUsage.to_dict',
    'demiflow.operator_llm:PromptContractDiagnostic',
    'demiflow.operator_llm:PromptDefinition',
    'demiflow.operator_llm:PromptModel',
    'demiflow.operator_llm:PromptPack',
    'demiflow.operator_llm:PromptPackInspection',
    'demiflow.operator_llm:PromptPlaceholder',
    'demiflow.operator_llm:TextPart',
    'demiflow.operator_llm:compile_template',
    'demiflow.operator_llm:inspect_prompt_pack',
    'demiflow.operator_llm:load_prompt_pack',
    'demiflow.operator_llm:parse_prompt_pack',
    'demiflow.operator_llm:render_template',
    'demiflow.operator_llm:required_environment',
    'demiflow.operator_llm:resolve_prompt',
})


def _missing_public_docstrings(reference):
    missing = set()
    for item in reference['catalog']['objects']:
        if item.get('origin', {}).get('kind') != 'platform':
            continue
        if item.get('signature', {}).get('status') != 'available':
            continue
        sym_id = item['id']
        leaf = sym_id.split(':')[-1].split('.')[-1]
        if leaf.startswith('_') or leaf.endswith('Error') or leaf.endswith('Warning'):
            continue
        doc = (item.get('documentation', {}).get('text') or '').strip()
        if not doc or doc.startswith(leaf + '('):
            missing.add(sym_id)
    return missing


def test_public_sdk_docstrings_ratchet():
    reference = build_reference()
    actual = _missing_public_docstrings(reference)
    newly_missing = sorted(actual - LEGACY_MISSING_DOCSTRINGS)
    assert not newly_missing, (
        'New public SDK symbols must carry a clear docstring (empty or auto-generated '
        'dataclass signature docstrings are forbidden): ' + ', '.join(newly_missing)
    )
    now_documented = sorted(LEGACY_MISSING_DOCSTRINGS - actual)
    assert not now_documented, (
        'Remove newly documented symbols from LEGACY_MISSING_DOCSTRINGS in '
        'tests/test_public_sdk_docstrings.py to tighten the ratchet: ' + ', '.join(now_documented)
    )


def test_training_contract_docstrings_and_api_source_excerpt():
    reference = build_reference()
    for symbol in (
        'demiflow.data:Dataset.fit',
        'demiflow.data:TrainingRecipe',
        'demiflow.data:PredictorSpec',
        'demiflow.data:FitRef',
        'demiflow.data:FitRef.select',
        'demiflow.data:ModelSelection',
        'demiflow.data:ModelSelection.predict',
    ):
        rows = select_apis(reference, api=symbol)
        assert len(rows) == 1
        doc = rows[0]['documentation'].strip()
        leaf = symbol.split(':')[-1].split('.')[-1]
        assert len(doc) >= 40 and not doc.startswith(leaf + '('), (symbol, doc)

    # Explicit --api queries on contract classes and terse/legacy symbols include a bounded source excerpt.
    recipe_row = select_apis(reference, api='demiflow.data:TrainingRecipe')[0]
    assert 'class TrainingRecipe' in recipe_row.get('source_excerpt', '')
    assert 'save_steps' in recipe_row['source_excerpt']
    assert '```python' in api_markdown([recipe_row])

    pred_row = select_apis(reference, api='demiflow.data:PredictorSpec')[0]
    assert 'model["snapshot"]' in pred_row['documentation']
    assert 'model["step"]' in pred_row['documentation']

    legacy_row = select_apis(reference, api='demiflow.objects:ObjectRef')[0]
    assert 'class ObjectRef' in legacy_row.get('source_excerpt', '')

    # Bulk essential SDK listing stays compact without source excerpts.
    essential_rows = select_apis(reference, api=None)
    assert all('source_excerpt' not in row for row in essential_rows)
