"""One public terminal over the complete, shared Dataset graph.

Paths and model endpoints are explicit configuration. Construction reads only
the five bounded prompt definitions; business data is read by Dataset sources.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import yaml

from demiflow import data
from demiflow.operator_llm.parser import parse_prompt_pack

import refgen_operators as op


HERE = Path(__file__).resolve().parent
PROMPTS = {
    'quality': ('quality.yaml', 'image_quality'),
    'focus': ('focus.yaml', 'concept_reference_focus'),
    'author': ('author_v4_1.yaml', 'reference_evidence_author'),
    'finalize': ('author_v4_1.yaml', 'reference_evidence_author'),
    'concept': ('concept_score_v22.yaml', 'judge_visual_direct'),
    'alignment': ('alignment_score_v1.yaml', 'judge_instruction_alignment'),
}


def prompt_configuration(stage, models):
    """Bind a model profile without rewriting the copied prompt/template/schema."""
    if stage in PROMPTS:
        filename, name = PROMPTS[stage]
        raw = (HERE / 'prompts' / filename).read_bytes()
        sources = json.loads((HERE / 'prompt_sources.json').read_text())['copies']
        expected = next(item['sha256'] for item in sources if item['copy'] == 'prompts/' + filename)
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError('Copied prompt changed: ' + filename)
        definition = deepcopy(yaml.safe_load(raw)['prompts'][name])
        definition['model'] = {k: v for k, v in models[stage].items() if k != 'revision'}
    else:
        name = stage
        # The service owns the image model and its implementation. The case owns
        # only the visible instruction and explicit ordered image binding.
        definition = {'version': 'refgen-reference-case-image-1', 'response_format': 'image',
            'model': dict(models[stage]), 'template': '{{ instruction }}{{ images | image }}'}
    return name, parse_prompt_pack(json.dumps({'schema_version': 'demiflow_prompt_pack_v2', 'prompts': {name: definition}}))


def run(*, sources, output_root, models, execution_kind='fixture', max_concepts=4):
    """Build and execute one complete reference experiment.

    sources are three fixed Lance URI/version bindings; output_root is owned by
    this experiment. execution_kind is persisted in every pair/result. Real
    profiles are explicit caller input and are never claimed tested by fixture.
    """
    if execution_kind not in ('fixture', 'real') or type(max_concepts) is not int or not 1 <= max_concepts <= 4:
        raise ValueError('Declare fixture/real provenance and at most four concepts')
    required = set(PROMPTS) | {'synthesis', 'boogu'}
    if set(models) != required:
        raise ValueError('Every model stage needs an explicit profile')
    if any(not isinstance(model.get('revision'), str) or not model['revision'] for model in models.values()):
        raise ValueError('Every model stage requires an explicit revision fact')
    fixture_models = [(model.get('model') or model.get('name', '')).startswith('fixture-') for model in models.values()]
    if (execution_kind == 'fixture' and not all(fixture_models)) or (execution_kind == 'real' and any(fixture_models)):
        raise ValueError('Fixture/real provenance must agree with all declared model profiles')
    root = Path(output_root)
    if not root.is_absolute(): raise ValueError('output_root must be an absolute owned directory')
    if set(sources) != {'concepts', 'images', 'relations'} or any(
            type(source.get('version')) is not int or source['version'] < 1 for source in sources.values()):
        raise ValueError('Three fixed positive Lance versions are required')
    quality_name, quality_pack = prompt_configuration('quality', models)
    focus_name, focus_pack = prompt_configuration('focus', models)
    author_name, author_pack = prompt_configuration('author', models)
    finalize_name, finalize_pack = prompt_configuration('finalize', models)
    synthesis_name, synthesis_pack = prompt_configuration('synthesis', models)
    boogu_name, boogu_pack = prompt_configuration('boogu', models)
    concept_name, concept_pack = prompt_configuration('concept', models)
    alignment_name, alignment_pack = prompt_configuration('alignment', models)
    text_options = {'trust_env': False, 'timeout_s': 15,
        'max_request_bytes': 512 * 1024, 'max_response_bytes': 64 * 1024, 'max_event_bytes': 64 * 1024,
        'input_limits': {'max_images': 8, 'max_image_bytes': 64 * 1024, 'max_total_image_bytes': 512 * 1024}}
    image_options = {'object_store': str(root / 'objects'), 'image_encoding': 'preserve',
        'io_workers': 1, 'prepare_workers': 1, 'response_workers': 1,
        'limits': {'max_images': 1, 'max_image_bytes': 64 * 1024, 'max_text_bytes': 8192,
                   'max_pixels': 4_194_304, 'max_total_pixels': 4_194_304,
                   'max_request_bytes': 256 * 1024, 'max_response_bytes': 256 * 1024}}

    concepts = data.read_lance(**sources['concepts'], columns=['concept_id', 'concept', 'description'], batch_size=1)
    images = data.read_lance(**sources['images'], columns=['image_id', 'image'], batch_size=1)
    relations = data.read_lance(**sources['relations'], columns=['concept_id', 'image_id', 'relation_decision'], batch_size=1)
    related = relations.filter(op.relation_passes).join(images, on='image_id').join(concepts, on='concept_id')
    quality = (related.read_object(reference='image', output='pixels', root=str(root), max_bytes=64 * 1024,
                                  error_output='read_error', label='读取已有图片')
        .map(op.quality_input, label='准备独立质量审核')
        .map_prompt_async(quality_name, config=quality_pack, inputs={'payload': 'payload', 'images': 'images'},
            output='quality_answer', call_output='quality_call', error_output='quality_error', when=op.has_images,
            max_requests=max_concepts * 8, concurrency=1, queue_depth=1, options={**text_options, 'model_revision': models['quality']['revision']}, label='已有图质量筛选')
        .map(op.quality_result, label='核对质量结论与图片绑定'))
    eligible = quality.filter(op.keep_quality_evidence).reduce_by_key('concept_id', op.collect_candidates, initial=None)
    selected = (concepts.join(eligible, on='concept_id', how='left')
        .map(op.focus_input, label='保留无候选概念与有界候选组')
        .map_prompt_async(focus_name, config=focus_pack, inputs={'payload': 'payload', 'images': 'images'},
            output='focus_answer', call_output='focus_call', error_output='focus_error', when=op.has_images,
            max_requests=max_concepts, concurrency=1, queue_depth=1, options={**text_options, 'model_revision': models['focus']['revision']}, label='概念参考价值筛选')
        .map(op.focus_result, label='核对参考价值三项要求')
        .map(op.author_input, fn_kwargs={'execution_kind': execution_kind}, label='概念身份与已有图联合输入'))
    pairs = (selected.map_prompt_async(author_name, config=author_pack, inputs={'context': 'context', 'images': 'images'},
            output='author_answer', call_output='author_call', error_output='author_error', when=op.author_ready,
            max_requests=max_concepts, concurrency=1, queue_depth=1, options={**text_options, 'model_revision': models['author']['revision']}, label='联合出题 v4.1')
        .flat_map(op.authored_pairs))
    reused = pairs.filter(op.is_reuse)
    blocked = pairs.filter(op.is_blocked)
    synthetic = (pairs.filter(op.is_synthesis).map(op.synthesis_input, label='合成参考图计划')
        .map_prompt_async(synthesis_name, config=synthesis_pack, inputs={'instruction': 'instruction', 'images': 'images'},
            output='synthesized_image', call_output='synthesis_call', error_output='synthesis_error',
            max_requests=max_concepts * 2, concurrency=1, queue_depth=1, options=image_options, label='合成参考图')
        .map(op.generated_reference, label='绑定真实合成图片对象')
        .read_object(reference='reference_image', output='pixels', root=str(root), max_bytes=64 * 1024,
                     error_output='read_error', label='读取实际合成像素')
        .map(op.finalize_input, label='实际图与原始三档草案')
        .map_prompt_async(finalize_name, config=finalize_pack, inputs={'context': 'context', 'images': 'images'},
            output='finalize_answer', call_output='finalize_call', error_output='finalize_error', when=op.finalize_ready,
            max_requests=max_concepts * 2, concurrency=1, queue_depth=1, options={**text_options, 'model_revision': models['finalize']['revision']}, label='合成图看图定稿 v4.1')
        .map(op.finalized_pair, label='保留合成来源与四项实际看图检查'))
    delivery = (reused.union(synthetic, blocked).map(op.lock_pair, label='冻结题面与图片内容绑定')
        .save_lance(root / 'questions.lance', schema=op.PAIR_SCHEMA, key='pair_id', stage='fixed_questions',
                    mode='append', max_batch=2, queue_depth=1, flush_interval=0.05, output_ref='delivery',
                    max_rows=8, max_row_bytes=256 * 1024, max_index_bytes=1024 * 1024))
    answers = (delivery.flat_map(op.expand_arms)
        .read_object(reference='input_reference', output='reference_pixels', root=str(root), max_bytes=64 * 1024,
                     error_output='reference_read_error', label='读取有参组固定参考图')
        .map(op.boogu_input, fn_kwargs={'profile': models['boogu']}, label='相同题面与显式参考条件')
        .map_prompt_async(boogu_name, config=boogu_pack, inputs={'instruction': 'instruction', 'images': 'images'},
            output='generated_image', call_output='boogu_call', error_output='boogu_error', when=op.generation_ready,
            max_requests=max_concepts * 12, concurrency=1, queue_depth=1, options=image_options, label='Boogu 有参无参作答')
        .read_object(reference='generated_image', output='generated_pixels', root=str(root), max_bytes=64 * 1024,
                     error_output='generated_read_error', label='读取同一份固定作答图片')
        .map(op.scoring_input, label='两路评分共用题面与实际作答图'))
    concept_scores = (answers.map_prompt_async(concept_name, config=concept_pack,
            inputs={'concept': 'concept', 'description': 'description', 'instruction': 'instruction', 'generated_images': 'generated_images'},
            output='concept_answer', call_output='concept_call', error_output='concept_error', when=op.scoring_ready,
            max_requests=max_concepts * 12, concurrency=1, queue_depth=1, options={**text_options, 'model_revision': models['concept']['revision']}, label='概念呈现评分 v22')
        .map(op.score_result, fn_kwargs={'rubric': 'concept'}, label='概念评分与原始证据'))
    alignment_scores = (answers.map_prompt_async(alignment_name, config=alignment_pack,
            inputs={'instruction': 'instruction', 'generated_image': 'generated_images'},
            output='alignment_answer', call_output='alignment_call', error_output='alignment_error', when=op.scoring_ready,
            max_requests=max_concepts * 12, concurrency=1, queue_depth=1, options={**text_options, 'model_revision': models['alignment']['revision']}, label='文本遵循评分 v1')
        .map(op.score_result, fn_kwargs={'rubric': 'alignment'}, label='文本评分与原始十三项维度'))
    scores = concept_scores.union(alignment_scores)
    summaries = (scores.reduce_by_key(['concept_id', 'conditioning_mode', 'tier', 'rubric'], op.summary_fold, initial=None)
        .map(op.summary_result, label='汇总对照与完整失败分母'))
    # Sole action: no earlier count/take/materialize/write_lance. Shared author,
    # image generation and scoring prefixes are the same actual Dataset objects.
    return scores.union(summaries).write_lance(str(root / 'results.lance'), mode='create',
                                             schema=op.RESULT_SCHEMA, return_receipt=True)
