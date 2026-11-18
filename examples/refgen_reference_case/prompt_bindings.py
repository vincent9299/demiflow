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
        definition = deepcopy(yaml.safe_load((HERE / 'prompts' / 'image.yaml').read_text())['prompts'][stage])
        definition['model'] = dict(models[stage])
    return name, parse_prompt_pack(json.dumps({'schema_version': 'demiflow_prompt_pack_v2', 'prompts': {name: definition}}))

