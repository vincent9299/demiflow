"""Bounded loopback model transport and synthetic input tables.

This is not Boogu, a visual judge, or a quality measurement. It substitutes only
external model responses; the Dataset graph, HTTP clients, images, journals,
writers and source versions remain real. Never imported by the business flow.
"""
from contextlib import contextmanager, suppress
from collections import Counter
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import threading

import lance
from PIL import Image
import pyarrow as pa

import refgen_operators as op


def png(color):
    with Image.new('RGB', (4, 4), color) as image:
        with io.BytesIO() as out:
            image.save(out, format='PNG')
            return out.getvalue()


def profiles(base):
    models = {stage: {'name': 'fixture-' + stage, 'transport': 'openai_compatible',
                      'base_url': base, 'api_key_env': 'REFGEN_FIXTURE_KEY', 'revision': 'synthetic-json-v1'}
              for stage in ('quality', 'focus', 'author', 'finalize', 'concept', 'alignment')}
    for stage in ('synthesis', 'boogu'):
        models[stage] = {'backend': 'modelhub', 'model': 'fixture-' + stage, 'api': 'chat_interleaved',
                         'base_url': base, 'api_key_env': '', 'revision': 'synthetic-pixels-v1', 'timeout_s': 10}
    # These are request facts from the original joint experiment. The synthetic
    # service records them but does NOT execute diffusion steps, CFG or sampling.
    models['boogu']['parameters'] = {'num_inference_steps': 50, 'text_guidance_scale': 4.0,
        'image_guidance_scale': 1.0, 'seed': None, 'noise_policy': 'unseeded-native-v1'}
    return models


def create_inputs(root, *, base):
    """Explicit fixture provisioning, outside the pipeline; owns only new root."""
    root = Path(root).resolve()
    if not str(root).startswith('/tmp/demiflow-'):
        raise ValueError('Synthetic example provisioning is restricted to /tmp/demiflow-*')
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()): raise ValueError('Fixture provisioning requires an empty owned directory')
    assets = root / 'input-images'; assets.mkdir()
    images = []
    for image_id, color in [('qualified', 'navy'), ('low-quality', 'gray'), ('unrelated', 'orange')]:
        raw = png(color); path = assets / (image_id + '.png'); path.write_bytes(raw)
        images.append({'image_id': image_id, 'image': {'uri': path.as_uri(), 'sha256': hashlib.sha256(raw).hexdigest()}})
    values = {
        'concepts': ([{'concept_id': 'woodlouse', 'concept': '鼠妇', 'description': '陆生等足类动物；仅合成流程样例。'}],
                     pa.schema([('concept_id', pa.string()), ('concept', pa.string()), ('description', pa.string())])),
        'images': (images, pa.schema([('image_id', pa.string()), ('image', op.REF)])),
        'relations': ([{'concept_id': 'woodlouse', 'image_id': item['image_id'],
                       'relation_decision': 'reject' if item['image_id'] == 'unrelated' else 'pass'} for item in images],
                      pa.schema([('concept_id', pa.string()), ('image_id', pa.string()), ('relation_decision', pa.string())])),
    }
    sources = {}
    for name, (rows, schema) in values.items():
        uri = root / (name + '.lance')
        result = lance.write_dataset(pa.Table.from_pylist(rows, schema=schema), str(uri), mode='create')
        sources[name] = {'uri': str(uri), 'version': result.version}
    config = {'sources': sources, 'output_root': str(root), 'models': profiles(base),
              'execution_kind': 'fixture', 'max_concepts': 1}
    (root / 'params.json').write_text(json.dumps({'arguments': config}, ensure_ascii=False, indent=2) + '\n')
    return config


def _text(body):
    values = []
    for message in body.get('messages', []):
        content = message.get('content', '')
        if isinstance(content, str): values.append(content)
        else: values.extend(part.get('text', '') for part in content if part.get('type') == 'text')
    return '\n'.join(values)


def _payload(text, marker):
    suffix = text.rsplit(marker, 1)[1].lstrip()
    return json.JSONDecoder().raw_decode(suffix)[0]


def _questions(slot):
    place = '浅色背景' if slot == '01' else '平整石面'
    return {'short': f'画一只完整、清晰的鼠妇，置于{place}。',
            'medium': f'请画一只鼠妇，完整呈现且清晰可见，安排在{place}之中。',
            'long': f'请生成一幅画面：其中有一只鼠妇，要完整地呈现在画面里，能够清晰地看见；画面中的这只鼠妇位于{place}之中。'}


def _checks():
    return {key: {'verdict': 'pass', 'evidence': 'Synthetic fixture response; not an actual visual assessment.'} for key in op.CHECKS}


def _pair(slot, *, reuse, image_id=None):
    return {'case_id': slot, 'questions': _questions(slot), 'reference_route': 'reuse' if reuse else 'synthesize',
        'selected_image_id': image_id if reuse else None,
        'reference_instruction': None if reuse else '鼠妇自然状态的清晰结构参考图片，背景简单，完整呈现。',
        'design_basis': 'Fixture structure only; no real question-quality claim.',
        'reference_support': 'Fixture image binding only; no real knowledge-support claim.',
        'author_checks': _checks() if reuse else None, 'status': 'ready', 'issue': None}


def _json_answer(stage, body):
    text = _text(body)
    if stage == 'quality':
        payload = _payload(text, '图片编号：')
        image_id = payload['images'][0]['image_id']; good = image_id == 'qualified'
        checks = [{'item': name, 'presence': 'present' if not good and name == 'blur' else 'not_observed',
                   'location': 'fixture', 'evidence': 'Synthetic fixture only.', 'impact': 'Synthetic fixture only.',
                   'decision': 'reject' if not good and name == 'blur' else 'pass'} for name in op.QUALITY_CHECKS]
        return {'images': [{'image_id': image_id, 'decision': 'pass' if good else 'reject',
                            'reason': 'Synthetic fixture only.', 'limitations': None, 'checks': checks}]}
    if stage == 'focus':
        payload = _payload(text, '当前目标概念和图片编号：')
        images = [{'image_id': item['image_id'],
                   **{key: {'decision': 'pass', 'evidence': 'Synthetic fixture only.'} for key in ('focus', 'sufficiency', 'fidelity')},
                   'provided_evidence': ['Synthetic fixture only.'], 'applicable_scope': 'Synthetic fixture only.',
                   'decision': 'select'} for item in payload['images']]
        return {'concept_id': payload['concept_id'], 'reference_focus': 'Synthetic fixture only.',
                'images': images, 'selected_image_ids': [image['image_id'] for image in images]}
    if stage in ('author', 'finalize'):
        context = _payload(text, '当前输入：')
        if stage == 'finalize':
            assert context['phase'] == 'finalize'
            return {'pairs': [_pair(context['requested_case_ids'][0], reuse=True,
                                   image_id=context['candidate_images'][0]['image_id'])]}
        assert context['phase'] == 'author'
        refs = context['candidate_images']
        return {'pairs': [_pair('01', reuse=bool(refs), image_id=refs[0]['image_id'] if refs else None), _pair('02', reuse=False)]}
    if stage == 'concept':
        return {'expected_presentation': 'Synthetic fixture; no actual concept reasoning.',
                'visual_check': 'Synthetic fixture; no actual visual assessment.', 'score': 8, 'reason': 'Fixture score only.'}
    if stage == 'alignment':
        keys = ('主体与数量', '外观与属性', '动作与状态', '位置与布局', '对象关系', '场景与背景',
                '视角与取景', '必要展示与可辨性', '光照与成像效果', '风格与表现形式', '文字与标注', '多视图与分格', '其他题面要求')
        return {'dimension_scores': {key: {'score': 2, 'requirements': ['Fixture only.'], 'reason': 'Fixture score only.'} for key in keys}}
    raise ValueError('Unknown fixture model stage: ' + stage)


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


@contextmanager
def model_transport(*, fail_boogu_at=None):
    """Real bounded HTTP, deterministic synthetic JSON/images, zero model calls."""
    state = {'requests': [], 'counts': Counter(), 'errors': [], 'lock': threading.Lock()}
    old_key = os.environ.get('REFGEN_FIXTURE_KEY')
    os.environ['REFGEN_FIXTURE_KEY'] = 'synthetic-fixture-only'

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args): pass

        def reply(self, status, body):
            raw = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status); self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw))); self.end_headers()
            with suppress(BrokenPipeError, ConnectionResetError): self.wfile.write(raw)

        def do_POST(self):
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 768 * 1024:
                self.reply(413, {'error': 'Fixture request budget'}); return
            try:
                body = json.loads(self.rfile.read(size)); model = body['model']
                if not model.startswith('fixture-'): raise ValueError('Fixture cannot impersonate a real model')
                stage = model.removeprefix('fixture-')
                with state['lock']:
                    if len(state['requests']) >= 128: raise ValueError('Fixture request count budget')
                    state['counts'][stage] += 1; ordinal = state['counts'][stage]
                    state['requests'].append({'stage': stage, 'body': body})
                if stage == 'boogu' and ordinal == fail_boogu_at:
                    self.reply(500, {'error': {'message': 'Controlled synthetic Boogu transport failure'}}); return
                if stage in ('synthesis', 'boogu'):
                    raw = png((ordinal * 17 % 256, 64 if stage == 'synthesis' else 128, 192))
                    image = 'data:image/png;base64,' + base64.b64encode(raw).decode()
                    answer = [{'type': 'image_url', 'image_url': {'url': image}}]
                else:
                    answer = json.dumps({'result': _json_answer(stage, body)}, ensure_ascii=False)
                self.reply(200, {'choices': [{'message': {'content': answer}}]})
            except Exception as exc:
                state['errors'].append(repr(exc)); self.reply(500, {'error': {'message': str(exc)}})

    server = _Server(('127.0.0.1', 0), Handler)
    state['base'] = 'http://127.0.0.1:' + str(server.server_port) + '/v1'
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try: yield state
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)
        if old_key is None: os.environ.pop('REFGEN_FIXTURE_KEY', None)
        else: os.environ['REFGEN_FIXTURE_KEY'] = old_key


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, help='New empty /tmp/demiflow-* directory')
    args = parser.parse_args()
    with model_transport() as server:
        create_inputs(args.root, base=server['base'])
        print(json.dumps({'fixture_only': True, 'params': str(Path(args.root) / 'params.json'),
                          'model_endpoint': server['base']}, ensure_ascii=False), flush=True)
        try:
            while True: threading.Event().wait(30)
        except KeyboardInterrupt: pass


if __name__ == '__main__': main()
