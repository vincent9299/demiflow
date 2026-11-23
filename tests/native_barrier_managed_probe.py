"""100x100 through actual native HTTP text/image journals and final terminals.

Only a loopback synthetic provider is started. No model weights, GPU or paid
endpoint are used. Request counters and result verification remain bounded.
"""
import base64
from collections import Counter
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import resource
import sqlite3
import socket
import sys
import threading
import time

from PIL import Image
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.operator_llm import parse_prompt_pack
from image_http_fixture import definition, json_definition

ROOT = Path(sys.argv[1])
MODE = sys.argv[2] if len(sys.argv) > 2 else 'materialize'
ROOTS = int(sys.argv[3]) if len(sys.argv) > 3 else 100
CHILDREN = int(sys.argv[4]) if len(sys.argv) > 4 else 100
COUNTS = Counter()
LOCK = threading.Lock()
STARTED = time.monotonic()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def setup(self):
        super().setup()
        # Keep tiny loopback responses from measuring delayed TCP ACKs.
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    def log_message(self, *args):
        pass

    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        if size > 32768:
            self.send_error(413)
            return
        value = json.loads(self.rfile.read(size))
        with LOCK:
            COUNTS['text' if self.path.startswith('/json/') else 'image'] += 1
        if self.path.startswith('/json/'):
            prompt = value['messages'][-1]['content']
            result = {'choices': [{'message': {'content': json.dumps({'answer': prompt})}}]}
        else:
            prompt = value['prompt']
            with Image.frombytes('RGB', (4, 4), hashlib.sha384(prompt.encode()).digest()) as picture:
                with io.BytesIO() as output:
                    picture.save(output, format='PNG')
                    raw = output.getvalue()
            result = {'data': [{'b64_json': base64.b64encode(raw).decode()}]}
        raw = json.dumps(result).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()


def expand(row):
    for child in range(CHILDREN):
        yield {'id': row['id'], 'child': child, 'instruction': str(row['id']) + ':' + str(child)}


def score(row):
    return {**row, 'score': len(row['pixels'])}


server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
server.daemon_threads = True
thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.05), daemon=True)
thread.start()
base = 'http://127.0.0.1:' + str(server.server_port) + '/v1'
os.environ['SYNTHETIC_JSON_KEY'] = 'local-fixture-only'
transactions = Counter()
original_transaction = None
from contextlib import contextmanager
from demiflow.execution.run_journal import RunJournal
original_transaction = RunJournal._transaction
@contextmanager
def observed_transaction(self):
    with LOCK:
        transactions['control'] += 1
    with original_transaction(self) as db:
        yield db
RunJournal._transaction = observed_transaction
fsyncs = Counter()
original_fsync = os.fsync

def observed_fsync(fd):
    with LOCK:
        fsyncs['python_artifact_fsync'] += 1
    return original_fsync(fd)
os.fsync = observed_fsync
import demiflow.execution.commit as protocol
from demiflow.execution.recovery_contract import RunRef
WRITER_METRICS = []
ATTEMPTS = []
INITIAL_USAGE = resource.getrusage(resource.RUSAGE_SELF)
RECOVER = MODE.endswith('-recover')
FAULTED = False
original_fault = protocol.fault_point

def fault(name, commit_id):
    global FAULTED
    if not RECOVER or FAULTED:
        return
    fact = RunJournal(ROOT/'run').execution_commit(commit_id)
    if MODE.startswith('direct'):
        selected = name == 'after_prepared' and fact['action_id'] == 'action-1'
    else:
        selected = False
        if name == 'after_acked' and fact['action_id'] == 'action-1':
            ref = fact['prepared']['state']['dataflow-cut-v3']
            cut = json.loads((ROOT/'run'/ref['path']).read_bytes())
            selected = cut['cut']['final']
    if selected:
        FAULTED = True
        raise RuntimeError('managed-probe-durable-boundary')

protocol.fault_point = fault
LIMITS = {'timeout_s': 1200, 'checkpoint_timeout_s': 180,
            'checkpoint_rows': 16, 'checkpoint_interval_ms': 60000, 'max_control_bytes': 128 * 1024**2,
            'max_row_bytes': 65536, 'max_batch_bytes': 262144}
try:
    for attempt in range(2 if RECOVER else 1):
        attempt_started = time.monotonic()
        before_requests = sum(COUNTS.values())
        prior = RunJournal(ROOT/'run').recovery_identity()
        scope = (RunContext.resume(RunRef(str(ROOT/'run'), prior['run_id'])) if prior else
                 RunContext.start(ROOT/'run', limits=LIMITS))
        try:
            with scope as run:
                text_pack = parse_prompt_pack(json.dumps(json_definition(base)))
                image_pack = definition(base, template='{{ instruction }}')
                image_pack['prompts']['generate']['model']['timeout_s'] = 10
                graph = data.from_items([{'id': i} for i in range(ROOTS)]).flat_map(expand).map_prompt_async(
                    'answer', config=text_pack, inputs={'instruction': 'instruction'}, output='planned',
                    concurrency=4, max_requests=ROOTS*CHILDREN,
                    options={'trust_env': False, 'timeout_s': 10, 'max_request_bytes': 8192,
                             'max_response_bytes': 8192, 'max_event_bytes': 8192}).map_prompt_async(
                    'generate', config=parse_prompt_pack(json.dumps(image_pack)), inputs={'instruction': 'planned'},
                    output='image', call_output='image_call', error_output='image_error', concurrency=4, max_requests=ROOTS*CHILDREN,
                    options={'object_store': str(ROOT/'assets'), 'image_encoding': 'preserve',
                        'io_workers': 4, 'prepare_workers': 4, 'response_workers': 4,
                        'limits': {'max_images': 1, 'max_image_bytes': 4096, 'max_text_bytes': 1024,
                            'max_pixels': 64, 'max_total_pixels': 64, 'max_request_bytes': 8192,
                            'max_response_bytes': 8192}}).read_object(
                        reference='image', output='pixels', root=str(ROOT/'assets'),
                        layout='sha256_prefix2', max_bytes=4096).map(score)
                if MODE.startswith('direct'):
                    graph.write_lance(str(ROOT/'result.lance'), mode='create')
                    result = data.read_lance(str(ROOT/'result.lance'), version=1)
                else:
                    result = graph.materialize()
                seen = bytearray(ROOTS * CHILDREN)
                count = 0
                for row in result.iter_rows():
                    index = row['id'] * CHILDREN + row['child']
                    assert not seen[index]
                    seen[index] = 1
                    assert row['planned'] == row['instruction'] and row['score'] == len(row['pixels']) > 0
                    count += 1
                assert count == ROOTS * CHILDREN and all(seen)
                with run.journal._connection() as db:
                    metrics = dict(calls=db.execute('SELECT count(*) FROM calls').fetchone()[0],
                        payload_bytes=db.execute('SELECT sum(length(request_json)+length(response_json)) FROM calls').fetchone()[0],
                        commits=db.execute('SELECT count(*) FROM runtime_commits').fetchone()[0],
                        registrations=db.execute('SELECT count(*) FROM runtime_artifacts').fetchone()[0],
                        registered_bytes=db.execute('SELECT bytes FROM runtime_storage').fetchone()[0],
                        response_reserved=int(db.execute("SELECT value FROM journal_state WHERE name='native_response_reserved'").fetchone()[0]))
                proofs = getattr(run, '_artifact_proofs', None)
                proof_metrics = dict(hashed_bytes=proofs.hashed_bytes, cache_hits=proofs.hits) if proofs else {}
        except RuntimeError as error:
            if not RECOVER or attempt != 0 or str(error) != 'managed-probe-durable-boundary':
                raise
        finally:
            ATTEMPTS.append(dict(attempt=attempt+1, seconds=time.monotonic()-attempt_started,
                                 new_http_requests=sum(COUNTS.values())-before_requests))
            journal = getattr(scope, '_native_prompt_journal', None)
            if journal is not None:
                WRITER_METRICS.append(journal._metrics.snapshot())
    assert COUNTS == {'text': ROOTS*CHILDREN, 'image': ROOTS*CHILDREN}
    assert metrics['response_reserved'] == 0
    if RECOVER:
        assert FAULTED and RunJournal(ROOT/'run').execution_status()['attempt'] == 2

finally:
    protocol.fault_point = original_fault
    RunJournal._transaction = original_transaction
    os.fsync = original_fsync
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
files = sum(1 for path in ROOT.rglob('*') if path.is_file())
print(json.dumps(dict(mode=MODE, roots=ROOTS, children=CHILDREN, outputs=count,
    seconds=time.monotonic()-STARTED, attempts=ATTEMPTS,
    cpu_user_s=resource.getrusage(resource.RUSAGE_SELF).ru_utime-INITIAL_USAGE.ru_utime,
    cpu_system_s=resource.getrusage(resource.RUSAGE_SELF).ru_stime-INITIAL_USAGE.ru_stime, requests=dict(COUNTS), model_writer_attempts=WRITER_METRICS, recovered=RECOVER,
    control_transactions=dict(transactions), fsyncs=dict(fsyncs), sqlite_fsync='SQLite FULL; counted separately by group commit transactions',
    metadata=metrics, proofs=proof_metrics, files=files,
    database_bytes=(ROOT/'run/run.sqlite').stat().st_size,
    max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    fd_at_end=len(os.listdir('/proc/self/fd'))), ensure_ascii=False))
