"""Submit delivery reviews from an author turn and run them after the author drains."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time

from .common import plain_path, reject, wire


REVIEW_TOOL = dict(name='request_pipeline_review',
    description='Request an independent read-only delivery review after this author turn finishes. '
                'By default review the source changes observed in this development session. '
                'Supply an entry (and optional params) to review a particular flow.',
    inputSchema=dict(type='object', additionalProperties=False, properties={
        'entry': dict(type='string', description='Project-relative flow.py:function'),
        'params': dict(type='string', description='Project-relative parameter file'),
    }))


class DeliveryReview:
    """One idempotent request in the active worker, using its existing execution slot."""

    def __init__(self, store, turn, project):
        self.store = store
        self.turn = turn
        self.project = project
        self.selection = None

    def request(self, arguments):
        if type(arguments) is not dict or arguments.keys() - {'entry', 'params'}:
            reject('评审只接受入口和参数文件。', 'invalid_request')
        if 'entry' in arguments:
            entry = arguments['entry']
            if not isinstance(entry, str) or ':' not in entry:
                reject('评审入口需要文件及函数名。', 'invalid_request')
            name, symbol = entry.rsplit(':', 1)
            if not symbol.isidentifier() or not plain_path(name).endswith('.py'):
                reject('无效的评审入口。', 'invalid_request')
        if 'params' in arguments:
            if 'entry' not in arguments:
                reject('评审参数需要指定入口。', 'invalid_request')
            plain_path(arguments['params'])
        if self.selection is None:
            self.store.event(self.turn['id'], 'review.requested',
                             {'request_id': self.turn['id'], 'selection': arguments})
            self.selection = dict(arguments)
        elif self.selection != arguments:
            reject('本轮已经提交了不同范围的评审。', 'conflict')
        return {'request_id': self.turn['id'], 'status': 'queued',
                'detail': '评审将在本轮开发进程结束后开始，结果保存在当前会话。'}

    def run(self, reviewer, checks, check_cancel):
        from demiflow.forge.review import review_project
        goal, prior = self.store.review_inputs(self.turn['session'], include_paths=not self.selection)
        if not self.selection and not prior:
            reject('当前会话没有记录到源码改动。请指定 Flow 入口评审当前实现。')
        observations = []
        if checks:
            for key in ('native_flow', 'project_layout'):
                item = checks.get(key)
                if item:
                    observations.append({'source': key, 'detail': wire({
                        'turn_id': self.turn['id'], 'status': item.get('status'),
                        'diagnostic_count': item.get('diagnostic_count')}).decode()})
        check_cancel()
        review = review_project(self.project['root'], goal=goal, **self.selection,
            paths=tuple(prior), prior_sources=prior or None, exclude=self.project.get('protected', ()),
            observations=observations, reviewer=reviewer)
        check_cancel()
        return review


def run_deployment_reviewer(prompt, *, deployment, turn, session, scratch, broker_home, author_spec,
                            lease_fd, deadline, check_cancel):
    """The host owns model transport, reviewer credentials and the turn's one slot."""
    from demiflow.forge.review_deepseek import run_reviewer
    config = deployment.providers[session['provider']]
    timeout = min(180, deadline - time.time())
    check_cancel()
    if config.get('model_provider', 'deepseek-official') == 'deepseek-official' and not config.get('credential_env'):
        reject('工作台未提供独立评审所需的 DeepSeek API 凭证配置。')
    if deployment.mode == 'trusted_local':
        return run_reviewer(prompt, timeout=timeout, config=config, check_cancel=check_cancel)
    if deployment.mode != 'restricted':
        reject('当前部署未提供独立只读评审。')

    from demiflow.forge.review_deepseek import prepare_launch, collect_review
    from .model_broker import start_model_broker
    from .sandbox import launch_sandbox
    with tempfile.TemporaryDirectory(dir=scratch.parent, prefix='review-') as temporary:
        home = Path(temporary)
        spec = replace(author_spec, role='readonly', seconds=timeout, private_home=temporary, write_paths=(),
            denied_paths=(*author_spec.denied_paths, author_spec.private_home),
            read_paths=tuple(path for path in author_spec.read_paths if path == '/etc/ssl/openssl.cnf'))
        # The control directory's parent is the same epoch as the author's
        # model broker, so both charges use that turn's existing quota.
        broker = start_model_broker(deployment, turn, session, broker_home.with_name('review-model'),
                                    defer_handshake=True)
        try:
            check_cancel()
            command, env = prepare_launch(broker.child_config, home,
                                          model_channel=Path(__file__).with_name('deepseek_model_proxy.cjs'))
            process = launch_sandbox(command, spec=spec, env=env, cwd=home, lease_fd=lease_fd,
                                     model_socket=broker.socket_path)
            try:
                return collect_review(process, prompt, cwd=home, model=config['model'], timeout=timeout,
                                      check_cancel=check_cancel)
            except ValueError:
                if broker.failure:
                    reject('DeepSeek 模型通道：' + broker.failure)
                raise
        finally:
            broker.close()


def report_events(report):
    """Keep each finding and coverage gap as individually bounded store events."""
    for field in ('findings', 'gaps', 'diagnostics', 'checks'):
        for item in report.get(field, ()):
            yield 'review.detail', {'field': field, 'item': item}
    yield 'review.completed', {key: report[key] for key in (
        'status', 'reviewer', 'review_id', 'scope', 'evidence_id', 'goal', 'rules_id',
        'guide_digest', 'sources', 'observations', 'additional_read') if key in report}


def recent_review(store, session):
    """Bound the last host report included as evidence in a subsequent author turn."""
    with store.connect() as db:
        row = db.execute("""SELECT id,json_extract(result,'$.pipeline_review'),
            json_extract(result,'$.review_reason') FROM turns
            WHERE session=? AND json_extract(result,'$.pipeline_review') IS NOT NULL
            ORDER BY created DESC,id DESC LIMIT 1""", (session,)).fetchone()
        if not row:
            return None
        notes = [row[2]] if row[2] else []
        for event in db.execute("""SELECT json_extract(body,'$.data') FROM events
                WHERE turn=? AND json_extract(body,'$.type')='review.detail'
                AND json_extract(body,'$.data.field')!='checks' ORDER BY sequence""", (row[0],)):
            item = json.loads(event[0])
            if len(notes) >= 6 or len(wire([*notes, item])) > 6144:
                notes.append('其余依据见会话中的完整评审。')
                break
            notes.append(item)
        return {'turn_id': row[0], 'status': row[1], 'details': notes}

