"""Submit delivery reviews from an author turn and run them after the author drains."""
from contextlib import ExitStack
from dataclasses import replace
import json
from pathlib import Path
import subprocess
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
    from demiflow.forge.review_deepseek import prepare_launch, collect_review
    from .execution import launch_provider
    session = dict(session, provider='deepseek')
    config = deployment.providers.get('deepseek')
    if config is None:
        reject('工作台未配置独立 DeepSeek 评审器。', 'inspection_incomplete')
    timeout = deadline - time.time()
    check_cancel()
    if config.get('model_provider', 'deepseek-official') == 'deepseek-official' and not config.get('credential_env'):
        reject('工作台未提供独立评审所需的 DeepSeek API 凭证配置。')
    if deployment.mode not in ('restricted', 'trusted_local'):
        reject('当前部署未提供独立只读评审。')

    with tempfile.TemporaryDirectory(dir=scratch.parent, prefix='review-') as temporary, ExitStack() as cleanup:
        home = Path(temporary)
        spec = broker = channel = None
        if deployment.mode == 'restricted':
            from .model_broker import start_model_broker
            from .policy import Deployment
            review_deployment = deployment
            from .execution import runtime_paths
            review_runtime = tuple(dict.fromkeys((*author_spec.runtime_paths, *runtime_paths(config))))
            spec = replace(author_spec, role='readonly', read_policy='scoped', write_policy='scoped',
                direct_network=False, seconds=timeout, private_home=temporary, write_paths=(),
                runtime_paths=tuple(path for path in review_runtime if Path(path).exists()),
                denied_paths=(*author_spec.denied_paths, author_spec.private_home),
                read_paths=tuple(path for path in author_spec.read_paths if path == '/etc/ssl/openssl.cnf'))
            # Both brokers debit the existing turn quota under this control directory.
            broker = start_model_broker(review_deployment, turn, session, broker_home.with_name('review-model'),
                                        defer_handshake=True, request_seconds=timeout, deadline=deadline)
            cleanup.callback(broker.close)
            config = broker.child_config
            channel = Path(__file__).with_name('deepseek_model_proxy.cjs')

        class Reviewer:
            name = 'deepseek'

            @staticmethod
            def launch(config, home, root):
                return prepare_launch(config, home, model_channel=channel)

        check_cancel()
        try:
            process = launch_provider(mode=deployment.mode, adapter=Reviewer, config=config, home=home, root=home,
                lease_fd=lease_fd, sandbox_spec=spec, model_socket=broker.socket_path if broker else None)
        except (RuntimeError, subprocess.SubprocessError) as exc:
            reject('独立评审进程未能启动：' + str(exc)[:512], 'inspection_incomplete')
        try:
            return collect_review(process, prompt, cwd=home, model=config['model'], timeout=timeout,
                                  check_cancel=check_cancel)
        except ValueError:
            if broker and broker.failure:
                reject('DeepSeek 模型通道：' + broker.failure)
            raise


def report_events(report):
    """Keep each finding and coverage gap as individually bounded store events."""
    for field in ('findings', 'gaps', 'diagnostics', 'checks'):
        for item in report.get(field, ()):
            yield 'review.detail', {'field': field, 'item': item}
    completed = {key: report[key] for key in (
        'status', 'reviewer', 'review_id', 'scope', 'evidence_id', 'goal', 'rules_id',
        'guide_digest', 'sources', 'observations', 'additional_read', 'current', 'complete', 'budget', 'units') if key in report}
    for field in ('goal', 'sources', 'units', 'scope'):
        value = completed.get(field)
        if value is None or len(wire(value)) <= 8192:
            continue
        encoded = json.dumps(value, ensure_ascii=False)
        parts = [encoded[offset:offset + 4096] for offset in range(0, len(encoded), 4096)]
        for index, part in enumerate(parts):
            yield 'review.material', dict(field=field, index=index, count=len(parts), json_part=part)
        # Keep the scope discriminator for the existing delivery boundary query.
        completed[field] = dict(details_in_events=True, parts=len(parts), **(
            {k:v for k,v in value.items() if k in ('mode', 'entry', 'params', 'base')} if field == 'scope' else {}))
    yield 'review.completed', completed


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
