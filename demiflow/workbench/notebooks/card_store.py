"""Commit a single cell through the project's native Notebook contents manager."""
import hashlib
import json
import re

import nbformat
from jupyter_server.auth.decorator import authorized
from jupyter_server.base.handlers import APIHandler
from tornado import web


def revision(cell):
    # Jupyter computes trust on reading. It is not an edit to the cell.
    metadata = {k: v for k, v in cell.get('metadata', {}).items() if k != 'trusted'}
    value = {**cell, 'metadata': metadata}
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
    return hashlib.sha256(raw).hexdigest()


class CardContents(APIHandler):
    auth_resource = 'contents'

    def notebook(self, path):
        if not path.endswith('.ipynb'):
            raise web.HTTPError(400, '请选择 Notebook 文件。')
        model = self.contents_manager.get(path, content=True, type='notebook')
        return model

    @web.authenticated
    @authorized
    def get(self, path):
        model = self.notebook(path)
        self.finish({'cells': {cell['id']: revision(cell) for cell in model['content'].cells},
                     'modified': model['last_modified'].isoformat()})

    @web.authenticated
    @authorized
    def post(self, path):
        # Native FileContentsManager reads and atomically writes synchronously in
        # this server's event loop, including other clients' Contents API requests.
        if not self.contents_manager.is_writable(path):
            raise web.HTTPError(403, '此分析为只读。')
        book = self.notebook(path)['content']
        request = self.get_json_body()
        key = request.get('id') if isinstance(request, dict) else None
        if not isinstance(key, str) or not re.fullmatch('[a-zA-Z0-9_-]{1,64}', key):
            raise web.HTTPError(400, '无效的卡片。')
        if 'expected' not in request or 'cell' not in request:
            raise web.HTTPError(400, '缺少卡片内容或原始版本。')
        cells = book.cells
        old = next((c for c in cells if c['id'] == key), None)
        value = request['cell']
        if value is not None and not (isinstance(value,dict) and value.get('id')==key and
                                      isinstance(value.get('metadata'),dict)):
            raise web.HTTPError(400, '保存目标与卡片内容不一致。')
        current = revision(old) if old is not None else None
        if current != request['expected']:
            # A retry can arrive after the first response was lost.
            if (old is None and value is None or isinstance(value,dict) and value.get('id')==key
                    and current==revision(value)):
                self.finish({'revision':current})
                return
            raise web.HTTPError(409, '这张卡片已在其他页面修改，请先核对草稿。')
        if value is None:
            if old is not None:
                cells.remove(old)
        else:
            if value.get('cell_type') not in ('code','markdown','raw'):
                raise web.HTTPError(400, '未知的卡片类型。')
            cell = nbformat.from_dict(value)
            try:
                nbformat.validate(cell, ref=cell.cell_type+'_cell', version=4, version_minor=5)
            except (AttributeError, KeyError, nbformat.ValidationError) as error:
                raise web.HTTPError(400, '卡片格式无效。') from error
            if old is not None:
                cells.remove(old)
            before = request.get('before')
            index = next((i for i, c in enumerate(cells) if c['id'] == before), len(cells))
            cells.insert(index, cell)
        if len(nbformat.writes(book).encode('utf-8')) > 16 * 1024**2:
            raise web.HTTPError(413, '保存后超出 Notebook 大小上限。')
        self.contents_manager.save({'type':'notebook','content':book}, path)
        self.finish({'revision': revision(cell) if value is not None else None})


default_handlers = [(r'/api/demiflow/cards/(.+)', CardContents)]
