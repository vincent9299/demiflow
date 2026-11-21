"""Private bootstrap for an explicitly enabled, project-owned Jupyter Server."""
from __future__ import annotations

import asyncio
from importlib import metadata
import json
import os
from pathlib import Path
import sys


def main():
    config_path = Path(sys.argv[1])
    settings = json.loads(config_path.read_text())
    sys.path.insert(0, settings['code_root'])
    # An abrupt workbench exit must not leave its native server and kernels.
    # Only this child signals itself; Jupyter performs its standard shutdown.
    import signal
    import threading
    import time
    def watch_owner():
        while os.getppid() == settings['parent_pid']:
            time.sleep(2)
        os.kill(os.getpid(), signal.SIGTERM)
    threading.Thread(target=watch_owner, daemon=True).start()
    from traitlets.config import Config
    from jupyter_server.serverapp import ServerApp
    from jupyter_server.services.contents.filemanager import FileContentsManager
    from jupyter_server.files.handlers import FilesHandler
    from jupyter_server.services.kernels.kernelmanager import MappingKernelManager
    from tornado.web import HTTPError

    class ProjectContents(FileContentsManager):
        """Keep Jupyter's standard persistence; bound its file API at the edge."""
        files_handler_class = FilesHandler

        def is_writable(self, path):
            return json.loads((config_path.parent/'grants.json').read_text())['writable'] and super().is_writable(path)
        def _get_os_path(self, path):
            result = super()._get_os_path(path)
            base = Path(self.root_dir).resolve()
            if not Path(result).resolve().is_relative_to(base):
                raise HTTPError(403, 'Notebook path leaves this project')
            relative = Path(result).relative_to(base)
            cursor = base
            for part in relative.parts:
                cursor /= part
                if cursor.is_symlink():
                    raise HTTPError(403, 'Notebook symlinks are not supported')
            return result

        def get(self, path, content=True, type=None, format=None, **kwargs):
            target = Path(self._get_os_path(path))
            if content and target.is_file() and target.stat().st_size > 16 * 1024**2:
                raise HTTPError(413, 'Notebook file exceeds 16 MiB')
            if content and target.is_dir():
                with os.scandir(target) as entries:
                    for index, _ in enumerate(entries):
                        if index >= 512:
                            raise HTTPError(413, 'Directory exceeds 512 entries')
            return super().get(path, content=content, type=type, format=format, **kwargs)

    kernel_limit = 2

    class ProjectKernels(MappingKernelManager):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._admission = asyncio.Lock()

        async def start_kernel(self, *, kernel_id=None, path=None, **kwargs):
            async with self._admission:
                if kernel_id is None and len(self.list_kernel_ids()) >= kernel_limit:
                    raise HTTPError(429, 'This project already has two kernels')
                kwargs['cwd'] = settings['root']
                return await super().start_kernel(kernel_id=kernel_id, path=None, **kwargs)

    config = Config()
    config.ServerApp.ip = '127.0.0.1'
    config.ServerApp.port = settings['port']
    config.ServerApp.port_retries = 0
    config.ServerApp.open_browser = False
    config.ServerApp.allow_root = True
    config.ServerApp.root_dir = settings['notebooks']
    config.ServerApp.base_url = settings['base_url']
    config.ServerApp.default_url = '/tree'
    config.ServerApp.terminals_enabled = False
    config.ServerApp.allow_remote_access = False
    config.ServerApp.contents_manager_class = ProjectContents
    config.ServerApp.kernel_manager_class = ProjectKernels
    config.ServerApp.max_body_size = 16 * 1024**2
    config.ServerApp.max_buffer_size = 16 * 1024**2
    config.ServerApp.websocket_ping_interval = 20
    config.ServerApp.websocket_ping_timeout = 20
    def page_config(handler, value):
        can_start = (json.loads((config_path.parent / 'grants.json').read_text())['execute']
                     and len(handler.kernel_manager.list_kernel_ids()) < kernel_limit)
        disabled = [*value.get('disabledExtensions', []),
                    '@jupyter-notebook/lab-extension:interface-switcher', '@jupyterlab/lsp-extension']
        widgets = '@jupyter-widgets/jupyterlab-manager'
        # The widget manager needs an attached kernel to register its comm targets.
        defer_widgets = not can_start and not any(item.split(':')[0] == widgets for item in disabled)
        if defer_widgets:
            disabled.append(widgets)
        return {**value, 'token': '', 'allow_hidden': False, 'notebookStartsKernel': can_start,
                'disabledExtensions': disabled, 'demiflowDeferredWidgets': defer_widgets}

    config.ServerApp.tornado_settings = {
        'headers': {'Content-Security-Policy': "frame-ancestors 'self'"},
        'page_config_hook': page_config,
    }
    config.IdentityProvider.token = settings['token']
    config.MappingKernelManager.cull_idle_timeout = 1800
    config.MappingKernelManager.cull_interval = 60
    config.MappingKernelManager.cull_connected = False
    config.MappingKernelManager.cull_busy = False
    config.ZMQChannelsWebsocketConnection.iopub_data_rate_limit = 2 * 1024**2
    config.ZMQChannelsWebsocketConnection.iopub_msg_rate_limit = 1000
    config.JupyterNotebookApp.expose_app_in_browser = True
    config.JupyterNotebookApp.user_settings_dir = str(config_path.parent / 'settings')
    config.JupyterNotebookApp.custom_css = True
    config.JupyterNotebookApp.default_url = '/tree'
    # A Notebook environment can inherit ipywidgets from another Python
    # installation. Notebook's default data paths only cover its own prefix.
    from jupyter_core.paths import jupyter_path
    manager = Path('@jupyter-widgets/jupyterlab-manager')
    if not any((Path(parent) / manager / 'package.json').is_file() for parent in jupyter_path('labextensions')):
        try:
            package = metadata.distribution('jupyterlab_widgets')
        except metadata.PackageNotFoundError:
            package = None
        if package:
            manifest = next((package.locate_file(f) for f in (package.files or ())
                             if Path(f).parts[-4:] == ('labextensions', *manager.parts, 'package.json')), None)
            if manifest and manifest.is_file():
                extensions = config_path.parent / 'widget-extensions'
                link = extensions / manager
                link.parent.mkdir(parents=True, exist_ok=True)
                if not link.is_symlink():
                    link.symlink_to(manifest.resolve().parent, target_is_directory=True)
                config.LabServerApp.extra_labextensions_path = [str(extensions)]
    config.KernelSpecManager.allowed_kernelspecs = {'python3'}
    config.FileContentsManager.delete_to_trash = False
    config.Application.log_level = 'WARN'
    # Prevent unrelated global server extensions from starting services.
    config.ServerApp.jpserver_extensions = {'notebook': True, 'jupyterlab': False,
                                          'jupyter_lsp': False, 'jupyter_server_terminals': False}
    ServerApp.launch_instance(argv=[], config=config)


if __name__ == '__main__':
    main()
