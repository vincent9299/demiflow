"""Local model transport; prompt runtime owns requests, journals and lifecycle."""
import base64

from demiflow.execution.blocking_io import BlockingIOPool
from .media import BoundedImageBuffer, checked_image, media_limits


class DiffusersTransport:
    def __init__(self, configuration, options):
        self.configuration, self.options = configuration, options
        self.model = None
        self.pool = BlockingIOPool(1, name='demiflow-diffusers')

    def load_model(self):
        import torch
        from diffusers import DiffusionPipeline
        self.model = DiffusionPipeline.from_pretrained(self.configuration['model_path'],
            torch_dtype=torch.bfloat16, local_files_only=True).to(self.configuration['device'])

    def generate(self, source):
        import torch
        if self.model is None:
            self.load_model()
        limits = media_limits(self.options)
        references = []
        outputs = []
        try:
            for url in source['body'].get('image', []):
                references.append(checked_image(base64.b64decode(url.split(',', 1)[1], validate=True), limits))
            outputs = self.model(prompt=source['body']['prompt'],
                **({'image': references} if references else {}),
                **self.configuration.get('parameters', {}),
                generator=torch.Generator('cpu').manual_seed(self.configuration['seed'])).images
            if len(outputs) != 1:
                raise ValueError('Image model must return exactly one image')
            if outputs[0].width * outputs[0].height > limits['max_pixels']:
                raise ValueError('Generated image exceeds max_pixels')
            with BoundedImageBuffer(limits['max_image_bytes']) as buffer:
                outputs[0].save(buffer, format='PNG')
                return buffer.getvalue()
        finally:
            for picture in [*references, *outputs]:
                picture.close()

    async def execute(self, source):
        return await self.pool.run(self.generate, source)

    def release(self):
        if self.model is not None:
            import gc
            import torch
            self.model = None
            gc.collect()
            torch.cuda.empty_cache()

    async def aclose(self):
        await self.pool.drain()
        try:
            await self.pool.run(self.release)
        finally:
            await self.pool.aclose()
