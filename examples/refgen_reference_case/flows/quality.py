"""独立质量审核；复用相同算子与 Prompt，不调用另一条流程。"""
from pathlib import Path
import pyarrow as pa
from demiflow import data
import refgen_operators as op
from prompt_bindings import prompt_configuration
from project_runtime import load_runtime


def run(*, sources: dict, output_root: str, max_images: int = 8):
    """已有图片质量审核"""
    if type(max_images) is not int or not 1 <= max_images <= 32:
        raise ValueError('max_images must be between 1 and 32')
    root = Path(output_root)
    if not root.is_absolute():
        raise ValueError('output_root must be absolute')
    runtime = load_runtime()
    models = runtime['models']
    name, pack = prompt_configuration('quality', models)
    images = data.read_lance(**sources['images'], columns=['image_id', 'image'], batch_size=1)
    reviewed = (images.read_object(reference='image', output='pixels', root=str(root), max_bytes=65536,
                                   error_output='read_error', label='读取待审核图片')
        .map(op.quality_input, label='准备质量审核输入')
        .map_prompt_async(name, config=pack, inputs={'payload':'payload', 'images':'images'},
            output='quality_answer', call_output='quality_call', error_output='quality_error', when=op.has_images,
            max_requests=max_images, concurrency=1, queue_depth=1,
            options={'trust_env':False, 'timeout_s':15, 'max_request_bytes':524288,
                     'max_response_bytes':65536, 'max_event_bytes':65536,
                     'model_revision':models['quality']['revision']}, label='已有图质量筛选')
        .map(op.quality_result, label='核对质量结论与图片绑定')
        .select_columns(['image_id', 'quality_passed', 'quality_problem']))
    return reviewed.write_lance(str(root / 'quality.lance'), mode='create',
        schema=pa.schema([('image_id',pa.string()),('quality_passed',pa.bool_()),('quality_problem',pa.string())]),
        return_receipt=True)
