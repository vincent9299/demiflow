"""Public read_images keeps the historical image ndarray and size/mode options."""
import numpy as np
from PIL import Image
import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext


def test_read_images_shape_mode_paths_and_owned_array(tmp_path):
    path=tmp_path/'rgb.png'; Image.new('RGB',(3,2),(10,20,30)).save(path)
    with RunContext.start(tmp_path/'run'), data.local_execution():
        original=data.read_images(str(path)).take_all()[0]
        assert set(original)=={'image'}
        assert original['image'].dtype==np.uint8 and original['image'].shape==(2,3,3)
        assert original['image'].tolist()==[[[10,20,30]]*3]*2
        resized=data.read_images(str(path),size=(4,1),mode='L',include_paths=True).take_all()[0]
        assert resized['path']==str(path)
        assert resized['image'].dtype==np.uint8 and resized['image'].shape==(4,1)
        assert resized['image'].tolist()==[[18]]*4
        resized['image'][0,0]=0
        assert original['image'][0,0].tolist()==[10,20,30]


def test_image_size_is_validated_before_decode(tmp_path):
    path=tmp_path/'rgb.png'; Image.new('RGB',(3,2)).save(path)
    with pytest.raises(Exception,match='size requires positive'):
        with RunContext.start(tmp_path/'run'),data.local_execution():
            data.read_images(str(path),size=(0,1)).take_all()


def test_image_decompression_bound_rejects_before_loading_pixels(tmp_path):
    path=tmp_path/'large.png'; Image.new('RGB',(1024,1024)).save(path)
    with pytest.raises(Exception,match='Decoded image exceeds max_row_bytes'):
        with RunContext.start(tmp_path/'run',limits={'max_row_bytes':1024*1024}),data.local_execution():
            data.read_images(str(path)).take_all()
