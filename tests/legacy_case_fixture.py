"""Private legacy Records/media data for non-Case workbench regression tests."""
from contextlib import contextmanager
from pathlib import Path
import hashlib
import io
import json
import os
import selectors
import subprocess
import sys
from PIL import Image, ImageDraw
OWN=Path(__file__).resolve().parents[1]
BACKEND=OWN

def cup(color,kind):
    image=Image.new('RGB',(384,288),'#f5f1e8');draw=ImageDraw.Draw(image)
    draw.rectangle((0,212,384,288),fill='#d8c8af');draw.ellipse((66,196,315,244),fill='#b5a994')
    draw.ellipse((245,83,324,190),outline=color,width=20)
    draw.rounded_rectangle((87,69,270,220),radius=27,fill=color)
    draw.ellipse((87,55,270,101),fill='#fff5e4',outline=color,width=12)
    if kind==1:
        for x,y in [(120,120),(165,120),(210,120),(143,167),(192,167)]:draw.ellipse((x-7,y-7,x+7,y+7),fill='white')
    output=io.BytesIO();image.save(output,format='PNG');return output.getvalue()


@contextmanager
def rpc(tmp_path,config,*,lock):
    original=BACKEND/'tests/_forge_analysis_media_capture_driver.py';text=original.read_text()
    needle="result=controller.metadata.route(authority,name,payload)";assert text.count(needle)==1
    text=text.replace(needle,"result=register_output_fixture(records,authority,payload) if name=='fixture-output-register' else controller.metadata.route(authority,name,payload)")
    text="import sys\nsys.path.insert(0,"+repr(str(OWN/'tests'))+")\nfrom _forge_analysis_case_ui_controller import register_output_fixture\n"+text
    driver=tmp_path/'case-private-media-driver.py';driver.write_text(text)
    (tmp_path/'driver-input.json').write_text(json.dumps(dict(base=str(original),base_sha256=hashlib.sha256(original.read_bytes()).hexdigest(),overlay_sha256=hashlib.sha256(text.encode()).hexdigest(),scope='Test-only sealed Notebook registration branch; original media/native/SQLite/PNG driver and explicit Runtime doubles'),indent=2)+'\n')
    process=subprocess.Popen([sys.executable,'-B','-u',str(driver),'rpc',str(config)],cwd=BACKEND,env=dict(os.environ,PYTHONPATH=str(BACKEND),PYTHONDONTWRITEBYTECODE='1',FORGE_CASE_ORIGIN_BACKEND_ROOT=str(BACKEND)),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    def receive():
        with selectors.DefaultSelector() as poll:
            poll.register(process.stdout,selectors.EVENT_READ);assert poll.select(75),'private fixture timed out'
        line=process.stdout.readline();assert line,process.stderr.read();return json.loads(line)
    def call(value):process.stdin.write(json.dumps(value)+'\n');process.stdin.flush();return receive()
    try:assert receive()==dict(ready=True);yield call
    finally:
        acquired=lock.acquire(timeout=60)
        if acquired:
            try:process.stdin.close()
            finally:lock.release()
        else:process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        error=process.stderr.read();process.stdout.close();process.stderr.close();assert process.returncode==0,error

