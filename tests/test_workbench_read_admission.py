"""The actual browser admission code bounds bursts and drops stale waiters."""
from pathlib import Path
import shutil
import subprocess


def test_read_burst_waits_fifo_cancels_and_releases_every_slot():
    source = (Path(__file__).resolve().parents[1] / 'demiflow/workbench/web/app.js').read_text()
    implementation = source[source.index('  let readController ='):source.index('  function pageKind(')]
    checks = r'''
const assert=require('node:assert/strict');
(async()=>{
  const current=new AbortController(), stale=new AbortController();
  await Promise.all(Array.from({length:8},()=>acquireRead(current.signal)));
  let order=[];
  const first=acquireRead(current.signal).then(()=>order.push('first'));
  const abandoned=acquireRead(stale.signal).catch(e=>assert.equal(e.name,'AbortError'));
  const last=acquireRead(current.signal).then(()=>order.push('last'));
  await Promise.resolve();assert.deepEqual(order,[]);assert.equal(activeReads,8);
  stale.abort();await abandoned;assert.equal(waitingReads.length,2);
  releaseRead();await first;assert.deepEqual(order,['first']);assert.equal(activeReads,8);
  releaseRead();await last;assert.deepEqual(order,['first','last']);assert.equal(activeReads,8);
  for(let i=0;i<8;i++)releaseRead();assert.equal(activeReads,0);
  await assert.rejects(acquireRead(stale.signal),e=>e.name==='AbortError');
  await Promise.all(Array.from({length:8},()=>acquireRead(current.signal)));
  const crowded=new AbortController();
  const pending=Array.from({length:32},()=>acquireRead(crowded.signal).catch(e=>assert.equal(e.name,'AbortError')));
  await assert.rejects(acquireRead(current.signal),/页面等待读取过多/);
  crowded.abort();await Promise.all(pending);assert.equal(waitingReads.length,0);
  for(let i=0;i<8;i++)releaseRead();assert.equal(activeReads,0);
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    result = subprocess.run([shutil.which('node') or 'node', '-e', implementation + checks],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
