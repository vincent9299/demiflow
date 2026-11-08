"""Bounded process output: two files, fixed chunks, one drain thread per owner."""
import os
from pathlib import Path
import selectors
import threading


class ProcessLog:
    def __init__(self, source, path, limit):
        self.source, self.path, self.limit = source, Path(path), limit
        self.stop=threading.Event(); self.error=None
        self.thread=threading.Thread(target=self._drain,name='demiflow-service-log',daemon=True)
        self.thread.start()

    def _drain(self):
        output=None
        try:
            if self.path.exists() and self.path.stat().st_size>=self.limit:
                os.replace(self.path,str(self.path)+'.1')
            output=self.path.open('ab',buffering=0); size=output.tell()
            with selectors.DefaultSelector() as selector:
                selector.register(self.source,selectors.EVENT_READ)
                while not self.stop.is_set():
                    for key,_ in selector.select(.1):
                        raw=os.read(key.fd,min(65536,self.limit))
                        if not raw: return
                        if size+len(raw)>self.limit:
                            output.close(); os.replace(self.path,str(self.path)+'.1')
                            output=self.path.open('wb',buffering=0); size=0
                        output.write(raw); size+=len(raw)
        except Exception as exc:
            self.error=exc
        finally:
            if output is not None: output.close()

    def close(self):
        # Child is already reaped. Allow the pipe's bounded residue to drain.
        self.thread.join(1)
        self.stop.set(); self.thread.join(1)
        self.source.close()
