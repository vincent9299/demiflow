"""Fixed trusted one-request HTTP worker; no tools, sessions or retry loop.

The parent validates the text-only request and supplies its configured endpoint.
Keeping blocking DNS/TLS/HTTP reads in this process makes cancellation drainable.
"""
import json
import os
import sys
import time
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('Model endpoint redirect is not allowed')


def main():
    raw = sys.stdin.buffer.read(1024 * 1024 + 8193)
    if len(raw) > 1024 * 1024 + 8192: raise ValueError('Model request exceeds budget')
    value = json.loads(raw)
    deadline = value['deadline']
    def remaining():
        left = min(180, deadline-time.time())
        if left <= 0: raise TimeoutError('Model deadline exhausted')
        return left
    request = urllib.request.Request(value['url'], data=json.dumps(value['body'],separators=(',',':')).encode(),
        headers={'Content-Type':'application/json','Authorization':'Bearer '+os.environ['DEMIFLOW_UPSTREAM_TOKEN']},method='POST')
    with urllib.request.build_opener(NoRedirect()).open(request,timeout=remaining()) as response:
        if response.status != 200: raise ValueError('Configured model request failed')
        content_type = response.headers.get('Content-Type','application/json').split(';',1)[0]
        if content_type not in {'application/json','text/event-stream'}: raise ValueError('Unsupported model response type')
        sys.stdout.buffer.write(json.dumps({'content_type':content_type}).encode()+b'\n');sys.stdout.buffer.flush()
        total = 0
        while True:
            sock = getattr(getattr(getattr(response,'fp',None),'raw',None),'_sock',None)
            if sock is not None: sock.settimeout(remaining())
            chunk = response.read1(65536)
            if not chunk: break
            total += len(chunk)
            if total > value['maximum']: raise ValueError('Model output exceeds task budget')
            sys.stdout.buffer.write(chunk);sys.stdout.buffer.flush()


if __name__ == '__main__':
    try: main()
    except BaseException:
        # Never return upstream error bodies, URLs or authentication details.
        sys.exit(1)
