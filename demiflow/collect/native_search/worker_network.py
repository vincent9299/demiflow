"""Adapt the bundled network ABI to parent-owned HTTP admission and budgets."""
import asyncio
import time
from urllib.parse import urlsplit, urljoin, unquote

http_receipts = []


class ResponseTooLarge(Exception):
    pass


def install(config, send, receive):
    from searx.network.client import AsyncClient
    from searx.network.network import Network
    original_request = AsyncClient.request
    serial = asyncio.Lock()

    async def request(self, method, url, **kwargs):
        # One active HTTP exchange per adapter worker. multi_requests still
        # retains its ordered result/exception contract, under this hard bound.
        if config.get('offline_audit'):
            raise RuntimeError('Network disabled during source audit')
        async with serial:
            history = []
            follow = kwargs.pop('allow_redirects', True)
            limit = min(kwargs.pop('max_redirects', self.max_redirects), config['max_redirects'])
            if kwargs.get('stream'):
                raise ValueError('Streaming responses belong to fetch_documents')
            total_bytes = 0
            for hop in range(limit + 1):
                self.check_url(url)
                parsed = urlsplit(str(url))
                if parsed.scheme not in ('http', 'https') or not parsed.hostname:
                    raise ValueError('Invalid adapter HTTP URL')
                if parsed.username is not None:
                    if not kwargs.get('auth'):
                        kwargs['auth'] = (unquote(parsed.username), unquote(parsed.password or ''))
                    parsed = parsed._replace(netloc=parsed.netloc.rsplit('@', 1)[-1])
                    url = parsed.geturl()
                chunks = []
                hop_bytes = 0
                too_large = False
                def body(chunk):
                    nonlocal total_bytes, hop_bytes, too_large
                    total_bytes += len(chunk)
                    hop_bytes += len(chunk)
                    if total_bytes > config['max_bytes']:
                        too_large = True
                        return 0
                    chunks.append(chunk)
                    return len(chunk)
                send({'event': 'http_open', 'host': parsed.hostname, 'method': method.upper()})
                if receive().get('event') != 'http_grant':
                    raise RuntimeError('Invalid admission protocol')
                started = time.monotonic()
                code = None
                try:
                    response = await original_request(self, method, url, **{
                        **kwargs, 'allow_redirects': False, 'content_callback': body,
                        'timeout': min(float(kwargs.get('timeout') or config['timeout_s']), config['timeout_s'])})
                    code = response.status_code
                    if too_large:
                        raise ResponseTooLarge()
                    response.content = b''.join(chunks)
                    response.history = list(history)
                    http_receipts.append({'host': parsed.hostname, 'method': method.upper(),
                        'http_status': code, 'retry_after': response.headers.get('retry-after'), 'bytes': len(response.content), 'elapsed_s': time.monotonic() - started})
                except BaseException:
                    http_receipts.append({'host': parsed.hostname, 'method': method.upper(),
                        'http_status': code, 'bytes': hop_bytes, 'elapsed_s': time.monotonic() - started})
                    if too_large:
                        raise ResponseTooLarge() from None
                    raise
                finally:
                    send({'event': 'http_close', 'receipt': http_receipts[-1]})
                if not follow or code not in (301, 302, 303, 307, 308) or not response.headers.get('location'):
                    return response
                if hop == limit:
                    raise ValueError('Adapter redirect limit exceeded')
                nxt = urljoin(str(url), response.headers['location'])
                destination = urlsplit(nxt)
                if (destination.scheme, destination.netloc) != (parsed.scheme, parsed.netloc):
                    kwargs.pop('auth', None)
                    kwargs.pop('cookies', None)
                    kwargs['headers'] = {k: v for k, v in kwargs.get('headers', {}).items()
                                         if k.lower() not in ('authorization', 'proxy-authorization', 'cookie', 'host')}
                if code == 303 or code in (301, 302) and method.upper() == 'POST':
                    method = 'GET'
                    for key in ('data', 'json', 'content'):
                        kwargs.pop(key, None)
                history.append(response)
                url = nxt
            raise AssertionError('unreachable')

    async def call_client(self, stream, method, url, **kwargs):
        # Deliberately no hidden reconnect/retry. Parent receipts own each attempt.
        do_raise = Network.extract_do_raise_for_httperror(kwargs)
        client_args = Network.extract_kwargs_clients(kwargs)
        client = await self.get_client(**client_args)
        client.check_url(url)
        if stream:
            raise ValueError('Use the document fetch API for streaming')
        response = await client.request(method.upper(), url, **kwargs)
        return self.patch_response(response, do_raise)

    AsyncClient.request = request
    Network.call_client = call_client

    import searx.network
    def multi_requests(request_list):
        # Preserve the list-of-response-or-exception ABI without creating an
        # unbounded list of futures inside the otherwise bounded worker.
        responses = []
        for request in request_list:
            try:
                responses.append(searx.network.request(request.method, request.url, **request.kwargs))
            except Exception as exc:
                responses.append(exc)
        return responses
    searx.network.multi_requests = multi_requests
