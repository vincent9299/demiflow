"""Source failures that adapters raise, HTTP status checks and receipt status names."""


class SourceError(Exception):
    """A source answered, but not with usable results."""
    status = 'parse_error'


class SourceCaptcha(SourceError):
    """The source demands a CAPTCHA or another human challenge."""
    status = 'captcha'


class SourceRateLimited(SourceError):
    """The source refuses because of request rate."""
    status = 'rate_limited'


class SourceAccessDenied(SourceError):
    """The source refuses access (blocked, forbidden or payment required)."""
    status = 'access_denied'


def _cloudflare_challenge(response, text):
    if response.status_code in (429, 503):
        if '__cf_chl_jschl_tk__=' in text or (
                '/cdn-cgi/challenge-platform/' in text and 'orchestrate/jsch/v1' in text
                and 'window._cf_chl_enter(' in text):
            return True
    return response.status_code == 403 and '__cf_chl_captcha_tk__=' in text


def check_status(response):
    """Raise the matching source error for an HTTP error response (status >= 400)."""
    code = response.status_code
    if not code or code < 400:
        return
    text = response.text
    if response.headers.get('Server', '').startswith('cloudflare'):
        if _cloudflare_challenge(response, text):
            raise SourceCaptcha('Cloudflare CAPTCHA')
        if code == 403 and '<span class="cf-error-code">1020</span>' in text:
            raise SourceAccessDenied('Cloudflare Firewall')
    if code == 503 and '"https://www.google.com/recaptcha/' in text:
        raise SourceCaptcha('ReCAPTCHA')
    if code in (402, 403):
        raise SourceAccessDenied('HTTP error ' + str(code))
    if code == 429:
        raise SourceRateLimited('HTTP error 429')
    response.raise_for_status()


def classify(exc):
    """Receipt status for an exception raised while searching one source."""
    from curl_cffi.requests.exceptions import HTTPError, RequestException, Timeout
    if isinstance(exc, SourceError):
        return exc.status
    if isinstance(exc, (Timeout, TimeoutError)):
        return 'timeout'
    if isinstance(exc, HTTPError):
        code = getattr(getattr(exc, 'response', None), 'status_code', None)
        return 'authentication_error' if code == 401 else 'http_error'
    if isinstance(exc, RequestException):
        return 'network_error'
    if isinstance(exc, ImportError):
        return 'dependency_error'
    if type(exc).__name__ == 'ResponseTooLarge':
        return 'response_too_large'
    return 'parse_error'
