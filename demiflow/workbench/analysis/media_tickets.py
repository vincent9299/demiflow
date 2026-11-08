"""Bounded HttpOnly playback tickets for the existing authenticated media API.

Not a login session or an authority capability: the caller rechecks current
project membership and the exact registered object at each range, before and
after IO. Only the X-Forge-Token-authenticated preparation path may mint or
renew. Cookie values never enter a URL, JSON response, disk, or log.
"""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
import hashlib
from http.cookies import SimpleCookie
from http.cookies import CookieError
import re
import secrets
import threading
import time

from demiflow.workbench.projects.registry import identity as project_identity
from demiflow.workbench.analysis import contracts as c

COOKIE = "DemiFlowMedia"
_TOKEN = re.compile(r"[A-Za-z0-9_-]{43}\Z", re.ASCII)


def path(project):
    return "/api/workspaces/"+project_identity(project)+"/analysis-media"


@dataclass(frozen=True)
class Ticket:
    key: str
    project: str
    principal: str
    reference_bytes: bytes
    descriptor_bytes: bytes
    expires: float
    authorization_epoch: str

    @property
    def reference(self): return c.decode(self.reference_bytes)

    @property
    def descriptor(self): return c.decode(self.descriptor_bytes)


class MediaTickets:
    MAXIMUM = 16
    PER_PROJECT = 4

    def __init__(self, *, seconds=300, clock=time.monotonic, authorization_epoch=lambda:"service-lifetime"):
        self.seconds=c.integer(seconds,"playback ticket lifetime",300,1)
        self.clock,self._lock,self._items=clock,threading.Lock(),{}
        self.authorization_epoch=authorization_epoch

    @staticmethod
    def _key(header, *, optional=False):
        if header is None or header=="":
            if optional:return None
            c.reject("Media playback ticket is absent",code="operation_rejected")
        c.text(header,"media cookie header",4096)
        count=len(re.findall(r"(?:^|;\s*)"+COOKIE+r"=",header))
        if count==0 and optional:return None
        if count!=1:
            c.reject("Media playback cookie is ambiguous",code="operation_rejected")
        cookie=SimpleCookie()
        try:cookie.load(header)
        except CookieError:c.reject("Media playback cookie is invalid",code="operation_rejected")
        value=cookie.get(COOKIE)
        if value is None or not _TOKEN.fullmatch(value.value):
            c.reject("Media playback cookie is invalid",code="operation_rejected")
        return hashlib.sha256(value.value.encode("ascii")).hexdigest()

    def _purge(self):
        now=self.clock()
        epoch=c.text(self.authorization_epoch(),"server authorization epoch",128)
        for key,value in list(self._items.items()):
            if value.expires<=now or value.authorization_epoch!=epoch:del self._items[key]

    def _owned(self, key, project, principal, *, optional=False):
        item=self._items.get(key)
        if item is None:
            if optional:return None
            c.reject("Media playback ticket expired or was revoked",code="operation_rejected")
        if item.project!=project or item.principal!=principal:
            c.reject("Media playback ticket belongs to another project/principal",code="operation_rejected")
        return item

    @staticmethod
    def cookie(value, project, *, secure, seconds):
        project_identity(project)
        if type(secure) is not bool:c.reject("Media cookie transport must be server-owned")
        cookie=SimpleCookie();cookie[COOKIE]=value;morsel=cookie[COOKIE]
        morsel["path"]=path(project);morsel["httponly"]=True;morsel["samesite"]="Strict";morsel["max-age"]=seconds
        if secure:morsel["secure"]=True
        return morsel.OutputString()

    def public(self,item):
        # Token/key are deliberately absent. Native media GET/HEAD uses only
        # this constant same-origin URL and the scoped HttpOnly cookie.
        return dict(url=path(item.project)+"?binding="+c.digest(item.reference),descriptor=item.descriptor,
            binding=item.reference["expected_binding"],expires_in=self.seconds)

    def prepare(self, project, principal, *, reference, descriptor, authorize, previous_cookie=None, secure=False):
        project_identity(project);c.identity(principal,"server principal")
        # These facts come from a successful trusted inspection, never a
        # browser-provided decoder/descriptor. Keep one immutable byte copy.
        c.sha(reference.get("expected_binding") if type(reference) is dict else None)
        raw_reference=c.encoded(reference,maximum=8192);raw_descriptor=c.encoded(descriptor,maximum=8192)
        if not callable(authorize) or authorize(reference,descriptor) is not True:
            c.reject("Current media read authority is absent",code="operation_rejected")
        prior=self._key(previous_cookie,optional=True)
        token=secrets.token_urlsafe(32);key=hashlib.sha256(token.encode("ascii")).hexdigest()
        with self._lock:
            self._purge()
            old=self._owned(prior,project,principal,optional=True)
            size=len(self._items)-(1 if old else 0)
            project_size=sum(value.project==project for value in self._items.values())-(1 if old else 0)
            if size>=self.MAXIMUM or project_size>=self.PER_PROJECT:
                c.reject("Playback ticket capacity is busy; close an existing player",code="busy")
            if old:del self._items[old.key]
            item=Ticket(key,project,principal,raw_reference,raw_descriptor,self.clock()+self.seconds,self.authorization_epoch())
            self._items[key]=item
        return self.public(item),self.cookie(token,project,secure=secure,seconds=self.seconds)

    def get(self, project, principal, cookie_header):
        project_identity(project);c.identity(principal,"server principal")
        key=self._key(cookie_header)
        with self._lock:
            self._purge()
            return self._owned(key,project,principal)

    def peek(self, project, principal, cookie_header):
        """Cleanup lookup may find an expired ticket; never acquire authority."""
        project_identity(project);c.identity(principal,"server principal")
        key=self._key(cookie_header,optional=True)
        with self._lock:
            self._purge()
            return self._owned(key,project,principal,optional=True)

    def current(self, item, authorize):
        """Check every request/range and AGAIN before returning any bytes."""
        with self._lock:
            self._purge()
            if self._items.get(item.key) is not item:
                c.reject("Original media ticket changed or ended",code="operation_rejected")
        # Never hold the small ticket mutex across permission/object IO.
        if not callable(authorize) or authorize(item.reference,item.descriptor) is not True:
            with self._lock:self._items.pop(item.key,None)
            c.reject("Media project membership or fixed object changed",code="operation_rejected")
        with self._lock:
            self._purge()
            if self._items.get(item.key) is not item:
                c.reject("Media ticket ended during authorization",code="operation_rejected")
        return item

    def renew(self, project, principal, cookie_header, *, authorize, secure=False):
        item=self.get(project,principal,cookie_header);self.current(item,authorize)
        # No new reference/descriptor argument exists: renewal cannot expand
        # the ticket to another source or object.
        with self._lock:
            self._purge()
            if self._items.get(item.key) is not item:c.reject("Original media ticket ended",code="operation_rejected")
            new=replace(item,expires=self.clock()+self.seconds);self._items[item.key]=new
        cookie=SimpleCookie();cookie.load(cookie_header)
        token=cookie[COOKIE].value
        return self.public(new),self.cookie(token,project,secure=secure,seconds=self.seconds)

    def revoke(self, project, principal, cookie_header, *, secure=False):
        project_identity(project);c.identity(principal,"server principal")
        key=self._key(cookie_header,optional=True)
        with self._lock:
            self._purge();item=self._owned(key,project,principal,optional=True)
            if item:del self._items[item.key]
        return self.cookie("",project,secure=secure,seconds=0)

    def close(self):
        with self._lock:self._items.clear()
