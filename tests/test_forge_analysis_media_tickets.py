"""Read-ticket auth/expiry/quota; no media decoder or HTTP login replacement."""
from http.cookies import SimpleCookie

import pytest

from demiflow.workbench.analysis.media_tickets import MediaTickets
from demiflow.workbench.analysis.media_tickets import COOKIE
from demiflow.workbench.analysis.media_tickets import path
from demiflow.workbench.analysis import contracts as c
from demiflow.forge.schema import ForgeError

PROJECT='w'+'1'*32
OTHER='w'+'2'*32


def reference():return dict(execution_id='e1',expected_output_id='o'+'1'*32,expected_binding='a'*64,output_index=0,mime='video/mp4',expected_sha256='b'*64)


def descriptor():return dict(artifact_id='o'+'1'*32,sha256='b'*64,mime='video/mp4',bytes=40*1024*1024,width=320,height=240,duration_s=30,codec='h264',fps=30)


def cookie_header(set_cookie):
    cookie=SimpleCookie();cookie.load(set_cookie);return cookie.output(header='',sep=';').strip().split(';')[0]


def prepare(tickets,project=PROJECT,**kwargs):
    return tickets.prepare(project,'principal',reference=reference(),descriptor=descriptor(),authorize=lambda *_:True,**kwargs)


def test_only_cookie_contains_opaque_secret_and_it_cannot_reauthorize_other_projects():
    tickets=MediaTickets()
    public,header=prepare(tickets,secure=True,previous_cookie='unrelated=existing')
    parsed=SimpleCookie();parsed.load(header);morsel=parsed[COOKIE]
    assert morsel['httponly'] and morsel['secure'] and morsel['samesite']=='Strict'
    assert morsel['path']==path(PROJECT) and not morsel['domain'] and int(morsel['max-age'])<=300
    assert public['url']==path(PROJECT)+'?binding='+c.digest(reference())
    assert morsel.value not in str(public) and 'key' not in public
    ticket=tickets.get(PROJECT,'principal',cookie_header(header))
    with pytest.raises(ForgeError):tickets.get(OTHER,'principal',cookie_header(header))
    with pytest.raises(ForgeError):tickets.get(PROJECT,'another-principal',cookie_header(header))
    with pytest.raises(ForgeError):tickets.get(PROJECT,'principal',cookie_header(header)+'; '+cookie_header(header))
    assert tickets.current(ticket,lambda *_:True) is ticket


def test_each_range_rechecks_current_read_authority_and_late_results_do_not_survive_revoke():
    tickets=MediaTickets();_,header=prepare(tickets);cookie=cookie_header(header)
    ticket=tickets.get(PROJECT,'principal',cookie)
    assert tickets.current(ticket,lambda ref,desc:ref==reference() and desc==descriptor())
    tickets.revoke(PROJECT,'principal',cookie)
    with pytest.raises(ForgeError):tickets.current(ticket,lambda *_:True)
    _,header=prepare(tickets);ticket=tickets.get(PROJECT,'principal',cookie_header(header))
    with pytest.raises(ForgeError):tickets.current(ticket,lambda *_:False)
    with pytest.raises(ForgeError):tickets.get(PROJECT,'principal',cookie_header(header))


def test_renewal_keeps_exact_object_old_range_fence_and_hard_expiry():
    now=[0.];tickets=MediaTickets(seconds=10,clock=lambda:now[0]);_,header=prepare(tickets);cookie=cookie_header(header)
    old=tickets.get(PROJECT,'principal',cookie);now[0]=8.
    value,renewed=tickets.renew(PROJECT,'principal',cookie,authorize=lambda ref,desc:ref==reference() and desc==descriptor())
    assert value['expires_in']==10 and cookie_header(renewed)==cookie
    with pytest.raises(ForgeError):tickets.current(old,lambda *_:True)
    item=tickets.get(PROJECT,'principal',cookie);assert item.reference==reference()
    now[0]=19.
    with pytest.raises(ForgeError):tickets.current(item,lambda *_:True)
    with pytest.raises(ForgeError):tickets.get(PROJECT,'principal',cookie)


def test_total_and_per_project_budgets_preserve_other_active_players():
    tickets=MediaTickets();headers=[]
    for _ in range(4):headers.append(prepare(tickets)[1])
    with pytest.raises(ForgeError) as error:prepare(tickets)
    assert error.value.code=='busy'
    # Explicit preparation on the same browser cookie replaces only that
    # browser's same-project player; it cannot discard another active ticket.
    _,new=prepare(tickets,previous_cookie=cookie_header(headers[0]))
    with pytest.raises(ForgeError):tickets.get(PROJECT,'principal',cookie_header(headers[0]))
    assert tickets.get(PROJECT,'principal',cookie_header(headers[1]))
    for index in range(3):
        for _ in range(4):prepare(tickets,'w'+str(index+3)*32)
    with pytest.raises(ForgeError):prepare(tickets,'w'+'f'*32)
    tickets.close()
    with pytest.raises(ForgeError):tickets.get(PROJECT,'principal',cookie_header(new))


def test_console_authorization_rotation_invalidates_native_cookie_without_exposing_token():
    epoch=['old'];tickets=MediaTickets(authorization_epoch=lambda:epoch[0]);public,header=prepare(tickets)
    item=tickets.get(PROJECT,'principal',cookie_header(header));epoch[0]='new'
    with pytest.raises(ForgeError):tickets.current(item,lambda *_:True)
    with pytest.raises(ForgeError):tickets.get(PROJECT,'principal',cookie_header(header))
    assert 'old' not in str(public) and 'new' not in str(public)
