"""Local JSON search fixture written as a DemiFlow native search adapter (module with request/response)."""
from urllib.parse import urlencode
base_url=''
engine_type='online'
categories=['general']
paging=False
language_support=True


def request(query,params):
    params['url']=base_url+'?'+urlencode({'q':query})


def response(response):
    return response.json()['results']
