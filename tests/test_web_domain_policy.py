"""Independent consumer URI selection; raw transport has its own fixed policy."""
import pytest
from demiflow.collect.web import URLPolicy,normalized_blocked_domains


def test_policy_declaration_validates_without_io(tmp_path):
    policy=URLPolicy(blocked_domains=['SHOP.Example.','shop.example'])
    assert normalized_blocked_domains(['SHOP.Example.','shop.example'])==('shop.example',)
    assert policy.blocked_domain('https://SHOP.EXAMPLE.:443/')=='shop.example'
    assert policy.blocked_domain('https://notshop.example/') is None
    assert policy.blocked_domain('https://shop.example.evil.test/') is None
    for invalid in ['shop.example',['https://shop.example'],['shop.example:443'],['*.example'],[''],['bad..example'],['shop.example/path']]:
        with pytest.raises(ValueError):normalized_blocked_domains(invalid)
    assert not list(tmp_path.iterdir())


def test_ordered_path_rules_keep_manuals_and_exclude_shopping():
    policy=URLPolicy(blocked_domains=['market.example'],fetch_url_rules=[
        {'name':'manuals','action':'allow','path_pattern':r'(?i)/(manuals|support)/'},
        {'name':'shopping','action':'exclude','path_pattern':r'(?i)^/(?:[a-z]{2}(?:-[a-z]{2})?/)?(products|shop|collections)(/|$)'}])
    assert policy.exclusion_reason('https://maker.example/products/one')=='url_rule:shopping'
    assert policy.exclusion_reason('https://maker.example/products/support/manual.pdf') is None
    assert policy.exclusion_reason('https://encyclopedia.example/wiki/Products') is None
    assert policy.exclusion_reason('https://market.example/support/manual.pdf')=='blocked_domain:market.example'


def test_domain_scoped_allow_does_not_allow_same_path_on_a_shop():
    rules=[{'name':'institutional_collection','action':'allow','domains':['Museum.Example.'],'path_pattern':r'/collections/'},
           {'name':'shopping','action':'exclude','path_pattern':r'/collections/'}]
    policy=URLPolicy(fetch_url_rules=rules)
    assert policy.exclusion_reason('https://archive.museum.example/zh-hans/collections/item') is None
    assert policy.exclusion_reason('https://shop.example/zh-hans/collections/item')=='url_rule:shopping'
    assert policy.exclusion_reason('https://museum.example.evil.test/collections/item')=='url_rule:shopping'
    for domains in ([],None,['*.example'],['https://museum.example']):
        with pytest.raises(ValueError):URLPolicy(fetch_url_rules=[{'name':'bad','action':'allow','path_pattern':'/','domains':domains}])
