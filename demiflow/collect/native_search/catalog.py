"""Named search sources available without a custom ``module``.

Each entry names its adapter under ``native_search/engines`` (``engine``) and
default settings; ``SearchConfig.engines`` entries override any of them.
``disabled`` marks sources that are not part of a default selection (selection
is always explicit in ``SearchConfig``). Names are kept stable because stored
configurations and receipts refer to them.

``network`` names the network a source sends through (``worker.source_networks``):
yandex images uses the one of yandex, and brave.images and qwant images use the
networks ``brave`` and ``qwant`` defined below, as these three always did.
"""

SOURCES = (
    # web
    {'name': 'google', 'engine': 'google', 'shortcut': 'go', 'disabled': True},
    {'name': 'bing', 'engine': 'bing', 'shortcut': 'bi', 'disabled': True},
    {'name': 'yandex', 'engine': 'yandex', 'categories': 'general', 'search_type': 'web', 'shortcut': 'yd',
     'disabled': True},
    {'name': 'mwmbl', 'engine': 'mwmbl', 'shortcut': 'mwm', 'disabled': True},
    {'name': 'wikipedia', 'engine': 'wikipedia', 'shortcut': 'wp', 'display_type': ['infobox'],
     'categories': ['general']},
    {'name': 'wikisearch', 'engine': 'wikisearch', 'categories': ['general']},
    # image search engines
    {'name': 'google images', 'engine': 'google_images', 'shortcut': 'goi', 'disabled': True},
    {'name': 'bing images', 'engine': 'bing_images', 'shortcut': 'bii'},
    {'name': 'yandex images', 'engine': 'yandex', 'network': 'yandex', 'categories': 'images',
     'search_type': 'images', 'shortcut': 'ydi', 'disabled': True},
    {'name': 'baidu images', 'engine': 'baidu', 'baidu_category': 'images', 'categories': ['images'],
     'shortcut': 'bdi', 'disabled': True},
    {'name': 'brave.images', 'engine': 'brave', 'network': 'brave', 'brave_category': 'images',
     'categories': ['images', 'web'], 'shortcut': 'brimg'},
    {'name': 'sogou images', 'engine': 'sogou_images', 'shortcut': 'sogoui', 'disabled': True},
    {'name': 'quark images', 'engine': 'quark', 'quark_category': 'images', 'categories': ['images'],
     'shortcut': 'qki', 'disabled': True},
    {'name': 'duckduckgo images', 'engine': 'duckduckgo_extra', 'categories': ['images'], 'ddg_category': 'images',
     'shortcut': 'ddi'},
    {'name': 'qwant images', 'engine': 'qwant', 'network': 'qwant', 'qwant_categ': 'images',
     'categories': ['images', 'web'], 'shortcut': 'qwi', 'disabled': True},
    {'name': 'pinterest', 'engine': 'pinterest', 'shortcut': 'pin'},
    # open image collections
    {'name': 'wikicommons.images', 'engine': 'wikicommons', 'categories': 'images', 'wc_search_type': 'image',
     'shortcut': 'wci'},
    {'name': 'public domain image archive', 'engine': 'public_domain_image_archive', 'shortcut': 'pdia',
     'disabled': True},
    {'name': 'pexels', 'engine': 'pexels', 'shortcut': 'pe'},
    {'name': 'openverse', 'engine': 'openverse', 'categories': 'images', 'shortcut': 'opv'},
    {'name': 'unsplash', 'engine': 'unsplash', 'shortcut': 'us'},
)

# Names a source declaration could set on these adapters before and cannot now (adapter -> names): the
# tables behind safe search and endpoints and the selectors that read a provider's pages. A declaration
# that sets one is rejected (``SearchConfig.source_configs``) instead of being accepted without effect.
WITHDRAWN_OPTIONS = {
    'brave': ('safesearch_map',),
    'duckduckgo_extra': ('safesearch_cookies', 'search_path_map'),
    'google': ('filter_mapping', 'suggestion_xpath'),
    'google_images': ('filter_mapping',),
    'public_domain_image_archive': ('pdia_config_start', 'pdia_config_end'),
    'yandex': ('content_xpath', 'results_xpath', 'title_xpath', 'url_xpath'),
}

# Network names that are no source of this catalog: the transport defaults of the adapter they name apply.
NETWORKS = {'brave': {'engine': 'brave'}, 'qwant': {'engine': 'qwant'}}


def entries():
    """Catalog entries keyed by lowercase name (fresh copies)."""
    import copy
    return {entry['name'].lower(): copy.deepcopy(entry) for entry in SOURCES}
