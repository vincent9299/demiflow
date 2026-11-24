"""Language tags of search requests mapped to source-specific codes.

Each source has a general rule and two tables fitted to the requests the
previous runtime produced for every known language tag and for its lower- and
upper-case spellings: ``*_EXCEPTIONS`` keyed by the tag as written and
``*_FOLDED`` keyed by the lower-cased tag (any other spelling). Lookup order:
as written, other spelling, rule (``_lookup``). Tags babel cannot parse never
reach this module (``config.validate_language`` rejects them).
"""
import babel

# Wikipedia editions by language (and script). Derived from the hosts the
# previous runtime chose for every babel locale and its other spellings;
# ``wikipedia_host`` reproduces all of them (table value None: no edition).
WIKIPEDIA_CODES = {
    'af': 'af', 'am': 'am', 'an': 'an', 'ar': 'ar', 'as': 'as', 'az': 'az', 'ba': 'ba', 'be': 'be', 'bg': 'bg',
    'bn': 'bn', 'bo': 'zh', 'bs': 'bs', 'ca': 'ca', 'ckb': 'ckb', 'cs': 'cs', 'cy': 'cy', 'da': 'da', 'de':
    'de', 'el': 'el', 'en': 'en', 'es': 'es', 'et': 'et', 'fa': 'fa', 'fi': 'fi', 'fil': 'tl', 'fo': 'fo', 'fr':
    'fr', 'fy': 'fy', 'gl': 'gl', 'gsw': 'als', 'gu': 'gu', 'he': 'he', 'hi': 'hi', 'hr': 'hr', 'hsb': 'hsb',
    'hu': 'hu', 'hy': 'hy', 'id': 'id', 'ii': 'zh', 'is': 'is', 'it': 'it', 'ja': 'ja', 'jv': 'jv', 'ka': 'ka',
    'kaa': 'kaa', 'kk-Arab': 'zh', 'km': 'km', 'kn': 'kn', 'ko': 'ko', 'ku': 'ku', 'lb': 'lb', 'lij': 'lij',
    'lt': 'lt', 'lv': 'lv', 'mai': 'mai', 'mk': 'mk', 'ml': 'ml', 'mn': 'mn', 'mn-Mong': 'zh', 'mr': 'mr', 'ms':
    'zh', 'nb': 'no', 'ne': 'ne', 'or': 'or', 'os': 'os', 'pa': 'pa', 'pl': 'pl', 'ps': 'ps', 'pt': 'pt', 'qu':
    'qu', 'ro': 'ro', 'ru': 'ru', 'sa': 'sa', 'sah': 'sah', 'sd': 'sd', 'si': 'si', 'sk': 'sk', 'sl': 'sl',
    'so': 'so', 'sq': 'sq', 'sr': 'sr', 'ta': 'ta', 'te': 'te', 'th': 'th', 'tr': 'tr', 'ug': 'zh', 'uk': 'uk',
    'ur': 'ur', 'uz': 'uz', 'vi': 'vi', 'vo': 'vo', 'wa': 'wa', 'yi': 'yi', 'yue-Hans': 'zh', 'yue-Hant': 'zh',
    'za': 'zh', 'zgh': 'zgh', 'zh': 'zh', 'zh-Hans': 'zh', 'zh-Hant': 'zh', 'zh-Latn': 'zh'
}
WIKIPEDIA_EXCEPTIONS = {}
WIKIPEDIA_FOLDED = {
    'bo': None, 'bo-in': None, 'en-sg': 'zh.wikipedia.org', 'ii': None, 'kk-arab': None, 'ko-cn':
    'zh.wikipedia.org', 'mn-mong': None, 'mn-mong-mn': None, 'ms': None, 'ms-bn': None, 'ms-id': None, 'ms-my':
    None, 'ta-sg': 'zh.wikipedia.org', 'ug': None, 'yue-hk': None, 'yue-hans': None, 'yue-hant': None,
    'yue-hant-hk': None, 'yue-hant-mo': None, 'yue-mo': None, 'za': None, 'zh-latn': None
}


_MISSING = object()


def _lookup(exact, folded, tag):
    """Table value for a tag: as written first, then any other letter case of it.

    The previous runtime looked a tag up as written and otherwise resolved its
    babel-normalized form, so ``zh-TW`` and ``zh-tw`` can map differently.
    ``exact`` is keyed by tags as written, ``folded`` by lower-cased tags.
    """
    if tag in exact:
        return exact[tag]
    return folded.get(tag.lower(), _MISSING)


def parse(tag):
    """babel.Locale for a request language tag, or None for ``all``/unknown tags."""
    if not tag or tag == 'all':
        return None
    try:
        return babel.Locale.parse(tag, sep='-')
    except (ValueError, babel.UnknownLocaleError):
        return None


def wikipedia_host(tag, default='en.wikipedia.org'):
    """Host of the Wikipedia edition for a request language; ``default`` when none is declared."""
    if not tag or tag == 'all':
        return default
    host = _lookup(WIKIPEDIA_EXCEPTIONS, WIKIPEDIA_FOLDED, tag)
    if host is _MISSING:
        host = _wikipedia_rule(tag)
    return host or default


def _wikipedia_rule(tag):
    locale = parse(tag)
    if locale is None:
        return None
    key = locale.language + ('-' + locale.script if locale.script else '')
    code = WIKIPEDIA_CODES.get(key) or (WIKIPEDIA_CODES.get(locale.language) if not locale.script else None)
    return code + '.wikipedia.org' if code else None


# Google interface languages and search regions. Derived from the request
# parameters the previous runtime produced for 1134 language tags (every babel
# locale plus the common language-region pairs) and their other spellings;
# ``google_parameters`` reproduces all of them.
GOOGLE_LANGUAGES = frozenset((
    'af', 'ak', 'am', 'ar', 'az', 'be', 'bem', 'bg', 'bn', 'br', 'bs', 'ca', 'ceb', 'chr', 'ckb', 'co', 'cs',
    'cy', 'da', 'de', 'ee', 'el', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'fi', 'fil', 'fo', 'fr', 'fy', 'ga',
    'gaa', 'gd', 'gl', 'gn', 'gu', 'ha', 'haw', 'hi', 'hr', 'ht', 'hu', 'hy', 'ia', 'id', 'ig', 'is', 'it',
    'ja', 'ka', 'kk', 'km', 'kn', 'ko', 'ky', 'la', 'lg', 'ln', 'lo', 'lt', 'lv', 'mfe', 'mg', 'mi', 'mk', 'ml',
    'mn', 'mr', 'ms', 'mt', 'my', 'ne', 'nl', 'nn', 'nso', 'ny', 'nyn', 'oc', 'om', 'or', 'pa', 'pcm', 'pl',
    'ps', 'qu', 'rm', 'rn', 'ro', 'ru', 'rw', 'sd', 'si', 'sk', 'sl', 'sn', 'so', 'sq', 'sr', 'st', 'su', 'sv',
    'sw', 'ta', 'te', 'tg', 'th', 'ti', 'tk', 'tn', 'to', 'tr', 'tt', 'ug', 'uk', 'ur', 'uz', 'vi', 'wo', 'xh',
    'yi', 'yo', 'zu'
))
GOOGLE_LANGUAGE_CODES = {'he': 'iw', 'jv': 'jw', 'nb': 'no', 'pt': 'pt-BR', 'sr-Latn': 'sr-ME', 'zh': 'zh-CN', 'zh-Hans': 'zh-CN', 'zh-Hant': 'zh-TW'}
GOOGLE_REGIONS = frozenset((
    'AD', 'AE', 'AF', 'AG', 'AM', 'AO', 'AR', 'AS', 'AT', 'AU', 'BA', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BO',
    'BR', 'BS', 'BW', 'BY', 'BZ', 'CA', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CO', 'CR', 'CU', 'CV',
    'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ', 'EC', 'EE', 'EG', 'ES', 'FI', 'FJ', 'FM', 'FR', 'GA', 'GB',
    'GG', 'GH', 'GI', 'GM', 'GR', 'GT', 'GY', 'HK', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IQ',
    'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KI', 'KR', 'KW', 'KZ', 'LB', 'LI', 'LS', 'LT', 'LU', 'LV',
    'LY', 'MA', 'MD', 'MG', 'ML', 'MT', 'MU', 'MW', 'MX', 'MZ', 'NA', 'NE', 'NG', 'NI', 'NL', 'NO', 'NR', 'NU',
    'NZ', 'OM', 'PA', 'PE', 'PG', 'PH', 'PK', 'PL', 'PN', 'PR', 'PS', 'PT', 'PY', 'QA', 'RO', 'RS', 'RU', 'RW',
    'SA', 'SB', 'SC', 'SE', 'SG', 'SH', 'SI', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'ST', 'SV', 'TD', 'TG', 'TH',
    'TL', 'TN', 'TO', 'TR', 'TT', 'TW', 'TZ', 'UA', 'UG', 'US', 'UY', 'VC', 'VE', 'VI', 'VN', 'VU', 'WS', 'ZA',
    'ZM', 'ZW'
))
# Region used when a tag names a territory Google does not offer.
GOOGLE_DEFAULT_REGIONS = {
    'aa': 'ZZ', 'ab': 'ZZ', 'am': 'ZZ', 'apc': 'ZZ', 'ar': 'JO', 'az': 'ZZ', 'bgn': 'ZZ', 'bn': 'IN', 'bo':
    'HK', 'byn': 'ZZ', 'ccp': 'ZZ', 'ckb': 'IQ', 'da': 'DK', 'dv': 'ZZ', 'dz': 'ZZ', 'en': 'US', 'eo': 'ZZ',
    'es': 'ES', 'fa': 'AF', 'ff': 'ZZ', 'fo': 'ZZ', 'fr': 'FR', 'gez': 'ZZ', 'ha': 'ZZ', 'ia': 'ZZ', 'ii': 'HK',
    'io': 'ZZ', 'it': 'IT', 'jbo': 'ZZ', 'ka': 'ZZ', 'kaa': 'ZZ', 'kk': 'HK', 'kl': 'ZZ', 'km': 'ZZ', 'ko':
    'HK', 'kpe': 'ZZ', 'la': 'ZZ', 'lo': 'ZZ', 'lrc': 'ZZ', 'mk': 'ZZ', 'mn': 'ZZ', 'ms': 'ZZ', 'my': 'ZZ',
    'mzn': 'ZZ', 'nb': 'NO', 'ne': 'IN', 'nl': 'NL', 'nqo': 'ZZ', 'nus': 'ZZ', 'om': 'ZZ', 'os': 'ZZ', 'pap':
    'ZZ', 'pt': 'PT', 'rhg': 'ZZ', 'sdh': 'ZZ', 'shn': 'ZZ', 'si': 'ZZ', 'sid': 'ZZ', 'so': 'SO', 'sq': 'ZZ',
    'sr': 'ZZ', 'ss': 'ZZ', 'ssy': 'ZZ', 'sv': 'SE', 'syr': 'ZZ', 'ta': 'IN', 'tg': 'ZZ', 'ti': 'ZZ', 'tig':
    'ZZ', 'tk': 'ZZ', 'tok': 'ZZ', 'ug': 'HK', 'uz': 'ZZ', 'vai': 'ZZ', 'vo': 'ZZ', 'wal': 'ZZ', 'yue': 'HK',
    'za': 'HK', 'zh': 'HK'
}
GOOGLE_EXCEPTIONS = {
    'zh-TW': ('zh-TW', 'lang_zh-TW', 'countryTW')
}
GOOGLE_FOLDED = {
    'be-tarask': ('be', 'lang_be', 'countryBY'), 'bs-cyrl': ('en', 'lang_en', 'countryBA'), 'el-polyton': ('el',
    'lang_el', 'countryGR'), 'ko-kp': ('ko', 'lang_ko', 'countryKR'), 'mn-cn': ('en', 'lang_en', 'countryHK'),
    'mn-mong-cn': ('en', 'lang_en', 'countryHK'), 'ms-bn': ('ms', 'lang_ms', 'countrySG'), 'ms-my': ('ms',
    'lang_ms', 'countrySG'), 'sd-deva': ('en', 'lang_en', 'countryIN'), 'sr-latn': ('sr-ME', 'lang_sr-ME',
    'countryRS'), 'sr-latn-me': ('sr-ME', 'lang_sr-ME', 'countryRS'), 'sr-latn-xk': ('sr-ME', 'lang_sr-ME',
    'countryRS'), 'sr-me': ('sr-ME', 'lang_sr-ME', 'countryRS'), 'trv-tw': ('en', 'lang_en', 'countryZZ'),
    'yue-hant-mo': ('en', 'lang_en', 'countryZZ'), 'yue-mo': ('en', 'lang_en', 'countryZZ'), 'zh-hans-mo':
    ('zh-CN', 'lang_zh-CN', 'countryZZ'), 'zh-hans-my': ('zh-CN', 'lang_zh-CN', 'countryZZ'), 'zh-hant':
    ('zh-TW', 'lang_zh-TW', 'countryHK'), 'zh-hant-tw': ('zh-TW', 'lang_zh-TW', 'countryHK'), 'zh-my': ('zh-CN',
    'lang_zh-CN', 'countryZZ'), 'zh-tw': ('zh-TW', 'lang_zh-TW', 'countryHK')
}


def google_parameters(tag):
    """Google ``hl``, ``lr``, ``cr``, ``ie`` and ``oe`` query arguments for a request language."""
    if not tag or tag == 'all':
        return {'hl': 'ZZ', 'lr': '', 'cr': '', 'ie': 'utf8', 'oe': 'utf8'}
    values = _lookup(GOOGLE_EXCEPTIONS, GOOGLE_FOLDED, tag)
    hl, lr, cr = _google_rule(tag) if values is _MISSING else values
    return {'hl': hl, 'lr': lr, 'cr': cr, 'ie': 'utf8', 'oe': 'utf8'}


def _google_rule(tag):
    locale = babel.Locale.parse(tag, sep='-')
    key = locale.language + ('-' + locale.script if locale.script else '')
    if key in GOOGLE_LANGUAGES or key in GOOGLE_LANGUAGE_CODES:
        hl = GOOGLE_LANGUAGE_CODES.get(key, key)
    elif not locale.script and (locale.language in GOOGLE_LANGUAGES or locale.language in GOOGLE_LANGUAGE_CODES):
        hl = GOOGLE_LANGUAGE_CODES.get(locale.language, locale.language)
    else:
        hl = 'en'
    if len(tag.split('-')) == 1:
        cr = ''
    elif locale.territory and locale.territory in GOOGLE_REGIONS:
        cr = 'country' + locale.territory
    elif locale.territory:
        cr = 'country' + GOOGLE_DEFAULT_REGIONS.get(locale.language, 'ZZ')
    else:
        cr = 'countryZZ'
    return hl, 'lang_' + hl, cr


# Bing markets (``mkt``). Derived like the Google tables from the previous
# runtime's requests; ``bing_market`` reproduces all of them. A tag naming a territory uses that territory's market when Bing has no
# market for the tag's own language there.
BING_MARKETS = frozenset((
    'am-et', 'ar-ae', 'ar-bh', 'ar-dj', 'ar-dz', 'ar-eg', 'ar-er', 'ar-il', 'ar-iq', 'ar-jo', 'ar-km', 'ar-kw',
    'ar-lb', 'ar-ly', 'ar-ma', 'ar-mr', 'ar-om', 'ar-ps', 'ar-qa', 'ar-sa', 'ar-sd', 'ar-so', 'ar-sy', 'ar-td',
    'ar-tn', 'ar-ye', 'az-az', 'be-by', 'bg-bg', 'bn-bd', 'bs-ba', 'ca-ad', 'cs-cz', 'da-dk', 'de-at', 'de-be',
    'de-ch', 'de-de', 'de-li', 'de-lu', 'dv-mv', 'dz-bt', 'el-cy', 'el-gr', 'en-ag', 'en-ai', 'en-as', 'en-au',
    'en-bb', 'en-bi', 'en-bm', 'en-bs', 'en-bw', 'en-bz', 'en-ca', 'en-cc', 'en-ck', 'en-cm', 'en-cx', 'en-dm',
    'en-er', 'en-fj', 'en-fk', 'en-fm', 'en-gb', 'en-gd', 'en-gg', 'en-gh', 'en-gi', 'en-gm', 'en-gu', 'en-gy',
    'en-hk', 'en-ie', 'en-in', 'en-je', 'en-jm', 'en-ke', 'en-ki', 'en-kn', 'en-ky', 'en-lc', 'en-lr', 'en-ls',
    'en-mg', 'en-mh', 'en-mp', 'en-ms', 'en-mt', 'en-mu', 'en-mw', 'en-na', 'en-nf', 'en-ng', 'en-nr', 'en-nu',
    'en-nz', 'en-pg', 'en-ph', 'en-pk', 'en-pn', 'en-pr', 'en-pw', 'en-rw', 'en-sb', 'en-sc', 'en-sd', 'en-sg',
    'en-sh', 'en-sl', 'en-ss', 'en-sx', 'en-sz', 'en-tc', 'en-tk', 'en-to', 'en-tt', 'en-tv', 'en-tz', 'en-ug',
    'en-us', 'en-vc', 'en-vg', 'en-vi', 'en-vu', 'en-ws', 'en-za', 'en-zm', 'en-zw', 'es-ar', 'es-bo', 'es-cl',
    'es-co', 'es-cr', 'es-do', 'es-ec', 'es-es', 'es-gq', 'es-gt', 'es-hn', 'es-mx', 'es-ni', 'es-pa', 'es-pe',
    'es-pr', 'es-py', 'es-sv', 'es-uy', 'es-ve', 'et-ee', 'fa-af', 'fa-ir', 'fi-fi', 'fil-ph', 'fo-fo', 'fr-be',
    'fr-bf', 'fr-bi', 'fr-bj', 'fr-bl', 'fr-ca', 'fr-cd', 'fr-cf', 'fr-cg', 'fr-ch', 'fr-ci', 'fr-cm', 'fr-dj',
    'fr-dz', 'fr-fr', 'fr-ga', 'fr-gf', 'fr-gn', 'fr-gp', 'fr-gq', 'fr-ht', 'fr-km', 'fr-lu', 'fr-ma', 'fr-mc',
    'fr-mf', 'fr-mg', 'fr-ml', 'fr-mq', 'fr-mu', 'fr-nc', 'fr-ne', 'fr-pf', 'fr-pm', 'fr-re', 'fr-rw', 'fr-sc',
    'fr-sn', 'fr-td', 'fr-tg', 'fr-tn', 'fr-vu', 'fr-wf', 'fr-yt', 'ga-ie', 'gn-py', 'gsw-ch', 'gsw-li',
    'he-il', 'hi-in', 'hr-ba', 'hr-hr', 'ht-ht', 'hu-hu', 'hy-am', 'id-id', 'is-is', 'it-ch', 'it-it', 'it-sm',
    'it-va', 'ja-jp', 'ka-ge', 'kk-kz', 'kl-gl', 'km-kh', 'ko-kr', 'ky-kg', 'lb-lu', 'lo-la', 'lt-lt', 'lv-lv',
    'mg-mg', 'mi-nz', 'mk-mk', 'mn-mn', 'ms-bn', 'ms-my', 'ms-sg', 'mt-mt', 'my-mm', 'nb-no', 'nd-zw', 'ne-np',
    'nl-aw', 'nl-be', 'nl-cw', 'nl-nl', 'nl-sr', 'nl-sx', 'nn-no', 'ny-mw', 'pap-aw', 'pap-cw', 'pl-pl',
    'ps-af', 'pt-ao', 'pt-br', 'pt-cv', 'pt-gq', 'pt-gw', 'pt-mo', 'pt-mz', 'pt-pt', 'pt-st', 'qu-bo', 'qu-ec',
    'qu-pe', 'rn-bi', 'ro-md', 'ro-ro', 'ru-by', 'ru-kg', 'ru-kz', 'ru-ru', 'ru-ua', 'rw-rw', 'sg-cf', 'si-lk',
    'sk-sk', 'sl-si', 'sn-zw', 'so-so', 'sq-al', 'sr-ba', 'sr-me', 'sr-rs', 'ss-sz', 'st-ls', 'sv-fi', 'sv-se',
    'sw-ke', 'sw-tz', 'sw-ug', 'ta-lk', 'ta-sg', 'tg-tj', 'th-th', 'ti-er', 'tk-tm', 'tn-bw', 'to-to', 'tpi-pg',
    'tr-cy', 'tr-tr', 'tzm-ma', 'uk-ua', 'ur-pk', 'uz-uz', 'vi-vn', 'wo-sn', 'yo-ng', 'yue-mo', 'zh-cn',
    'zh-mo', 'zh-sg', 'zh-tw'
))
BING_LANGUAGE_MARKETS = {
    'aa': None, 'ab': None, 'af': None, 'agq': None, 'ak': None, 'am': 'am-et', 'an': None, 'ann': None, 'apc':
    None, 'ar': 'ar-jo', 'arn': None, 'as': None, 'asa': None, 'ast': None, 'az': 'az-az', 'ba': None, 'bal':
    None, 'bas': None, 'be': 'be-by', 'bem': None, 'bew': None, 'bez': None, 'bg': 'bg-bg', 'bgc': None, 'bgn':
    None, 'bho': None, 'blo': None, 'blt': None, 'bm': None, 'bn': 'bn-bd', 'bo': None, 'br': None, 'brx': None,
    'bs': 'bs-ba', 'bss': None, 'byn': None, 'ca': 'ca-ad', 'cad': None, 'cch': None, 'ccp': None, 'ce': None,
    'ceb': None, 'cgg': None, 'cho': None, 'chr': None, 'cic': None, 'ckb': None, 'co': None, 'cop': None, 'cs':
    'cs-cz', 'csw': None, 'cu': None, 'cv': None, 'cy': None, 'da': 'da-dk', 'dav': None, 'de': 'de-de', 'dje':
    None, 'doi': None, 'dsb': None, 'dua': None, 'dv': 'dv-mv', 'dyo': None, 'dz': 'dz-bt', 'ebu': None, 'ee':
    None, 'el': 'el-gr', 'en': 'en-us', 'eo': None, 'es': 'es-es', 'et': 'et-ee', 'eu': None, 'ewo': None, 'fa':
    'fa-ir', 'ff': None, 'fi': 'fi-fi', 'fil': 'fil-ph', 'fo': 'fo-fo', 'fr': 'fr-fr', 'frr': None, 'fur': None,
    'fy': None, 'ga': 'ga-ie', 'gaa': None, 'gd': None, 'gez': None, 'gl': None, 'gn': 'gn-py', 'gsw': 'gsw-li',
    'gu': None, 'guz': None, 'gv': None, 'ha': None, 'haw': None, 'he': 'he-il', 'hi': 'hi-in', 'hnj': None,
    'hr': 'hr-hr', 'hsb': None, 'ht': 'ht-ht', 'hu': 'hu-hu', 'hy': 'hy-am', 'ia': None, 'id': 'id-id', 'ie':
    None, 'ig': None, 'ii': None, 'io': None, 'is': 'is-is', 'it': 'it-it', 'iu': None, 'ja': 'ja-jp', 'jbo':
    None, 'jgo': None, 'jmc': None, 'jv': None, 'ka': 'ka-ge', 'kaa': None, 'kab': None, 'kaj': None, 'kam':
    None, 'kcg': None, 'kde': None, 'kea': None, 'ken': None, 'kgp': None, 'khq': None, 'ki': None, 'kk':
    'kk-kz', 'kkj': None, 'kl': 'kl-gl', 'kln': None, 'km': 'km-kh', 'kn': None, 'ko': 'ko-kr', 'kok': None,
    'kpe': None, 'ks': None, 'ksb': None, 'ksf': None, 'ksh': None, 'ku': None, 'kw': None, 'kxv': None, 'ky':
    'ky-kg', 'la': None, 'lag': None, 'lb': 'lb-lu', 'lg': None, 'lij': None, 'lkt': None, 'lld': None, 'lmo':
    None, 'ln': None, 'lo': 'lo-la', 'lrc': None, 'lt': 'lt-lt', 'ltg': None, 'lu': None, 'luo': None, 'luy':
    None, 'lv': 'lv-lv', 'mai': None, 'mas': None, 'mdf': None, 'mer': None, 'mfe': None, 'mg': 'mg-mg', 'mgh':
    None, 'mgo': None, 'mhn': None, 'mi': 'mi-nz', 'mic': None, 'mk': 'mk-mk', 'ml': None, 'mn': 'mn-mn', 'mni':
    None, 'moh': None, 'mr': None, 'ms': 'ms-bn', 'mt': 'mt-mt', 'mua': None, 'mus': None, 'my': 'my-mm', 'myv':
    None, 'mzn': None, 'naq': None, 'nb': 'nb-no', 'nd': 'nd-zw', 'nds': None, 'ne': 'ne-np', 'nl': 'nl-nl',
    'nmg': None, 'nn': 'nn-no', 'nnh': None, 'no': None, 'nqo': None, 'nr': None, 'nso': None, 'nus': None,
    'nv': None, 'ny': 'ny-mw', 'nyn': None, 'oc': None, 'om': None, 'or': None, 'os': None, 'osa': None, 'pa':
    None, 'pap': 'pap-cw', 'pcm': None, 'pis': None, 'pl': 'pl-pl', 'prg': None, 'ps': 'ps-af', 'pt': 'pt-pt',
    'qu': 'qu-bo', 'quc': None, 'raj': None, 'rhg': None, 'rif': None, 'rm': None, 'rn': 'rn-bi', 'ro': 'ro-ro',
    'rof': None, 'ru': 'ru-ru', 'rw': 'rw-rw', 'rwk': None, 'sa': None, 'sah': None, 'saq': None, 'sat': None,
    'sbp': None, 'sc': None, 'scn': None, 'sd': None, 'sdh': None, 'se': None, 'seh': None, 'ses': None, 'sg':
    'sg-cf', 'shi': None, 'shn': None, 'si': 'si-lk', 'sid': None, 'sk': 'sk-sk', 'skr': None, 'sl': 'sl-si',
    'sma': None, 'smj': None, 'smn': None, 'sms': None, 'sn': 'sn-zw', 'so': 'so-so', 'sq': 'sq-al', 'sr':
    'sr-rs', 'ss': 'ss-sz', 'ssy': None, 'st': 'st-ls', 'su': None, 'sv': 'sv-se', 'sw': 'sw-tz', 'syr': None,
    'szl': None, 'ta': 'ta-lk', 'te': None, 'teo': None, 'tg': 'tg-tj', 'th': 'th-th', 'ti': 'ti-er', 'tig':
    None, 'tk': 'tk-tm', 'tn': 'tn-bw', 'to': 'to-to', 'tok': None, 'tpi': 'tpi-pg', 'tr': 'tr-tr', 'trv': None,
    'trw': None, 'ts': None, 'tt': None, 'twq': None, 'tyv': None, 'tzm': 'tzm-ma', 'ug': None, 'uk': 'uk-ua',
    'ur': 'ur-pk', 'uz': 'uz-uz', 'vai': None, 've': None, 'vec': None, 'vi': 'vi-vn', 'vmw': None, 'vo': None,
    'vun': None, 'wa': None, 'wae': None, 'wal': None, 'wbp': None, 'wo': 'wo-sn', 'xh': None, 'xnr': None,
    'xog': None, 'yav': None, 'yi': None, 'yo': 'yo-ng', 'yrl': None, 'yue': 'yue-mo', 'za': None, 'zgh': None,
    'zh': 'zh-cn', 'zu': None
}
BING_TERRITORY_MARKETS = {
    'AE': 'ar-ae', 'AF': 'fa-af', 'AO': 'pt-ao', 'AT': 'de-at', 'AU': 'en-au', 'BD': 'bn-bd', 'BE': 'nl-be',
    'BF': 'fr-bf', 'BJ': 'fr-bj', 'BR': 'pt-br', 'BZ': 'en-bz', 'CA': 'en-ca', 'CD': 'fr-cd', 'CF': 'sg-cf',
    'CG': 'fr-cg', 'CH': 'de-ch', 'CL': 'es-cl', 'CM': 'fr-cm', 'CN': 'zh-cn', 'CO': 'es-co', 'CV': 'pt-cv',
    'CY': 'el-cy', 'CZ': 'cs-cz', 'DE': 'de-de', 'DJ': 'fr-dj', 'DK': 'da-dk', 'DZ': 'ar-dz', 'EE': 'et-ee',
    'EG': 'ar-eg', 'ER': 'ti-er', 'ES': 'es-es', 'ET': 'am-et', 'FI': 'fi-fi', 'FR': 'fr-fr', 'GB': 'en-gb',
    'GE': 'ka-ge', 'GH': 'en-gh', 'GL': 'kl-gl', 'GM': 'en-gm', 'GN': 'fr-gn', 'GT': 'es-gt', 'GW': 'pt-gw',
    'HK': 'en-hk', 'HU': 'hu-hu', 'ID': 'id-id', 'IL': 'he-il', 'IN': 'hi-in', 'IQ': 'ar-iq', 'IR': 'fa-ir',
    'IT': 'it-it', 'KE': 'sw-ke', 'LR': 'en-lr', 'LU': 'fr-lu', 'LV': 'lv-lv', 'MA': 'ar-ma', 'MD': 'ro-md',
    'MK': 'mk-mk', 'ML': 'fr-ml', 'MM': 'my-mm', 'MO': 'yue-mo', 'MR': 'ar-mr', 'MU': 'fr-mu', 'MV': 'dv-mv',
    'MY': 'ms-my', 'MZ': 'pt-mz', 'NA': 'en-na', 'NE': 'fr-ne', 'NG': 'en-ng', 'NL': 'nl-nl', 'NO': 'nb-no',
    'OM': 'ar-om', 'PH': 'en-ph', 'PK': 'ur-pk', 'PL': 'pl-pl', 'PT': 'pt-pt', 'RO': 'ro-ro', 'RU': 'ru-ru',
    'SB': 'en-sb', 'SD': 'en-sd', 'SE': 'sv-se', 'SI': 'sl-si', 'SK': 'sk-sk', 'SL': 'en-sl', 'SN': 'wo-sn',
    'SS': 'en-ss', 'SY': 'ar-sy', 'TG': 'fr-tg', 'TH': 'th-th', 'TR': 'tr-tr', 'TZ': 'sw-tz', 'UA': 'uk-ua',
    'UG': 'sw-ug', 'US': 'en-us', 'UZ': 'uz-uz', 'VA': 'it-va', 'VE': 'es-ve', 'VN': 'vi-vn', 'ZA': 'en-za',
    'ZM': 'en-zm'
}
BING_EXCEPTIONS = {
    'ar-DJ': 'ar-dj', 'ar-ER': 'ar-er', 'ar-IL': 'ar-il', 'ar-SD': 'ar-sd', 'ar-SO': 'ar-so', 'de-BE': 'de-be',
    'de-LU': 'de-lu', 'en-BI': 'en-bi', 'en-CM': 'en-cm', 'en-ER': 'en-er', 'en-IN': 'en-in', 'en-KE': 'en-ke',
    'en-LS': 'en-ls', 'en-MG': 'en-mg', 'en-MT': 'en-mt', 'en-MU': 'en-mu', 'en-MW': 'en-mw', 'en-PG': 'en-pg',
    'en-PK': 'en-pk', 'en-PR': 'en-pr', 'en-RW': 'en-rw', 'en-SC': 'en-sc', 'en-TO': 'en-to', 'en-TZ': 'en-tz',
    'en-UG': 'en-ug', 'en-ZW': 'en-zw', 'es-PY': 'es-py', 'fil-PH': 'fil-ph', 'fr-BE': 'fr-be', 'fr-BI':
    'fr-bi', 'fr-CA': 'fr-ca', 'fr-CF': 'fr-cf', 'fr-CH': 'fr-ch', 'fr-DZ': 'fr-dz', 'fr-GQ': 'fr-gq', 'fr-HT':
    'fr-ht', 'fr-KM': 'fr-km', 'fr-MA': 'fr-ma', 'fr-MG': 'fr-mg', 'fr-RW': 'fr-rw', 'fr-SN': 'fr-sn', 'fr-TD':
    'fr-td', 'fr-TN': 'fr-tn', 'fr-VU': 'fr-vu', 'ga-IE': 'ga-ie', 'gsw-CH': 'gsw-ch', 'gsw-LI': 'gsw-li',
    'hr-BA': 'hr-ba', 'it-CH': 'it-ch', 'kk-KZ': 'kk-kz', 'lb-LU': 'lb-lu', 'mi-NZ': 'mi-nz', 'ms-SG': 'ms-sg',
    'nd-ZW': 'nd-zw', 'nl-CW': 'nl-cw', 'nl-SX': 'nl-sx', 'nn-NO': 'nn-no', 'pap-AW': 'pap-aw', 'ps-AF':
    'ps-af', 'pt-GQ': 'pt-gq', 'pt-MO': 'pt-mo', 'qu-BO': 'qu-bo', 'qu-EC': 'qu-ec', 'qu-PE': 'qu-pe', 'ru-BY':
    'ru-by', 'ru-KG': 'ru-kg', 'ru-UA': 'ru-ua', 'sr-BA': 'sr-ba', 'ss-SZ': 'ss-sz', 'sv-FI': 'sv-fi', 'ta-LK':
    'ta-lk', 'ta-SG': 'ta-sg', 'tn-BW': 'tn-bw', 'tr-CY': 'tr-cy', 'tzm-MA': 'tzm-ma', 'yo-NG': 'yo-ng',
    'zh-MO': 'zh-mo', 'zh-SG': 'zh-sg', 'zh-TW': 'zh-tw'
}
BING_FOLDED = {
    'ar-dj': 'fr-dj', 'ar-er': 'ti-er', 'ar-il': 'he-il', 'ar-sd': 'en-sd', 'ar-so': 'so-so', 'az-arab': None,
    'az-latn': None, 'bs-latn': None, 'de-be': 'nl-be', 'de-lu': 'fr-lu', 'en-bi': 'rn-bi', 'en-cm': 'fr-cm',
    'en-dsrt': None, 'en-er': 'ti-er', 'en-in': 'hi-in', 'en-ke': 'sw-ke', 'en-ls': 'st-ls', 'en-mg': 'mg-mg',
    'en-mt': 'mt-mt', 'en-mu': 'fr-mu', 'en-mw': 'ny-mw', 'en-pg': 'tpi-pg', 'en-pk': 'ur-pk', 'en-pr': 'es-pr',
    'en-rw': 'rw-rw', 'en-sc': 'fr-sc', 'en-shaw': None, 'en-to': 'to-to', 'en-tz': 'sw-tz', 'en-ug': 'sw-ug',
    'en-zw': 'sn-zw', 'es-py': 'gn-py', 'fil-ph': 'en-ph', 'fr-be': 'nl-be', 'fr-bi': 'rn-bi', 'fr-ca': 'en-ca',
    'fr-cf': 'sg-cf', 'fr-ch': 'de-ch', 'fr-dz': 'ar-dz', 'fr-gq': 'es-gq', 'fr-ht': 'ht-ht', 'fr-km': 'ar-km',
    'fr-ma': 'ar-ma', 'fr-mg': 'mg-mg', 'fr-rw': 'rw-rw', 'fr-sn': 'wo-sn', 'fr-td': 'ar-td', 'fr-tn': 'ar-tn',
    'fr-vu': 'en-vu', 'ga-ie': 'en-ie', 'gsw-ch': 'de-ch', 'gsw-li': 'de-li', 'hi-latn': None, 'hr-ba': 'bs-ba',
    'it-ch': 'de-ch', 'kk-arab': None, 'kk-cyrl': None, 'kk-cyrl-kz': 'ru-kz', 'kk-kz': 'ru-kz', 'lb-lu':
    'fr-lu', 'mi-nz': 'en-nz', 'mn-mong': None, 'ms-sg': 'en-sg', 'nd-zw': 'sn-zw', 'nl-cw': 'pap-cw', 'nl-sx':
    'en-sx', 'nn-no': 'nb-no', 'pap-aw': 'nl-aw', 'ps-af': 'fa-af', 'pt-gq': 'es-gq', 'pt-mo': 'yue-mo',
    'qu-bo': 'es-bo', 'qu-ec': 'es-ec', 'qu-pe': 'es-pe', 'ru-by': 'be-by', 'ru-kg': 'ky-kg', 'ru-ua': 'uk-ua',
    'sr-ba': 'bs-ba', 'sr-cyrl': None, 'sr-cyrl-ba': 'bs-ba', 'sr-cyrl-me': None, 'sr-cyrl-xk': None, 'sr-latn':
    'sr-me', 'sr-latn-ba': 'bs-ba', 'sr-latn-me': 'sr-me', 'sr-latn-xk': 'sr-me', 'sr-me': 'sr-me', 'sr-xk':
    None, 'ss-sz': 'en-sz', 'sv-fi': 'fi-fi', 'ta-lk': 'si-lk', 'ta-sg': 'en-sg', 'tn-bw': 'en-bw', 'tr-cy':
    'el-cy', 'tzm-ma': 'ar-ma', 'uz-arab': None, 'uz-latn': None, 'yo-ng': 'en-ng', 'yue-hans': None,
    'yue-hant': None, 'zh-hans': None, 'zh-hans-sg': 'en-sg', 'zh-hant': 'zh-mo', 'zh-hant-tw': 'zh-mo',
    'zh-latn': None, 'zh-sg': 'en-sg', 'zh-tw': 'zh-mo'
}


def bing_market(tag):
    """Bing market code for a request language, or None for ``all`` (Bing's "clear" market)."""
    if not tag or tag == 'all':
        return None
    market = _lookup(BING_EXCEPTIONS, BING_FOLDED, tag)
    return _bing_rule(tag) if market is _MISSING else market


def _bing_rule(tag):
    locale = babel.Locale.parse(tag, sep='-')
    if locale.territory:
        own = locale.language + '-' + locale.territory.lower()
        if own in BING_MARKETS and not locale.script:
            return own
        if locale.territory in BING_TERRITORY_MARKETS:
            return BING_TERRITORY_MARKETS[locale.territory]
    return BING_LANGUAGE_MARKETS.get(locale.language)


# Brave ``country`` and ``ui_lang`` cookies. Derived like the tables above from
# the previous runtime's requests; ``brave_cookies`` reproduces all of them.
BRAVE_COUNTRIES = frozenset((
    'ar', 'at', 'au', 'be', 'br', 'ca', 'ch', 'cl', 'cn', 'de', 'dk', 'es', 'fi', 'fr', 'gb', 'hk', 'id', 'in',
    'it', 'jp', 'kr', 'mx', 'my', 'nl', 'no', 'nz', 'ph', 'pl', 'pt', 'ru', 'sa', 'se', 'tr', 'tw', 'us', 'za'
))
BRAVE_UI_LANGUAGES = frozenset((
    'az', 'bg', 'br', 'ca', 'cs', 'cy', 'da', 'de-de', 'el', 'en-ca', 'en-gb', 'en-in', 'en-us', 'es', 'et',
    'eu', 'fi-fi', 'fr-ca', 'fr-fr', 'gl', 'hr', 'hu', 'id', 'it', 'ja-jp', 'ka', 'ko', 'lt', 'lv', 'ms', 'nb',
    'nl', 'pl', 'pt-br', 'ro', 'ru', 'sk', 'sl', 'sq-al', 'sr', 'sr-latn', 'sv', 'sw-ke', 'th', 'tr', 'uk',
    'vi', 'zh', 'zh-tw'
))
BRAVE_LANGUAGE_COUNTRIES = {
    'aa': 'all', 'ab': 'all', 'af': 'all', 'agq': 'all', 'ak': 'all', 'am': 'all', 'an': 'all', 'ann': 'all',
    'apc': 'all', 'ar': 'sa', 'arn': 'all', 'as': 'all', 'asa': 'all', 'ast': 'all', 'az': 'all', 'ba': 'all',
    'bal': 'all', 'bas': 'all', 'be': 'all', 'bem': 'all', 'bew': 'all', 'bez': 'all', 'bg': 'all', 'bgc':
    'all', 'bgn': 'all', 'bho': 'all', 'blo': 'all', 'blt': 'all', 'bm': 'all', 'bn': 'all', 'bo': 'all', 'br':
    'all', 'brx': 'all', 'bs': 'all', 'bss': 'all', 'byn': 'all', 'ca': 'all', 'cad': 'all', 'cch': 'all',
    'ccp': 'all', 'ce': 'all', 'ceb': 'all', 'cgg': 'all', 'cho': 'all', 'chr': 'all', 'cic': 'all', 'ckb':
    'all', 'co': 'all', 'cop': 'all', 'cs': 'all', 'csw': 'all', 'cu': 'all', 'cv': 'all', 'cy': 'all', 'da':
    'dk', 'dav': 'all', 'de': 'de', 'dje': 'all', 'doi': 'all', 'dsb': 'all', 'dua': 'all', 'dv': 'all', 'dyo':
    'all', 'dz': 'all', 'ebu': 'all', 'ee': 'all', 'el': 'all', 'en': 'us', 'eo': 'all', 'es': 'es', 'et':
    'all', 'eu': 'all', 'ewo': 'all', 'fa': 'all', 'ff': 'all', 'fi': 'fi', 'fil': 'ph', 'fo': 'all', 'fr':
    'fr', 'frr': 'all', 'fur': 'all', 'fy': 'all', 'ga': 'all', 'gaa': 'all', 'gd': 'all', 'gez': 'all', 'gl':
    'all', 'gn': 'all', 'gsw': 'ch', 'gu': 'all', 'guz': 'all', 'gv': 'all', 'ha': 'all', 'haw': 'all', 'he':
    'all', 'hi': 'in', 'hnj': 'all', 'hr': 'all', 'hsb': 'all', 'ht': 'all', 'hu': 'all', 'hy': 'all', 'ia':
    'all', 'id': 'id', 'ie': 'all', 'ig': 'all', 'ii': 'all', 'io': 'all', 'is': 'all', 'it': 'it', 'iu': 'all',
    'ja': 'jp', 'jbo': 'all', 'jgo': 'all', 'jmc': 'all', 'jv': 'all', 'ka': 'all', 'kaa': 'all', 'kab': 'all',
    'kaj': 'all', 'kam': 'all', 'kcg': 'all', 'kde': 'all', 'kea': 'all', 'ken': 'all', 'kgp': 'all', 'khq':
    'all', 'ki': 'all', 'kk': 'all', 'kkj': 'all', 'kl': 'all', 'kln': 'all', 'km': 'all', 'kn': 'all', 'ko':
    'kr', 'kok': 'all', 'kpe': 'all', 'ks': 'all', 'ksb': 'all', 'ksf': 'all', 'ksh': 'all', 'ku': 'all', 'kw':
    'all', 'kxv': 'all', 'ky': 'all', 'la': 'all', 'lag': 'all', 'lb': 'all', 'lg': 'all', 'lij': 'all', 'lkt':
    'all', 'lld': 'all', 'lmo': 'all', 'ln': 'all', 'lo': 'all', 'lrc': 'all', 'lt': 'all', 'ltg': 'all', 'lu':
    'all', 'luo': 'all', 'luy': 'all', 'lv': 'all', 'mai': 'all', 'mas': 'all', 'mdf': 'all', 'mer': 'all',
    'mfe': 'all', 'mg': 'all', 'mgh': 'all', 'mgo': 'all', 'mhn': 'all', 'mi': 'nz', 'mic': 'all', 'mk': 'all',
    'ml': 'all', 'mn': 'all', 'mni': 'all', 'moh': 'all', 'mr': 'all', 'ms': 'my', 'mt': 'all', 'mua': 'all',
    'mus': 'all', 'my': 'all', 'myv': 'all', 'mzn': 'all', 'naq': 'all', 'nb': 'no', 'nd': 'all', 'nds': 'all',
    'ne': 'all', 'nl': 'nl', 'nmg': 'all', 'nn': 'no', 'nnh': 'all', 'no': 'all', 'nqo': 'all', 'nr': 'all',
    'nso': 'all', 'nus': 'all', 'nv': 'all', 'ny': 'all', 'nyn': 'all', 'oc': 'all', 'om': 'all', 'or': 'all',
    'os': 'all', 'osa': 'all', 'pa': 'all', 'pap': 'all', 'pcm': 'all', 'pis': 'all', 'pl': 'pl', 'prg': 'all',
    'ps': 'all', 'pt': 'pt', 'qu': 'all', 'quc': 'all', 'raj': 'all', 'rhg': 'all', 'rif': 'all', 'rm': 'all',
    'rn': 'all', 'ro': 'all', 'rof': 'all', 'ru': 'ru', 'rw': 'all', 'rwk': 'all', 'sa': 'all', 'sah': 'all',
    'saq': 'all', 'sat': 'all', 'sbp': 'all', 'sc': 'all', 'scn': 'all', 'sd': 'all', 'sdh': 'all', 'se': 'all',
    'seh': 'all', 'ses': 'all', 'sg': 'all', 'shi': 'all', 'shn': 'all', 'si': 'all', 'sid': 'all', 'sk': 'all',
    'skr': 'all', 'sl': 'all', 'sma': 'all', 'smj': 'all', 'smn': 'all', 'sms': 'all', 'sn': 'all', 'so': 'all',
    'sq': 'all', 'sr': 'all', 'ss': 'all', 'ssy': 'all', 'st': 'all', 'su': 'all', 'sv': 'se', 'sw': 'all',
    'syr': 'all', 'szl': 'all', 'ta': 'all', 'te': 'all', 'teo': 'all', 'tg': 'all', 'th': 'all', 'ti': 'all',
    'tig': 'all', 'tk': 'all', 'tn': 'all', 'to': 'all', 'tok': 'all', 'tpi': 'all', 'tr': 'tr', 'trv': 'all',
    'trw': 'all', 'ts': 'all', 'tt': 'all', 'twq': 'all', 'tyv': 'all', 'tzm': 'all', 'ug': 'all', 'uk': 'all',
    'ur': 'all', 'uz': 'all', 'vai': 'all', 've': 'all', 'vec': 'all', 'vi': 'all', 'vmw': 'all', 'vo': 'all',
    'vun': 'all', 'wa': 'all', 'wae': 'all', 'wal': 'all', 'wbp': 'all', 'wo': 'all', 'xh': 'all', 'xnr': 'all',
    'xog': 'all', 'yav': 'all', 'yi': 'all', 'yo': 'all', 'yrl': 'all', 'yue': 'all', 'za': 'all', 'zgh': 'all',
    'zh': 'cn', 'zu': 'all'
}
BRAVE_LANGUAGE_UI = {
    'aa': 'en-us', 'ab': 'en-us', 'af': 'en-us', 'agq': 'en-us', 'ak': 'en-us', 'am': 'en-us', 'an': 'en-us',
    'ann': 'en-us', 'apc': 'en-us', 'ar': 'en-us', 'arn': 'en-us', 'as': 'en-us', 'asa': 'en-us', 'ast':
    'en-us', 'az': 'az', 'ba': 'en-us', 'bal': 'en-us', 'bas': 'en-us', 'be': 'en-us', 'bem': 'en-us', 'bew':
    'en-us', 'bez': 'en-us', 'bg': 'bg', 'bgc': 'en-us', 'bgn': 'en-us', 'bho': 'en-us', 'blo': 'en-us', 'blt':
    'en-us', 'bm': 'en-us', 'bn': 'en-us', 'bo': 'en-us', 'br': 'br', 'brx': 'en-us', 'bs': 'en-us', 'bss':
    'en-us', 'byn': 'en-us', 'ca': 'ca', 'cad': 'en-us', 'cch': 'en-us', 'ccp': 'en-us', 'ce': 'en-us', 'ceb':
    'en-us', 'cgg': 'en-us', 'cho': 'en-us', 'chr': 'en-us', 'cic': 'en-us', 'ckb': 'en-us', 'co': 'en-us',
    'cop': 'en-us', 'cs': 'cs', 'csw': 'en-us', 'cu': 'en-us', 'cv': 'en-us', 'cy': 'cy', 'da': 'da', 'dav':
    'en-us', 'de': 'de-de', 'dje': 'en-us', 'doi': 'en-us', 'dsb': 'en-us', 'dua': 'en-us', 'dv': 'en-us',
    'dyo': 'en-us', 'dz': 'en-us', 'ebu': 'en-us', 'ee': 'en-us', 'el': 'el', 'en': 'en-us', 'eo': 'en-us',
    'es': 'es', 'et': 'et', 'eu': 'eu', 'ewo': 'en-us', 'fa': 'en-us', 'ff': 'en-us', 'fi': 'fi-fi', 'fil':
    'en-us', 'fo': 'en-us', 'fr': 'fr-fr', 'frr': 'en-us', 'fur': 'en-us', 'fy': 'en-us', 'ga': 'en-us', 'gaa':
    'en-us', 'gd': 'en-us', 'gez': 'en-us', 'gl': 'gl', 'gn': 'en-us', 'gsw': 'en-us', 'gu': 'en-us', 'guz':
    'en-us', 'gv': 'en-us', 'ha': 'en-us', 'haw': 'en-us', 'he': 'en-us', 'hi': 'en-us', 'hnj': 'en-us', 'hr':
    'hr', 'hsb': 'en-us', 'ht': 'en-us', 'hu': 'hu', 'hy': 'en-us', 'ia': 'en-us', 'id': 'id', 'ie': 'en-us',
    'ig': 'en-us', 'ii': 'en-us', 'io': 'en-us', 'is': 'en-us', 'it': 'it', 'iu': 'en-us', 'ja': 'ja-jp', 'jbo':
    'en-us', 'jgo': 'en-us', 'jmc': 'en-us', 'jv': 'en-us', 'ka': 'ka', 'kaa': 'en-us', 'kab': 'en-us', 'kaj':
    'en-us', 'kam': 'en-us', 'kcg': 'en-us', 'kde': 'en-us', 'kea': 'en-us', 'ken': 'en-us', 'kgp': 'en-us',
    'khq': 'en-us', 'ki': 'en-us', 'kk': 'en-us', 'kkj': 'en-us', 'kl': 'en-us', 'kln': 'en-us', 'km': 'en-us',
    'kn': 'en-us', 'ko': 'ko', 'kok': 'en-us', 'kpe': 'en-us', 'ks': 'en-us', 'ksb': 'en-us', 'ksf': 'en-us',
    'ksh': 'en-us', 'ku': 'en-us', 'kw': 'en-us', 'kxv': 'en-us', 'ky': 'en-us', 'la': 'en-us', 'lag': 'en-us',
    'lb': 'en-us', 'lg': 'en-us', 'lij': 'en-us', 'lkt': 'en-us', 'lld': 'en-us', 'lmo': 'en-us', 'ln': 'en-us',
    'lo': 'en-us', 'lrc': 'en-us', 'lt': 'lt', 'ltg': 'en-us', 'lu': 'en-us', 'luo': 'en-us', 'luy': 'en-us',
    'lv': 'lv', 'mai': 'en-us', 'mas': 'en-us', 'mdf': 'en-us', 'mer': 'en-us', 'mfe': 'en-us', 'mg': 'en-us',
    'mgh': 'en-us', 'mgo': 'en-us', 'mhn': 'en-us', 'mi': 'en-us', 'mic': 'en-us', 'mk': 'en-us', 'ml': 'en-us',
    'mn': 'en-us', 'mni': 'en-us', 'moh': 'en-us', 'mr': 'en-us', 'ms': 'ms', 'mt': 'en-us', 'mua': 'en-us',
    'mus': 'en-us', 'my': 'en-us', 'myv': 'en-us', 'mzn': 'en-us', 'naq': 'en-us', 'nb': 'nb', 'nd': 'en-us',
    'nds': 'en-us', 'ne': 'en-us', 'nl': 'nl', 'nmg': 'en-us', 'nn': 'en-us', 'nnh': 'en-us', 'no': 'en-us',
    'nqo': 'en-us', 'nr': 'en-us', 'nso': 'en-us', 'nus': 'en-us', 'nv': 'en-us', 'ny': 'en-us', 'nyn': 'en-us',
    'oc': 'en-us', 'om': 'en-us', 'or': 'en-us', 'os': 'en-us', 'osa': 'en-us', 'pa': 'en-us', 'pap': 'en-us',
    'pcm': 'en-us', 'pis': 'en-us', 'pl': 'pl', 'prg': 'en-us', 'ps': 'en-us', 'pt': 'pt-br', 'qu': 'en-us',
    'quc': 'en-us', 'raj': 'en-us', 'rhg': 'en-us', 'rif': 'en-us', 'rm': 'en-us', 'rn': 'en-us', 'ro': 'ro',
    'rof': 'en-us', 'ru': 'ru', 'rw': 'en-us', 'rwk': 'en-us', 'sa': 'en-us', 'sah': 'en-us', 'saq': 'en-us',
    'sat': 'en-us', 'sbp': 'en-us', 'sc': 'en-us', 'scn': 'en-us', 'sd': 'en-us', 'sdh': 'en-us', 'se': 'en-us',
    'seh': 'en-us', 'ses': 'en-us', 'sg': 'en-us', 'shi': 'en-us', 'shn': 'en-us', 'si': 'en-us', 'sid':
    'en-us', 'sk': 'sk', 'skr': 'en-us', 'sl': 'sl', 'sma': 'en-us', 'smj': 'en-us', 'smn': 'en-us', 'sms':
    'en-us', 'sn': 'en-us', 'so': 'en-us', 'sq': 'sq-al', 'sr': 'sr', 'ss': 'en-us', 'ssy': 'en-us', 'st':
    'en-us', 'su': 'en-us', 'sv': 'sv', 'sw': 'sw-ke', 'syr': 'en-us', 'szl': 'en-us', 'ta': 'en-us', 'te':
    'en-us', 'teo': 'en-us', 'tg': 'en-us', 'th': 'th', 'ti': 'en-us', 'tig': 'en-us', 'tk': 'en-us', 'tn':
    'en-us', 'to': 'en-us', 'tok': 'en-us', 'tpi': 'en-us', 'tr': 'tr', 'trv': 'en-us', 'trw': 'en-us', 'ts':
    'en-us', 'tt': 'en-us', 'twq': 'en-us', 'tyv': 'en-us', 'tzm': 'en-us', 'ug': 'en-us', 'uk': 'uk', 'ur':
    'en-us', 'uz': 'en-us', 'vai': 'en-us', 've': 'en-us', 'vec': 'en-us', 'vi': 'vi', 'vmw': 'en-us', 'vo':
    'en-us', 'vun': 'en-us', 'wa': 'en-us', 'wae': 'en-us', 'wal': 'en-us', 'wbp': 'en-us', 'wo': 'en-us', 'xh':
    'en-us', 'xnr': 'en-us', 'xog': 'en-us', 'yav': 'en-us', 'yi': 'en-us', 'yo': 'en-us', 'yrl': 'en-us',
    'yue': 'en-us', 'za': 'en-us', 'zgh': 'en-us', 'zh': 'zh', 'zu': 'en-us'
}
BRAVE_TERRITORY_UI = {
    'BR': 'pt-br', 'CA': 'en-ca', 'DE': 'de-de', 'FI': 'fi-fi', 'FR': 'fr-fr', 'GB': 'en-gb', 'IN': 'en-in',
    'KE': 'sw-ke', 'TW': 'zh-tw', 'US': 'en-us'
}
BRAVE_EXCEPTIONS = {
    'fr-CA': ('ca', 'fr-ca'), 'zh-TW': ('tw', 'zh-tw')
}
BRAVE_FOLDED = {
    'az-az': ('all', 'en-us'), 'az-arab': ('all', 'en-us'), 'az-arab-iq': ('all', 'en-us'), 'az-arab-ir':
    ('all', 'en-us'), 'az-arab-tr': ('tr', 'en-us'), 'az-cyrl': ('all', 'en-us'), 'az-cyrl-az': ('all',
    'en-us'), 'az-iq': ('all', 'en-us'), 'az-ir': ('all', 'en-us'), 'az-latn': ('all', 'en-us'), 'az-latn-az':
    ('all', 'en-us'), 'br-fr': ('fr', 'br'), 'ca-fr': ('fr', 'ca'), 'cy-gb': ('gb', 'cy'), 'en-dsrt': ('all',
    'en-us'), 'en-shaw': ('all', 'en-us'), 'es-br': ('br', 'es'), 'es-us': ('us', 'es'), 'fr-ca': ('ca',
    'en-ca'), 'hi-latn': ('all', 'en-us'), 'ms-arab': ('all', 'en-us'), 'ms-arab-bn': ('all', 'en-us'),
    'ms-arab-my': ('my', 'en-us'), 'sr-ba': ('all', 'en-us'), 'sr-cyrl': ('all', 'en-us'), 'sr-cyrl-ba': ('all',
    'en-us'), 'sr-cyrl-me': ('all', 'en-us'), 'sr-cyrl-rs': ('all', 'en-us'), 'sr-cyrl-xk': ('all', 'en-us'),
    'sr-latn': ('all', 'sr-latn'), 'sr-latn-ba': ('all', 'sr-latn'), 'sr-latn-me': ('all', 'sr-latn'),
    'sr-latn-rs': ('all', 'sr-latn'), 'sr-latn-xk': ('all', 'sr-latn'), 'sr-me': ('all', 'sr-latn'), 'sr-rs':
    ('all', 'en-us'), 'sr-xk': ('all', 'en-us'), 'sv-fi': ('fi', 'sv'), 'trv-tw': ('all', 'en-us'), 'zh-cn':
    ('cn', 'en-us'), 'zh-hk': ('hk', 'zh-tw'), 'zh-hans': ('all', 'en-us'), 'zh-hans-cn': ('cn', 'en-us'),
    'zh-hans-hk': ('hk', 'en-us'), 'zh-hans-mo': ('all', 'en-us'), 'zh-hans-my': ('my', 'en-us'), 'zh-hans-sg':
    ('all', 'en-us'), 'zh-hant': ('hk', 'zh-tw'), 'zh-hant-hk': ('hk', 'zh-tw'), 'zh-hant-mo': ('hk', 'zh-tw'),
    'zh-hant-my': ('my', 'zh-tw'), 'zh-hant-tw': ('hk', 'zh-tw'), 'zh-latn': ('all', 'en-us'), 'zh-latn-cn':
    ('cn', 'en-us'), 'zh-mo': ('hk', 'zh-tw'), 'zh-my': ('my', 'en-us'), 'zh-sg': ('all', 'en-us'), 'zh-tw':
    ('hk', 'zh-tw')
}


def brave_cookies(tag):
    """Brave ``(country, ui_lang)`` cookie values for a request language."""
    if not tag or tag == 'all':
        return 'all', 'en-us'
    values = _lookup(BRAVE_EXCEPTIONS, BRAVE_FOLDED, tag)
    return _brave_rule(tag) if values is _MISSING else values


def _brave_rule(tag):
    locale = babel.Locale.parse(tag, sep='-')
    territory = locale.territory.lower() if locale.territory else None
    if territory in BRAVE_COUNTRIES:
        country = territory
    else:
        country = BRAVE_LANGUAGE_COUNTRIES.get(locale.language, 'all')
    own = locale.language + '-' + territory if territory else None
    if own in BRAVE_UI_LANGUAGES and len(tag.split('-')) == 2:
        ui = own
    elif locale.territory in BRAVE_TERRITORY_UI:
        ui = BRAVE_TERRITORY_UI[locale.territory]
    else:
        ui = BRAVE_LANGUAGE_UI.get(locale.language, 'en-us')
    return country, ui


# Qwant ``locale`` argument. Derived like the tables above from the previous
# runtime's requests; ``qwant_locale`` reproduces all of them.
QWANT_LOCALES = frozenset((
    'bg_BG', 'ca_ES', 'cs_CZ', 'da_DK', 'de_AT', 'de_CH', 'de_DE', 'el_GR', 'en_AU', 'en_CA', 'en_GB', 'en_IE',
    'en_MY', 'en_NZ', 'en_US', 'es_AR', 'es_CL', 'es_ES', 'es_MX', 'et_EE', 'fi_FI', 'fr_BE', 'fr_CA', 'fr_CH',
    'fr_FR', 'he_IL', 'hu_HU', 'it_CH', 'it_IT', 'ko_KR', 'nb_NO', 'nl_BE', 'nl_NL', 'pl_PL', 'pt_PT', 'ro_RO',
    'sv_SE', 'th_TH', 'zh_CN'
))
QWANT_LANGUAGE_LOCALES = {
    'aa': 'en_US', 'ab': 'en_US', 'af': 'en_US', 'agq': 'en_US', 'ak': 'en_US', 'am': 'en_US', 'an': 'en_US',
    'ann': 'en_US', 'apc': 'en_US', 'ar': 'en_US', 'arn': 'en_US', 'as': 'en_US', 'asa': 'en_US', 'ast':
    'en_US', 'az': 'en_US', 'ba': 'en_US', 'bal': 'en_US', 'bas': 'en_US', 'be': 'en_US', 'bem': 'en_US', 'bew':
    'en_US', 'bez': 'en_US', 'bg': 'bg_BG', 'bgc': 'en_US', 'bgn': 'en_US', 'bho': 'en_US', 'blo': 'en_US',
    'blt': 'en_US', 'bm': 'en_US', 'bn': 'en_US', 'bo': 'en_US', 'br': 'en_US', 'brx': 'en_US', 'bs': 'en_US',
    'bss': 'en_US', 'byn': 'en_US', 'ca': 'ca_ES', 'cad': 'en_US', 'cch': 'en_US', 'ccp': 'en_US', 'ce':
    'en_US', 'ceb': 'en_US', 'cgg': 'en_US', 'cho': 'en_US', 'chr': 'en_US', 'cic': 'en_US', 'ckb': 'en_US',
    'co': 'en_US', 'cop': 'en_US', 'cs': 'cs_CZ', 'csw': 'en_US', 'cu': 'en_US', 'cv': 'en_US', 'cy': 'en_US',
    'da': 'da_DK', 'dav': 'en_US', 'de': 'de_DE', 'dje': 'en_US', 'doi': 'en_US', 'dsb': 'en_US', 'dua':
    'en_US', 'dv': 'en_US', 'dyo': 'en_US', 'dz': 'en_US', 'ebu': 'en_US', 'ee': 'en_US', 'el': 'el_GR', 'en':
    'en_US', 'eo': 'en_US', 'es': 'es_ES', 'et': 'et_EE', 'eu': 'en_US', 'ewo': 'en_US', 'fa': 'en_US', 'ff':
    'en_US', 'fi': 'fi_FI', 'fil': 'en_US', 'fo': 'en_US', 'fr': 'fr_FR', 'frr': 'en_US', 'fur': 'en_US', 'fy':
    'en_US', 'ga': 'en_US', 'gaa': 'en_US', 'gd': 'en_US', 'gez': 'en_US', 'gl': 'en_US', 'gn': 'en_US', 'gsw':
    'en_US', 'gu': 'en_US', 'guz': 'en_US', 'gv': 'en_US', 'ha': 'en_US', 'haw': 'en_US', 'he': 'he_IL', 'hi':
    'en_US', 'hnj': 'en_US', 'hr': 'en_US', 'hsb': 'en_US', 'ht': 'en_US', 'hu': 'hu_HU', 'hy': 'en_US', 'ia':
    'en_US', 'id': 'en_US', 'ie': 'en_US', 'ig': 'en_US', 'ii': 'en_US', 'io': 'en_US', 'is': 'en_US', 'it':
    'it_IT', 'iu': 'en_US', 'ja': 'en_US', 'jbo': 'en_US', 'jgo': 'en_US', 'jmc': 'en_US', 'jv': 'en_US', 'ka':
    'en_US', 'kaa': 'en_US', 'kab': 'en_US', 'kaj': 'en_US', 'kam': 'en_US', 'kcg': 'en_US', 'kde': 'en_US',
    'kea': 'en_US', 'ken': 'en_US', 'kgp': 'en_US', 'khq': 'en_US', 'ki': 'en_US', 'kk': 'en_US', 'kkj':
    'en_US', 'kl': 'en_US', 'kln': 'en_US', 'km': 'en_US', 'kn': 'en_US', 'ko': 'ko_KR', 'kok': 'en_US', 'kpe':
    'en_US', 'ks': 'en_US', 'ksb': 'en_US', 'ksf': 'en_US', 'ksh': 'en_US', 'ku': 'en_US', 'kw': 'en_US', 'kxv':
    'en_US', 'ky': 'en_US', 'la': 'en_US', 'lag': 'en_US', 'lb': 'en_US', 'lg': 'en_US', 'lij': 'en_US', 'lkt':
    'en_US', 'lld': 'en_US', 'lmo': 'en_US', 'ln': 'en_US', 'lo': 'en_US', 'lrc': 'en_US', 'lt': 'en_US', 'ltg':
    'en_US', 'lu': 'en_US', 'luo': 'en_US', 'luy': 'en_US', 'lv': 'en_US', 'mai': 'en_US', 'mas': 'en_US',
    'mdf': 'en_US', 'mer': 'en_US', 'mfe': 'en_US', 'mg': 'en_US', 'mgh': 'en_US', 'mgo': 'en_US', 'mhn':
    'en_US', 'mi': 'en_US', 'mic': 'en_US', 'mk': 'en_US', 'ml': 'en_US', 'mn': 'en_US', 'mni': 'en_US', 'moh':
    'en_US', 'mr': 'en_US', 'ms': 'en_US', 'mt': 'en_US', 'mua': 'en_US', 'mus': 'en_US', 'my': 'en_US', 'myv':
    'en_US', 'mzn': 'en_US', 'naq': 'en_US', 'nb': 'nb_NO', 'nd': 'en_US', 'nds': 'en_US', 'ne': 'en_US', 'nl':
    'nl_NL', 'nmg': 'en_US', 'nn': 'en_US', 'nnh': 'en_US', 'no': 'en_US', 'nqo': 'en_US', 'nr': 'en_US', 'nso':
    'en_US', 'nus': 'en_US', 'nv': 'en_US', 'ny': 'en_US', 'nyn': 'en_US', 'oc': 'en_US', 'om': 'en_US', 'or':
    'en_US', 'os': 'en_US', 'osa': 'en_US', 'pa': 'en_US', 'pap': 'en_US', 'pcm': 'en_US', 'pis': 'en_US', 'pl':
    'pl_PL', 'prg': 'en_US', 'ps': 'en_US', 'pt': 'pt_PT', 'qu': 'en_US', 'quc': 'en_US', 'raj': 'en_US', 'rhg':
    'en_US', 'rif': 'en_US', 'rm': 'en_US', 'rn': 'en_US', 'ro': 'ro_RO', 'rof': 'en_US', 'ru': 'en_US', 'rw':
    'en_US', 'rwk': 'en_US', 'sa': 'en_US', 'sah': 'en_US', 'saq': 'en_US', 'sat': 'en_US', 'sbp': 'en_US',
    'sc': 'en_US', 'scn': 'en_US', 'sd': 'en_US', 'sdh': 'en_US', 'se': 'en_US', 'seh': 'en_US', 'ses': 'en_US',
    'sg': 'en_US', 'shi': 'en_US', 'shn': 'en_US', 'si': 'en_US', 'sid': 'en_US', 'sk': 'en_US', 'skr': 'en_US',
    'sl': 'en_US', 'sma': 'en_US', 'smj': 'en_US', 'smn': 'en_US', 'sms': 'en_US', 'sn': 'en_US', 'so': 'en_US',
    'sq': 'en_US', 'sr': 'en_US', 'ss': 'en_US', 'ssy': 'en_US', 'st': 'en_US', 'su': 'en_US', 'sv': 'sv_SE',
    'sw': 'en_US', 'syr': 'en_US', 'szl': 'en_US', 'ta': 'en_US', 'te': 'en_US', 'teo': 'en_US', 'tg': 'en_US',
    'th': 'th_TH', 'ti': 'en_US', 'tig': 'en_US', 'tk': 'en_US', 'tn': 'en_US', 'to': 'en_US', 'tok': 'en_US',
    'tpi': 'en_US', 'tr': 'en_US', 'trv': 'en_US', 'trw': 'en_US', 'ts': 'en_US', 'tt': 'en_US', 'twq': 'en_US',
    'tyv': 'en_US', 'tzm': 'en_US', 'ug': 'en_US', 'uk': 'en_US', 'ur': 'en_US', 'uz': 'en_US', 'vai': 'en_US',
    've': 'en_US', 'vec': 'en_US', 'vi': 'en_US', 'vmw': 'en_US', 'vo': 'en_US', 'vun': 'en_US', 'wa': 'en_US',
    'wae': 'en_US', 'wal': 'en_US', 'wbp': 'en_US', 'wo': 'en_US', 'xh': 'en_US', 'xnr': 'en_US', 'xog':
    'en_US', 'yav': 'en_US', 'yi': 'en_US', 'yo': 'en_US', 'yrl': 'en_US', 'yue': 'en_US', 'za': 'en_US', 'zgh':
    'en_US', 'zh': 'zh_CN', 'zu': 'en_US'
}
QWANT_TERRITORY_LOCALES = {
    'AT': 'de_AT', 'AU': 'en_AU', 'BE': 'nl_BE', 'CA': 'en_CA', 'CH': 'de_CH', 'CL': 'es_CL', 'CN': 'zh_CN',
    'CZ': 'cs_CZ', 'DE': 'de_DE', 'DK': 'da_DK', 'EE': 'et_EE', 'ES': 'es_ES', 'FI': 'fi_FI', 'FR': 'fr_FR',
    'GB': 'en_GB', 'HU': 'hu_HU', 'IE': 'en_IE', 'IL': 'he_IL', 'IT': 'it_IT', 'NL': 'nl_NL', 'NO': 'nb_NO',
    'NZ': 'en_NZ', 'PL': 'pl_PL', 'PT': 'pt_PT', 'RO': 'ro_RO', 'SE': 'sv_SE', 'TH': 'th_TH', 'US': 'en_US'
}
QWANT_EXCEPTIONS = {
    'ca-ES': 'ca_ES', 'en-MY': 'en_MY', 'fr-BE': 'fr_BE', 'fr-CA': 'fr_CA', 'fr-CH': 'fr_CH', 'it-CH': 'it_CH'
}
QWANT_FOLDED = {
    'ca-es': 'es_ES', 'ca-es-valencia': 'es_ES', 'en-my': 'en_US', 'fr-be': 'nl_BE', 'fr-ca': 'en_CA', 'fr-ch':
    'de_CH', 'it-ch': 'de_CH', 'zh-hk': 'en_US', 'zh-hans': 'en_US', 'zh-hans-hk': 'en_US', 'zh-hans-mo':
    'en_US', 'zh-hans-my': 'en_US', 'zh-hans-sg': 'en_US', 'zh-hant': 'en_US', 'zh-hant-hk': 'en_US',
    'zh-hant-mo': 'en_US', 'zh-hant-my': 'en_US', 'zh-hant-tw': 'en_US', 'zh-latn': 'en_US', 'zh-mo': 'en_US',
    'zh-my': 'en_US', 'zh-sg': 'en_US', 'zh-tw': 'en_US'
}


def qwant_locale(tag):
    """Qwant locale (``en_US`` style) for a request language; ``en_US`` when none fits."""
    if not tag or tag == 'all':
        return 'en_US'
    value = _lookup(QWANT_EXCEPTIONS, QWANT_FOLDED, tag)
    return _qwant_rule(tag) if value is _MISSING else value


def _qwant_rule(tag):
    locale = babel.Locale.parse(tag, sep='-')
    own = f'{locale.language}_{locale.territory}' if locale.territory else None
    if own in QWANT_LOCALES and not locale.script:
        return own
    if locale.territory in QWANT_TERRITORY_LOCALES:
        return QWANT_TERRITORY_LOCALES[locale.territory]
    return QWANT_LANGUAGE_LOCALES.get(locale.language, 'en_US')


# DuckDuckGo region (``l``/``ah``) and language (``ad``) values. Derived like the
# tables above from the previous runtime's requests; ``duckduckgo_locale``
# reproduces all of them.
DDG_REGIONS = frozenset((
    'ar-es', 'at-de', 'au-en', 'be-fr', 'be-nl', 'bg-bg', 'br-pt', 'ca-en', 'ca-fr', 'ch-de', 'ch-fr', 'cl-es',
    'cn-zh', 'co-es', 'cz-cs', 'de-de', 'dk-da', 'ee-et', 'es-ca', 'es-es', 'fi-fi', 'fr-fr', 'gr-el', 'hk-tzh',
    'hr-hr', 'hu-hu', 'id-en', 'ie-en', 'il-en', 'in-en', 'is-is', 'it-it', 'jp-jp', 'kr-kr', 'lt-lt', 'lv-lv',
    'mx-es', 'my-en', 'nl-nl', 'no-no', 'nz-en', 'pe-es', 'ph-en', 'pk-en', 'pl-pl', 'pt-pt', 'ro-ro', 'ru-ru',
    'se-sv', 'sg-en', 'sk-sk', 'sl-sl', 'th-en', 'tr-tr', 'tw-tzh', 'ua-uk', 'uk-en', 'us-en', 'us-es', 'vn-en',
    'wt-wt', 'xa-ar', 'za-en'
))
DDG_LANGUAGES = frozenset((
    'af_ZA', 'ar_DZ', 'ar_EG', 'ar_SA', 'ast_ES', 'az_AZ', 'be_BY', 'bg_BG', 'bn_BD', 'bn_IN', 'br_FR', 'bs_BA',
    'ca_ES', 'cs_CZ', 'cy_GB', 'da_DK', 'de_CH', 'de_DE', 'el_GR', 'en_AU', 'en_CA', 'en_GB', 'en_US', 'eo_XX',
    'es_AR', 'es_CL', 'es_CO', 'es_CR', 'es_EC', 'es_ES', 'es_MX', 'es_PE', 'es_UY', 'es_VE', 'et_EE', 'eu_ES',
    'fa_IR', 'fi_FI', 'fr_BE', 'fr_CA', 'fr_CH', 'fr_FR', 'ga_IE', 'gd_GB', 'gl_ES', 'he_IL', 'hi_IN', 'hr_HR',
    'hu_HU', 'hy_AM', 'id_ID', 'is_IS', 'it_IT', 'ja_JP', 'kab_DZ', 'kn_IN', 'ko_KR', 'ku', 'kw_GB', 'lt_LT',
    'lv_LV', 'ml_IN', 'mr_IN', 'ms_MY', 'nb_NO', 'nl_BE', 'nl_NL', 'nn_NO', 'pl_PL', 'pt_BR', 'pt_PT', 'ro_RO',
    'ru_RU', 'sc_IT', 'si_LK', 'sk_SK', 'sl_SI', 'sq_AL', 'sr_RS', 'sv_SE', 'ta_IN', 'te_IN', 'th_TH', 'tl_PH',
    'tr_TR', 'uk_UA', 'ur_PK', 'vi_VN', 'wt-wt', 'zh_CN', 'zh_TW'
))
DDG_LANGUAGE_REGIONS = {
    'aa': 'wt-wt', 'ab': 'wt-wt', 'af': 'wt-wt', 'agq': 'wt-wt', 'ak': 'wt-wt', 'am': 'wt-wt', 'an': 'wt-wt',
    'ann': 'wt-wt', 'apc': 'wt-wt', 'ar': 'xa-ar', 'arn': 'wt-wt', 'as': 'wt-wt', 'asa': 'wt-wt', 'ast':
    'wt-wt', 'az': 'wt-wt', 'ba': 'wt-wt', 'bal': 'wt-wt', 'bas': 'wt-wt', 'be': 'wt-wt', 'bem': 'wt-wt', 'bew':
    'wt-wt', 'bez': 'wt-wt', 'bg': 'bg-bg', 'bgc': 'wt-wt', 'bgn': 'wt-wt', 'bho': 'wt-wt', 'blo': 'wt-wt',
    'blt': 'wt-wt', 'bm': 'wt-wt', 'bn': 'wt-wt', 'bo': 'wt-wt', 'br': 'wt-wt', 'brx': 'wt-wt', 'bs': 'wt-wt',
    'bss': 'wt-wt', 'byn': 'wt-wt', 'ca': 'es-ca', 'cad': 'wt-wt', 'cch': 'wt-wt', 'ccp': 'wt-wt', 'ce':
    'wt-wt', 'ceb': 'wt-wt', 'cgg': 'wt-wt', 'cho': 'wt-wt', 'chr': 'wt-wt', 'cic': 'wt-wt', 'ckb': 'wt-wt',
    'co': 'wt-wt', 'cop': 'wt-wt', 'cs': 'cz-cs', 'csw': 'wt-wt', 'cu': 'wt-wt', 'cv': 'wt-wt', 'cy': 'wt-wt',
    'da': 'dk-da', 'dav': 'wt-wt', 'de': 'de-de', 'dje': 'wt-wt', 'doi': 'wt-wt', 'dsb': 'wt-wt', 'dua':
    'wt-wt', 'dv': 'wt-wt', 'dyo': 'wt-wt', 'dz': 'wt-wt', 'ebu': 'wt-wt', 'ee': 'wt-wt', 'el': 'gr-el', 'en':
    'us-en', 'eo': 'wt-wt', 'es': 'es-es', 'et': 'ee-et', 'eu': 'wt-wt', 'ewo': 'wt-wt', 'fa': 'wt-wt', 'ff':
    'wt-wt', 'fi': 'fi-fi', 'fil': 'wt-wt', 'fo': 'wt-wt', 'fr': 'fr-fr', 'frr': 'wt-wt', 'fur': 'wt-wt', 'fy':
    'wt-wt', 'ga': 'wt-wt', 'gaa': 'wt-wt', 'gd': 'wt-wt', 'gez': 'wt-wt', 'gl': 'wt-wt', 'gn': 'wt-wt', 'gsw':
    'wt-wt', 'gu': 'wt-wt', 'guz': 'wt-wt', 'gv': 'wt-wt', 'ha': 'wt-wt', 'haw': 'wt-wt', 'he': 'wt-wt', 'hi':
    'wt-wt', 'hnj': 'wt-wt', 'hr': 'hr-hr', 'hsb': 'wt-wt', 'ht': 'wt-wt', 'hu': 'hu-hu', 'hy': 'wt-wt', 'ia':
    'wt-wt', 'id': 'id-en', 'ie': 'wt-wt', 'ig': 'wt-wt', 'ii': 'wt-wt', 'io': 'wt-wt', 'is': 'is-is', 'it':
    'it-it', 'iu': 'wt-wt', 'ja': 'jp-jp', 'jbo': 'wt-wt', 'jgo': 'wt-wt', 'jmc': 'wt-wt', 'jv': 'wt-wt', 'ka':
    'wt-wt', 'kaa': 'wt-wt', 'kab': 'wt-wt', 'kaj': 'wt-wt', 'kam': 'wt-wt', 'kcg': 'wt-wt', 'kde': 'wt-wt',
    'kea': 'wt-wt', 'ken': 'wt-wt', 'kgp': 'wt-wt', 'khq': 'wt-wt', 'ki': 'wt-wt', 'kk': 'wt-wt', 'kkj':
    'wt-wt', 'kl': 'wt-wt', 'kln': 'wt-wt', 'km': 'wt-wt', 'kn': 'wt-wt', 'ko': 'kr-kr', 'kok': 'wt-wt', 'kpe':
    'wt-wt', 'ks': 'wt-wt', 'ksb': 'wt-wt', 'ksf': 'wt-wt', 'ksh': 'wt-wt', 'ku': 'wt-wt', 'kw': 'wt-wt', 'kxv':
    'wt-wt', 'ky': 'wt-wt', 'la': 'wt-wt', 'lag': 'wt-wt', 'lb': 'wt-wt', 'lg': 'wt-wt', 'lij': 'wt-wt', 'lkt':
    'wt-wt', 'lld': 'wt-wt', 'lmo': 'wt-wt', 'ln': 'wt-wt', 'lo': 'wt-wt', 'lrc': 'wt-wt', 'lt': 'lt-lt', 'ltg':
    'wt-wt', 'lu': 'wt-wt', 'luo': 'wt-wt', 'luy': 'wt-wt', 'lv': 'lv-lv', 'mai': 'wt-wt', 'mas': 'wt-wt',
    'mdf': 'wt-wt', 'mer': 'wt-wt', 'mfe': 'wt-wt', 'mg': 'wt-wt', 'mgh': 'wt-wt', 'mgo': 'wt-wt', 'mhn':
    'wt-wt', 'mi': 'wt-wt', 'mic': 'wt-wt', 'mk': 'wt-wt', 'ml': 'wt-wt', 'mn': 'wt-wt', 'mni': 'wt-wt', 'moh':
    'wt-wt', 'mr': 'wt-wt', 'ms': 'wt-wt', 'mt': 'wt-wt', 'mua': 'wt-wt', 'mus': 'wt-wt', 'my': 'wt-wt', 'myv':
    'wt-wt', 'mzn': 'wt-wt', 'naq': 'wt-wt', 'nb': 'no-no', 'nd': 'wt-wt', 'nds': 'wt-wt', 'ne': 'wt-wt', 'nl':
    'nl-nl', 'nmg': 'wt-wt', 'nn': 'wt-wt', 'nnh': 'wt-wt', 'no': 'wt-wt', 'nqo': 'wt-wt', 'nr': 'wt-wt', 'nso':
    'wt-wt', 'nus': 'wt-wt', 'nv': 'wt-wt', 'ny': 'wt-wt', 'nyn': 'wt-wt', 'oc': 'wt-wt', 'om': 'wt-wt', 'or':
    'wt-wt', 'os': 'wt-wt', 'osa': 'wt-wt', 'pa': 'wt-wt', 'pap': 'wt-wt', 'pcm': 'wt-wt', 'pis': 'wt-wt', 'pl':
    'pl-pl', 'prg': 'wt-wt', 'ps': 'wt-wt', 'pt': 'pt-pt', 'qu': 'wt-wt', 'quc': 'wt-wt', 'raj': 'wt-wt', 'rhg':
    'wt-wt', 'rif': 'wt-wt', 'rm': 'wt-wt', 'rn': 'wt-wt', 'ro': 'ro-ro', 'rof': 'wt-wt', 'ru': 'ru-ru', 'rw':
    'wt-wt', 'rwk': 'wt-wt', 'sa': 'wt-wt', 'sah': 'wt-wt', 'saq': 'wt-wt', 'sat': 'wt-wt', 'sbp': 'wt-wt',
    'sc': 'wt-wt', 'scn': 'wt-wt', 'sd': 'wt-wt', 'sdh': 'wt-wt', 'se': 'wt-wt', 'seh': 'wt-wt', 'ses': 'wt-wt',
    'sg': 'wt-wt', 'shi': 'wt-wt', 'shn': 'wt-wt', 'si': 'wt-wt', 'sid': 'wt-wt', 'sk': 'sk-sk', 'skr': 'wt-wt',
    'sl': 'sl-sl', 'sma': 'wt-wt', 'smj': 'wt-wt', 'smn': 'wt-wt', 'sms': 'wt-wt', 'sn': 'wt-wt', 'so': 'wt-wt',
    'sq': 'wt-wt', 'sr': 'wt-wt', 'ss': 'wt-wt', 'ssy': 'wt-wt', 'st': 'wt-wt', 'su': 'wt-wt', 'sv': 'se-sv',
    'sw': 'wt-wt', 'syr': 'wt-wt', 'szl': 'wt-wt', 'ta': 'wt-wt', 'te': 'wt-wt', 'teo': 'wt-wt', 'tg': 'wt-wt',
    'th': 'th-en', 'ti': 'wt-wt', 'tig': 'wt-wt', 'tk': 'wt-wt', 'tn': 'wt-wt', 'to': 'wt-wt', 'tok': 'wt-wt',
    'tpi': 'wt-wt', 'tr': 'tr-tr', 'trv': 'wt-wt', 'trw': 'wt-wt', 'ts': 'wt-wt', 'tt': 'wt-wt', 'twq': 'wt-wt',
    'tyv': 'wt-wt', 'tzm': 'wt-wt', 'ug': 'wt-wt', 'uk': 'ua-uk', 'ur': 'wt-wt', 'uz': 'wt-wt', 'vai': 'wt-wt',
    've': 'wt-wt', 'vec': 'wt-wt', 'vi': 'vn-en', 'vmw': 'wt-wt', 'vo': 'wt-wt', 'vun': 'wt-wt', 'wa': 'wt-wt',
    'wae': 'wt-wt', 'wal': 'wt-wt', 'wbp': 'wt-wt', 'wo': 'wt-wt', 'xh': 'wt-wt', 'xnr': 'wt-wt', 'xog':
    'wt-wt', 'yav': 'wt-wt', 'yi': 'wt-wt', 'yo': 'wt-wt', 'yrl': 'wt-wt', 'yue': 'wt-wt', 'za': 'wt-wt', 'zgh':
    'wt-wt', 'zh': 'cn-zh', 'zu': 'wt-wt'
}
DDG_LANGUAGE_CODES = {
    'aa': 'en_US', 'ab': 'en_US', 'af': 'af_ZA', 'agq': 'en_US', 'ak': 'en_US', 'am': 'en_US', 'an': 'en_US',
    'ann': 'en_US', 'apc': 'en_US', 'ar': 'ar_EG', 'arn': 'en_US', 'as': 'en_US', 'asa': 'en_US', 'ast':
    'ast_ES', 'az': 'en_US', 'ba': 'en_US', 'bal': 'en_US', 'bas': 'en_US', 'be': 'be_BY', 'bem': 'en_US',
    'bew': 'en_US', 'bez': 'en_US', 'bg': 'bg_BG', 'bgc': 'en_US', 'bgn': 'en_US', 'bho': 'en_US', 'blo':
    'en_US', 'blt': 'en_US', 'bm': 'en_US', 'bn': 'bn_BD', 'bo': 'en_US', 'br': 'br_FR', 'brx': 'en_US', 'bs':
    'en_US', 'bss': 'en_US', 'byn': 'en_US', 'ca': 'ca_ES', 'cad': 'en_US', 'cch': 'en_US', 'ccp': 'en_US',
    'ce': 'en_US', 'ceb': 'en_US', 'cgg': 'en_US', 'cho': 'en_US', 'chr': 'en_US', 'cic': 'en_US', 'ckb':
    'en_US', 'co': 'en_US', 'cop': 'en_US', 'cs': 'cs_CZ', 'csw': 'en_US', 'cu': 'en_US', 'cv': 'en_US', 'cy':
    'cy_GB', 'da': 'da_DK', 'dav': 'en_US', 'de': 'de_DE', 'dje': 'en_US', 'doi': 'en_US', 'dsb': 'en_US',
    'dua': 'en_US', 'dv': 'en_US', 'dyo': 'en_US', 'dz': 'en_US', 'ebu': 'en_US', 'ee': 'en_US', 'el': 'el_GR',
    'en': 'en_US', 'eo': 'eo_XX', 'es': 'es_ES', 'et': 'et_EE', 'eu': 'eu_ES', 'ewo': 'en_US', 'fa': 'fa_IR',
    'ff': 'en_US', 'fi': 'fi_FI', 'fil': 'tl_PH', 'fo': 'en_US', 'fr': 'fr_FR', 'frr': 'en_US', 'fur': 'en_US',
    'fy': 'en_US', 'ga': 'ga_IE', 'gaa': 'en_US', 'gd': 'gd_GB', 'gez': 'en_US', 'gl': 'gl_ES', 'gn': 'en_US',
    'gsw': 'en_US', 'gu': 'en_US', 'guz': 'en_US', 'gv': 'en_US', 'ha': 'en_US', 'haw': 'en_US', 'he': 'he_IL',
    'hi': 'hi_IN', 'hnj': 'en_US', 'hr': 'hr_HR', 'hsb': 'en_US', 'ht': 'en_US', 'hu': 'hu_HU', 'hy': 'hy_AM',
    'ia': 'en_US', 'id': 'id_ID', 'ie': 'en_US', 'ig': 'en_US', 'ii': 'en_US', 'io': 'en_US', 'is': 'is_IS',
    'it': 'it_IT', 'iu': 'en_US', 'ja': 'ja_JP', 'jbo': 'en_US', 'jgo': 'en_US', 'jmc': 'en_US', 'jv': 'en_US',
    'ka': 'en_US', 'kaa': 'en_US', 'kab': 'kab_DZ', 'kaj': 'en_US', 'kam': 'en_US', 'kcg': 'en_US', 'kde':
    'en_US', 'kea': 'en_US', 'ken': 'en_US', 'kgp': 'en_US', 'khq': 'en_US', 'ki': 'en_US', 'kk': 'en_US',
    'kkj': 'en_US', 'kl': 'en_US', 'kln': 'en_US', 'km': 'en_US', 'kn': 'kn_IN', 'ko': 'ko_KR', 'kok': 'en_US',
    'kpe': 'en_US', 'ks': 'en_US', 'ksb': 'en_US', 'ksf': 'en_US', 'ksh': 'en_US', 'ku': 'ku', 'kw': 'kw_GB',
    'kxv': 'en_US', 'ky': 'en_US', 'la': 'en_US', 'lag': 'en_US', 'lb': 'en_US', 'lg': 'en_US', 'lij': 'en_US',
    'lkt': 'en_US', 'lld': 'en_US', 'lmo': 'en_US', 'ln': 'en_US', 'lo': 'en_US', 'lrc': 'en_US', 'lt': 'lt_LT',
    'ltg': 'en_US', 'lu': 'en_US', 'luo': 'en_US', 'luy': 'en_US', 'lv': 'lv_LV', 'mai': 'en_US', 'mas':
    'en_US', 'mdf': 'en_US', 'mer': 'en_US', 'mfe': 'en_US', 'mg': 'en_US', 'mgh': 'en_US', 'mgo': 'en_US',
    'mhn': 'en_US', 'mi': 'en_US', 'mic': 'en_US', 'mk': 'en_US', 'ml': 'ml_IN', 'mn': 'en_US', 'mni': 'en_US',
    'moh': 'en_US', 'mr': 'mr_IN', 'ms': 'ms_MY', 'mt': 'en_US', 'mua': 'en_US', 'mus': 'en_US', 'my': 'en_US',
    'myv': 'en_US', 'mzn': 'en_US', 'naq': 'en_US', 'nb': 'nb_NO', 'nd': 'en_US', 'nds': 'en_US', 'ne': 'en_US',
    'nl': 'nl_NL', 'nmg': 'en_US', 'nn': 'nn_NO', 'nnh': 'en_US', 'no': 'en_US', 'nqo': 'en_US', 'nr': 'en_US',
    'nso': 'en_US', 'nus': 'en_US', 'nv': 'en_US', 'ny': 'en_US', 'nyn': 'en_US', 'oc': 'en_US', 'om': 'en_US',
    'or': 'en_US', 'os': 'en_US', 'osa': 'en_US', 'pa': 'en_US', 'pap': 'en_US', 'pcm': 'en_US', 'pis': 'en_US',
    'pl': 'pl_PL', 'prg': 'en_US', 'ps': 'en_US', 'pt': 'pt_PT', 'qu': 'en_US', 'quc': 'en_US', 'raj': 'en_US',
    'rhg': 'en_US', 'rif': 'en_US', 'rm': 'en_US', 'rn': 'en_US', 'ro': 'ro_RO', 'rof': 'en_US', 'ru': 'ru_RU',
    'rw': 'en_US', 'rwk': 'en_US', 'sa': 'en_US', 'sah': 'en_US', 'saq': 'en_US', 'sat': 'en_US', 'sbp':
    'en_US', 'sc': 'sc_IT', 'scn': 'en_US', 'sd': 'en_US', 'sdh': 'en_US', 'se': 'en_US', 'seh': 'en_US', 'ses':
    'en_US', 'sg': 'en_US', 'shi': 'en_US', 'shn': 'en_US', 'si': 'si_LK', 'sid': 'en_US', 'sk': 'sk_SK', 'skr':
    'en_US', 'sl': 'sl_SI', 'sma': 'en_US', 'smj': 'en_US', 'smn': 'en_US', 'sms': 'en_US', 'sn': 'en_US', 'so':
    'en_US', 'sq': 'sq_AL', 'sr': 'en_US', 'ss': 'en_US', 'ssy': 'en_US', 'st': 'en_US', 'su': 'en_US', 'sv':
    'sv_SE', 'sw': 'en_US', 'syr': 'en_US', 'szl': 'en_US', 'ta': 'ta_IN', 'te': 'te_IN', 'teo': 'en_US', 'tg':
    'en_US', 'th': 'th_TH', 'ti': 'en_US', 'tig': 'en_US', 'tk': 'en_US', 'tn': 'en_US', 'to': 'en_US', 'tok':
    'en_US', 'tpi': 'en_US', 'tr': 'tr_TR', 'trv': 'en_US', 'trw': 'en_US', 'ts': 'en_US', 'tt': 'en_US', 'twq':
    'en_US', 'tyv': 'en_US', 'tzm': 'en_US', 'ug': 'en_US', 'uk': 'uk_UA', 'ur': 'ur_PK', 'uz': 'en_US', 'vai':
    'en_US', 've': 'en_US', 'vec': 'en_US', 'vi': 'vi_VN', 'vmw': 'en_US', 'vo': 'en_US', 'vun': 'en_US', 'wa':
    'en_US', 'wae': 'en_US', 'wal': 'en_US', 'wbp': 'en_US', 'wo': 'en_US', 'xh': 'en_US', 'xnr': 'en_US',
    'xog': 'en_US', 'yav': 'en_US', 'yi': 'en_US', 'yo': 'en_US', 'yrl': 'en_US', 'yue': 'en_US', 'za': 'en_US',
    'zgh': 'en_US', 'zh': 'en_US', 'zu': 'en_US'
}
DDG_TERRITORY_REGIONS = {
    'AT': 'at-de', 'AU': 'au-en', 'BE': 'be-nl', 'BR': 'br-pt', 'CA': 'ca-en', 'CH': 'ch-de', 'CL': 'cl-es',
    'CN': 'cn-zh', 'CO': 'co-es', 'CZ': 'cz-cs', 'DE': 'de-de', 'DK': 'dk-da', 'EE': 'ee-et', 'ES': 'es-es',
    'FI': 'fi-fi', 'FR': 'fr-fr', 'HK': 'hk-tzh', 'HU': 'hu-hu', 'ID': 'id-en', 'IE': 'ie-en', 'IN': 'in-en',
    'IT': 'it-it', 'JP': 'jp-jp', 'KR': 'kr-kr', 'LV': 'lv-lv', 'NL': 'nl-nl', 'NO': 'no-no', 'NZ': 'nz-en',
    'PE': 'pe-es', 'PH': 'ph-en', 'PK': 'pk-en', 'PL': 'pl-pl', 'PT': 'pt-pt', 'RO': 'ro-ro', 'RU': 'ru-ru',
    'SE': 'se-sv', 'SG': 'sg-en', 'SK': 'sk-sk', 'TH': 'th-en', 'TR': 'tr-tr', 'TW': 'tw-tzh', 'UA': 'ua-uk',
    'US': 'us-en', 'VN': 'vn-en', 'ZA': 'za-en'
}
DDG_EXCEPTIONS = {
    'ar-DZ': ('xa-ar', 'ar_DZ'), 'ar-SA': ('xa-ar', 'ar_SA'), 'bn-IN': ('in-en', 'bn_IN'), 'ca-ES': ('es-ca',
    'ca_ES'), 'de-CH': ('ch-de', 'de_CH'), 'en-AU': ('au-en', 'en_AU'), 'en-CA': ('ca-en', 'en_CA'), 'en-GB':
    ('uk-en', 'en_GB'), 'en-IL': ('il-en', 'en_US'), 'en-MY': ('my-en', 'en_US'), 'es-AR': ('ar-es', 'es_AR'),
    'es-CL': ('cl-es', 'es_CL'), 'es-CO': ('co-es', 'es_CO'), 'es-CR': ('es-es', 'es_CR'), 'es-EC': ('es-es',
    'es_EC'), 'es-MX': ('mx-es', 'es_MX'), 'es-PE': ('pe-es', 'es_PE'), 'es-US': ('us-es', 'es_ES'), 'es-UY':
    ('es-es', 'es_UY'), 'es-VE': ('es-es', 'es_VE'), 'fr-BE': ('be-fr', 'fr_BE'), 'fr-CA': ('ca-fr', 'fr_CA'),
    'fr-CH': ('ch-fr', 'fr_CH'), 'nl-BE': ('be-nl', 'nl_BE'), 'pt-BR': ('br-pt', 'pt_BR'), 'zh-TW': ('tw-tzh',
    'zh_TW')
}
DDG_FOLDED = {
    'ar-dz': ('xa-ar', 'ar_EG'), 'ar-sa': ('xa-ar', 'ar_EG'), 'az-latn': ('wt-wt', 'az_AZ'), 'az-latn-az':
    ('wt-wt', 'az_AZ'), 'bn-in': ('in-en', 'bn_BD'), 'bs-latn': ('wt-wt', 'bs_BA'), 'bs-latn-ba': ('wt-wt',
    'bs_BA'), 'ca-es': ('es-es', 'ca_ES'), 'cy-gb': ('uk-en', 'cy_GB'), 'de-ch': ('ch-de', 'de_DE'), 'en-au':
    ('au-en', 'en_US'), 'en-ca': ('ca-en', 'en_US'), 'en-dsrt': ('wt-wt', 'en_US'), 'en-gb': ('uk-en', 'en_US'),
    'en-hk': ('us-en', 'en_US'), 'en-il': ('us-en', 'en_US'), 'en-my': ('us-en', 'en_US'), 'en-si': ('sl-sl',
    'en_US'), 'en-shaw': ('wt-wt', 'en_US'), 'en-shaw-gb': ('uk-en', 'en_US'), 'es-ar': ('ar-es', 'es_ES'),
    'es-cl': ('cl-es', 'es_ES'), 'es-co': ('co-es', 'es_ES'), 'es-cr': ('es-es', 'es_ES'), 'es-ec': ('es-es',
    'es_ES'), 'es-mx': ('mx-es', 'es_ES'), 'es-pe': ('pe-es', 'es_ES'), 'es-us': ('us-en', 'es_ES'), 'es-uy':
    ('es-es', 'es_ES'), 'es-ve': ('es-es', 'es_ES'), 'fr-be': ('be-nl', 'fr_FR'), 'fr-ca': ('ca-en', 'fr_FR'),
    'fr-ch': ('ch-de', 'fr_FR'), 'ga-gb': ('uk-en', 'ga_IE'), 'gd-gb': ('uk-en', 'gd_GB'), 'hi-latn': ('wt-wt',
    'en_US'), 'hi-latn-in': ('in-en', 'en_US'), 'kw-gb': ('uk-en', 'kw_GB'), 'ms-arab': ('wt-wt', 'en_US'),
    'ms-arab-bn': ('wt-wt', 'en_US'), 'ms-arab-my': ('wt-wt', 'en_US'), 'nl-be': ('be-nl', 'nl_NL'), 'pt-br':
    ('br-pt', 'pt_PT'), 'sr-ba': ('wt-wt', 'sr_RS'), 'sr-cyrl': ('wt-wt', 'sr_RS'), 'sr-cyrl-ba': ('wt-wt',
    'sr_RS'), 'sr-cyrl-me': ('wt-wt', 'sr_RS'), 'sr-cyrl-rs': ('wt-wt', 'sr_RS'), 'sr-cyrl-xk': ('wt-wt',
    'sr_RS'), 'sr-xk': ('wt-wt', 'sr_RS'), 'trv-tw': ('wt-wt', 'en_US'), 'yue-hk': ('wt-wt', 'en_US'),
    'yue-hant-hk': ('wt-wt', 'en_US'), 'zh-hk': ('hk-tzh', 'zh_TW'), 'zh-hans': ('wt-wt', 'zh_CN'),
    'zh-hans-cn': ('cn-zh', 'zh_CN'), 'zh-hans-hk': ('wt-wt', 'zh_CN'), 'zh-hans-mo': ('wt-wt', 'zh_CN'),
    'zh-hans-my': ('wt-wt', 'zh_CN'), 'zh-hans-sg': ('sg-en', 'zh_CN'), 'zh-hant': ('hk-tzh', 'zh_TW'),
    'zh-hant-hk': ('hk-tzh', 'zh_TW'), 'zh-hant-mo': ('hk-tzh', 'zh_TW'), 'zh-hant-my': ('hk-tzh', 'zh_TW'),
    'zh-hant-tw': ('hk-tzh', 'zh_TW'), 'zh-latn': ('wt-wt', 'en_US'), 'zh-mo': ('hk-tzh', 'zh_TW'), 'zh-my':
    ('wt-wt', 'zh_CN'), 'zh-sg': ('sg-en', 'zh_CN'), 'zh-tw': ('hk-tzh', 'zh_TW')
}


def duckduckgo_locale(tag):
    """DuckDuckGo ``(region, language)`` for a request language, e.g. ``('cn-zh', 'zh_CN')``."""
    if not tag or tag == 'all':
        return 'wt-wt', 'wt-wt'
    values = _lookup(DDG_EXCEPTIONS, DDG_FOLDED, tag)
    return _duckduckgo_rule(tag) if values is _MISSING else values


def _duckduckgo_rule(tag):
    locale = babel.Locale.parse(tag, sep='-')
    two_part = len(tag.split('-')) == 2
    territory = locale.territory
    own = f'{territory.lower()}-{locale.language}' if territory else None
    if own in DDG_REGIONS and two_part:
        region = own
    elif territory in DDG_TERRITORY_REGIONS:
        region = DDG_TERRITORY_REGIONS[territory]
    else:
        region = DDG_LANGUAGE_REGIONS.get(locale.language, 'wt-wt')
    code = f'{locale.language}_{territory}' if territory else None
    language = code if code in DDG_LANGUAGES and two_part else DDG_LANGUAGE_CODES.get(locale.language, 'en_US')
    return region, language
