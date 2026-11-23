"""Language tags of search requests mapped to source-specific codes."""
import babel

# Wikipedia editions whose subdomain is not the ISO language code.
WIKIPEDIA_SUBDOMAINS = {'nb': 'no', 'gsw': 'als', 'yue': 'zh-yue', 'nan': 'zh-min-nan', 'lzh': 'zh-classical',
                        'sgs': 'bat-smg', 'vro': 'fiu-vro', 'rup': 'roa-rup', 'nrf': 'nrm', 'cbk': 'cbk-zam',
                        'ak': 'ak', 'nds-NL': 'nds-nl', 'be-tarask': 'be-tarask'}


def parse(tag):
    """babel.Locale for a request language tag, or None for ``all``/unknown tags."""
    if not tag or tag == 'all':
        return None
    try:
        return babel.Locale.parse(tag, sep='-')
    except (ValueError, babel.UnknownLocaleError):
        return None


def wikipedia_host(tag, default='en'):
    """Host of the Wikipedia edition for a request language, e.g. ``zh-CN`` -> ``zh.wikipedia.org``."""
    if tag and tag.lower() in ('be-tarask', 'nds-nl'):
        return tag.lower() + '.wikipedia.org'
    locale = parse(tag)
    if locale is None:
        code = default
    else:
        code = WIKIPEDIA_SUBDOMAINS.get(locale.language, locale.language)
    return code + '.wikipedia.org'


# Google interface languages and search regions. Derived from the request
# parameters the previous runtime produced for 1134 language tags (every babel
# locale plus the common language-region pairs); ``google_parameters``
# reproduces all of them. Exceptions are keyed by the tag as written.
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
    'be-TARASK': ('be', 'lang_be', 'countryBY'), 'bs-Cyrl': ('en', 'lang_en', 'countryBA'), 'el-POLYTON': ('el',
    'lang_el', 'countryGR'), 'ko-KP': ('ko', 'lang_ko', 'countryKR'), 'mn-CN': ('en', 'lang_en', 'countryHK'),
    'mn-Mong-CN': ('en', 'lang_en', 'countryHK'), 'ms-BN': ('ms', 'lang_ms', 'countrySG'), 'ms-MY': ('ms',
    'lang_ms', 'countrySG'), 'sd-Deva': ('en', 'lang_en', 'countryIN'), 'sr-Latn': ('sr-ME', 'lang_sr-ME',
    'countryRS'), 'sr-Latn-ME': ('sr-ME', 'lang_sr-ME', 'countryRS'), 'sr-Latn-XK': ('sr-ME', 'lang_sr-ME',
    'countryRS'), 'sr-ME': ('sr-ME', 'lang_sr-ME', 'countryRS'), 'trv-TW': ('en', 'lang_en', 'countryZZ'),
    'yue-Hant-MO': ('en', 'lang_en', 'countryZZ'), 'yue-MO': ('en', 'lang_en', 'countryZZ'), 'zh-Hans-MO':
    ('zh-CN', 'lang_zh-CN', 'countryZZ'), 'zh-Hans-MY': ('zh-CN', 'lang_zh-CN', 'countryZZ'), 'zh-Hant':
    ('zh-TW', 'lang_zh-TW', 'countryHK'), 'zh-Hant-TW': ('zh-TW', 'lang_zh-TW', 'countryHK'), 'zh-MY': ('zh-CN',
    'lang_zh-CN', 'countryZZ')
}


def google_parameters(tag):
    """Google ``hl``, ``lr``, ``cr``, ``ie`` and ``oe`` query arguments for a request language."""
    if not tag or tag == 'all':
        return {'hl': 'ZZ', 'lr': '', 'cr': '', 'ie': 'utf8', 'oe': 'utf8'}
    if tag in GOOGLE_EXCEPTIONS:
        hl, lr, cr = GOOGLE_EXCEPTIONS[tag]
        return {'hl': hl, 'lr': lr, 'cr': cr, 'ie': 'utf8', 'oe': 'utf8'}
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
    return {'hl': hl, 'lr': 'lang_' + hl, 'cr': cr, 'ie': 'utf8', 'oe': 'utf8'}


# Bing markets (``mkt``). Derived like the Google tables from the previous
# runtime's requests for 1135 language tags; ``bing_market`` reproduces all of
# them. A tag naming a territory uses that territory's market when Bing has no
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
    'az-Arab': None, 'az-Latn': None, 'bs-Latn': None, 'en-Dsrt': None, 'en-Shaw': None, 'hi-Latn': None,
    'kk-Arab': None, 'kk-Cyrl': None, 'kk-Cyrl-KZ': 'ru-kz', 'mn-Mong': None, 'sr-BA': 'sr-ba', 'sr-Cyrl': None,
    'sr-Cyrl-BA': 'bs-ba', 'sr-Cyrl-ME': None, 'sr-Cyrl-XK': None, 'sr-Latn': 'sr-me', 'sr-Latn-BA': 'bs-ba',
    'sr-Latn-ME': 'sr-me', 'sr-Latn-XK': 'sr-me', 'sr-ME': 'sr-me', 'sr-XK': None, 'uz-Arab': None, 'uz-Latn':
    None, 'yue-Hans': None, 'yue-Hant': None, 'zh-Hans': None, 'zh-Hans-SG': 'en-sg', 'zh-Hant': 'zh-mo',
    'zh-Hant-TW': 'zh-mo', 'zh-Latn': None, 'zh-MO': 'zh-mo', 'zh-SG': 'zh-sg', 'zh-TW': 'zh-tw'
}


def bing_market(tag):
    """Bing market code for a request language, or None for ``all`` (Bing's "clear" market)."""
    if not tag or tag == 'all':
        return None
    if tag in BING_EXCEPTIONS:
        return BING_EXCEPTIONS[tag]
    locale = babel.Locale.parse(tag, sep='-')
    if locale.territory:
        own = locale.language + '-' + locale.territory.lower()
        if own in BING_MARKETS and not locale.script:
            return own
        if locale.territory in BING_TERRITORY_MARKETS:
            return BING_TERRITORY_MARKETS[locale.territory]
    return BING_LANGUAGE_MARKETS.get(locale.language)
