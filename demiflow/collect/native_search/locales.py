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
