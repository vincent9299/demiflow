"""Merge the result records of several sources into one ordered response.

A worker search returns plain result records (dictionaries) from one source.
``merge`` combines the records of all successful sources of a query. The
behaviour is a stable contract: stored ``response_json`` payloads, candidate
normalization in ``collect.searxng`` and business readers depend on its fields
and order.

* Records with one of the keys ``suggestion``, ``answer``, ``correction``,
  ``infobox`` or ``engine_data`` are collected separately; every other record
  is a main result.
* Every record receives the standard fields (``url``, ``template``, ``engine``,
  ``parsed_url``, ``title``, ``content``, ``img_src``, ``thumbnail``,
  ``priority``, ``engines``, ``positions``, ``score``, ``category``,
  ``publishedDate``, ``iframe_src``); URLs default to ``http``, text has its
  whitespace collapsed and content equal to the title is dropped.
* Main results are the same result when template, URL without scheme and image
  URL match (image results: template, full URL and image URL). A duplicate adds
  its source and position, keeps the longer title and content, fills missing
  fields and upgrades the URL to a secure scheme.
* score = product of source weights x occurrences x sum(1 / position).
* Results are ordered by score; afterwards a result joins an earlier result of
  the same category, template and image presence when that group still accepts
  members (8) and is near enough (20 slots).
"""
from __future__ import annotations

import datetime
import re
from urllib.parse import urlparse

GROUP_MEMBERS = 8
GROUP_DISTANCE = 20
_SPACES = re.compile(r'( |\t|\n)+', re.M | re.U)


def plain(value):
    """JSON-compatible copy used for worker output (sets sorted, dates as ISO text)."""
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((plain(v) for v in value), key=str)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _with_scheme(url):
    parsed = urlparse(url)
    return parsed._replace(scheme=parsed.scheme or 'http').geturl()


def standard_record(item, engine):
    """Copy ``item`` with the standard fields filled and normalized."""
    record = dict(item)
    record['engine'] = record.get('engine') or engine or ''
    for name, default in (('url', None), ('template', 'default.html'), ('parsed_url', None),
                          ('title', ''), ('content', ''), ('img_src', ''), ('thumbnail', ''),
                          ('priority', ''), ('positions', ''), ('score', 0), ('category', ''),
                          ('publishedDate', None), ('iframe_src', None)):
        record.setdefault(name, default)
    engines = record.get('engines')
    record['engines'] = set(engines) if isinstance(engines, (set, frozenset, list, tuple)) else set()
    if 'infobox' in record:
        record.setdefault('urls', [])
        record.setdefault('attributes', [])
    if 'answer' in record:
        record['template'] = 'answer/legacy.html'

    published = record['publishedDate']
    if published:
        try:
            record['pubdate'] = published.strftime('%Y-%m-%d %H:%M:%S%z')
        except ValueError:
            record['publishedDate'] = None

    url = record['url']
    if url and not record['parsed_url']:
        if isinstance(url, str):
            record['parsed_url'] = urlparse(url)
        else:
            record['url'], record['parsed_url'] = '', None
    parsed = record['parsed_url']
    if parsed:
        if parsed.netloc.startswith('xn--'):
            parsed = parsed._replace(netloc=parsed.netloc.encode().decode('idna'))
        parsed = parsed._replace(scheme=parsed.scheme or 'http')
        record['parsed_url'], record['url'] = parsed, parsed.geturl()
    if record.get('infobox'):
        for link in record['urls']:
            if link.get('url'):
                link['url'] = _with_scheme(link['url'])
        if record.get('id'):
            record['id'] = _with_scheme(record['id'])

    for name in ('title', 'content'):
        text = record[name]
        if text and not isinstance(text, str):
            text = str(text)
        record[name] = _SPACES.sub(' ', text).strip() if text else text
    if record['content'] == record['title']:
        record['content'] = ''
    if record['engine']:
        record['engines'].add(record['engine'])
    return record


def identity(record):
    """Key under which duplicate main results merge."""
    if record['template'] == 'images.html':
        return ('images.html', record['url'], record['img_src'])
    parsed = record['parsed_url']
    if not parsed:
        raise ValueError("Search result has no usable 'url'")
    return (record['template'], parsed.netloc, parsed.path, parsed.params, parsed.query,
            parsed.fragment, record['img_src'])


class Merger:
    """Collect source records; ``sources`` maps name -> {weight, categories, paging}."""

    def __init__(self, sources):
        self.sources = sources
        self.main = {}
        self.infoboxes = []
        self.answers = []
        self.answer_keys = set()
        self.suggestions = set()
        self.corrections = set()
        self.engine_data = {}
        self.paging = False

    def weight(self, name, default=None):
        source = self.sources.get(name)
        return default if source is None else float(source.get('weight', 1.0))

    def add(self, engine, records):
        position = 0
        for item in list(records):
            record = standard_record(item, engine)
            if 'suggestion' in record:
                self.suggestions.add(record['suggestion'])
            elif 'answer' in record:
                if record['answer'] not in self.answer_keys:
                    self.answer_keys.add(record['answer'])
                    self.answers.append(record)
            elif 'correction' in record:
                self.corrections.add(record['correction'])
            elif 'infobox' in record:
                self._add_infobox(record)
            elif 'engine_data' in record:
                if record['engine']:
                    self.engine_data.setdefault(record['engine'], {})[record['key']] = record['engine_data']
            else:
                position += 1
                self._add_main(record, position)
        if engine in self.sources and self.sources[engine].get('paging'):
            self.paging = True

    def _add_main(self, record, position):
        key = identity(record)
        existing = self.main.get(key)
        if existing is None:
            record['positions'] = [position]
            self.main[key] = record
            return
        for name in ('content', 'title'):
            if len(record[name] or '') > len(existing[name] or ''):
                existing[name] = record[name]
        for name, value in record.items():
            if name not in existing and value != '':
                existing[name] = value
        existing['engines'].add(record['engine'] or '')
        mine, theirs = existing['parsed_url'], record['parsed_url']
        if mine and not mine.scheme.endswith('s') and theirs and theirs.scheme.endswith('s'):
            existing['parsed_url'] = mine._replace(scheme=theirs.scheme)
            existing['url'] = existing['parsed_url'].geturl()
        existing['positions'].append(position)

    def _add_infobox(self, box):
        box_id = box.get('id')
        merged = False
        if box_id is not None:
            for existing in self.infoboxes:
                if existing.get('id') == box_id:
                    self._merge_infobox(existing, box)
                    merged = True
        if not merged:
            self.infoboxes.append(box)

    def _merge_infobox(self, origin, other):
        heavier = self.weight(other['engine'], 1) > self.weight(origin['engine'], 1)
        if heavier:
            origin['engine'] = other['engine']
        origin['engines'] |= other['engines']
        if other['urls']:
            links = origin.get('urls', [])
            for link in other['urls']:
                entity = link.get('entity')
                if not any((entity is not None and entity == seen.get('entity')) or seen.get('url') == link.get('url')
                           for seen in origin.get('urls', [])):
                    links.append(link)
            origin['urls'] = links
        if other['img_src'] and (not origin['img_src'] or heavier):
            origin['img_src'] = other['img_src']
        if other['attributes']:
            if not origin['attributes']:
                origin['attributes'] = other['attributes']
            else:
                known = {attr.get(k) for attr in origin['attributes'] for k in ('label', 'entity') if attr.get(k)}
                for attr in other['attributes']:
                    if attr.get('label') not in known and attr.get('entity') not in known:
                        origin['attributes'].append(attr)
        if other['content'] and (not origin['content'] or len(other['content']) > len(origin['content'])):
            origin['content'] = other['content']

    def _score(self, record):
        weight = 1.0
        for name in record['engines']:
            if name in self.sources:
                weight *= self.weight(name)
        weight *= len(record['positions'])
        if record['priority'] == 'low':
            return 0
        if record['priority'] == 'high':
            return weight * len(record['positions'])
        return sum(weight / position for position in record['positions'])

    def ordered(self):
        for record in self.main.values():
            record['score'] = self._score(record)
        ranked = sorted(self.main.values(), key=lambda r: r['score'], reverse=True)
        ordered, groups = [], {}
        for record in ranked:
            source = self.sources.get(record['engine'] or '')
            if source is not None:
                categories = source.get('categories') or []
                record['category'] = categories[0] if categories else ''
            image = 'img_src' if (record['thumbnail'] or record['img_src']) else ''
            key = f"{record['category']}:{record['template']}:{image}"
            group = groups.get(key)
            if group is not None and group['free'] > 0 and len(ordered) - group['index'] < GROUP_DISTANCE:
                index = group['index']
                ordered.insert(index, record)
                for other in groups.values():
                    if other['index'] >= index:
                        other['index'] += 1
                group['free'] -= 1
            else:
                ordered.append(record)
                groups[key] = {'index': len(ordered), 'free': GROUP_MEMBERS}
        return ordered

    def payload(self):
        answers = sorted(self.answers, key=lambda r: r['template'])
        return {'results': plain(self.ordered()), 'infoboxes': plain(self.infoboxes),
                'answers': plain(answers), 'suggestions': sorted(self.suggestions),
                'corrections': sorted(self.corrections), 'engine_data': plain(self.engine_data),
                'paging': self.paging}


def validate(engine, records):
    """Raise when records returned by source ``engine`` cannot be merged."""
    merger = Merger({engine: {'name': engine}})
    merger.add(engine, records)
    merger.ordered()


def merge(sources, groups):
    """``sources``: [{name, weight, paging, categories}]; ``groups``: [{engine, results}]."""
    merger = Merger({s['name']: s for s in sources})
    for group in groups:
        merger.add(group['engine'], group['results'])
    return merger.payload()
