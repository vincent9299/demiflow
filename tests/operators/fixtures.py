"""Ordinary functions/actors; no registration or operator wrapper classes."""
import asyncio
import threading

calls = []
entered = threading.Event()
release = threading.Event()


async def scale(value, *, multiplier):
    calls.append(('scale', value, multiplier))
    return [value * multiplier]


async def pictures(query, *, source):
    calls.append(('pictures', query, source))
    return {'candidates': [{'uri': source['uri'], 'sha': source['sha256'], '_distance': .25}]
            * (2 if query == 'pair' else 1)}


def blocking(value):
    entered.set()
    assert release.wait(3)
    calls.append(('finished', value))
    return value


class Counter:
    def __init__(self, start=0, fail_start=False):
        self.value = start
        self.fail_start = fail_start
        self.active = False
        calls.append(('construct', id(self)))

    async def astart(self):
        calls.append(('start', id(self)))
        if self.fail_start:
            raise OSError('fixture startup failed')
        self.active = True

    async def __call__(self, value):
        assert self.active
        calls.append(('invoke', id(self), value))
        if value == -1:
            raise OSError('fixture execution failed')
        if value == -2:
            entered.set()
            await asyncio.Event().wait()
        self.value += value
        return self.value

    async def astop(self):
        calls.append(('stop', id(self)))
        self.active = False

    async def aclose(self):
        calls.append(('close', id(self)))
        self.active = False


class BlockingCounter(Counter):
    def __call__(self, value):
        assert self.active
        return blocking(value)

    async def aclose(self):
        assert any(c[0] == 'finished' for c in calls)
        await super().aclose()
