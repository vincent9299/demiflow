# Third-party source notices

This distribution includes SearXNG source code, copyright its contributors,
licensed under **AGPL-3.0-or-later**. Original SPDX headers remain on each file.
The complete license and author list are bundled at
`demiflow/_vendor/searxng/LICENSE` and `AUTHORS.rst`.

Origin: https://github.com/searxng/searxng

The imported baseline is the supplied SearXNG working snapshot, including
pre-existing local changes. It did not carry an independent upstream Git commit
or frozen release version. `BASELINE.json` records its full SHA256 file manifest;
`baseline.tar.gz` preserves the exact corresponding source. Do not label this
snapshot as an unmodified upstream release.

Native integration changes, including the replacement search coordinator and
network ownership, are recorded in `LOCAL_CHANGES.md` beside the baseline. Full
bundled source, native modifications and this notice ship in source and wheel
archives. Maintain these notices and the corresponding source when distributing
updates. The website UI is not used by the native Dataset operator.
