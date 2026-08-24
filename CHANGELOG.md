# Changelog

## [1.0.0](https://github.com/jtrusty/data-ingest-aws/compare/v0.1.2...v1.0.0) (2026-08-24)


### ⚠ BREAKING CHANGES

* name job scripts <layer>_load[_<source>]
* **config:** drop the deprecated config spellings
* **config:** one s3:// location per layer, replacing bucket + prefix
* **config:** move the checkpoint table under landing

### Features

* **bronze:** Landing -&gt; Iceberg Bronze via Athena MERGE ([47f4f83](https://github.com/jtrusty/data-ingest-aws/commit/47f4f832f3f893980e8369007ba3869a18f2b479))


### Performance Improvements

* lazy imports so the Bronze job fits the smallest DPU, and partition Bronze ([6dc7fa4](https://github.com/jtrusty/data-ingest-aws/commit/6dc7fa461fe354801dd8de7c69f5290d2988c824))


### Documentation

* **config:** make the example show the layer symmetry, and validate it ([dd3be5a](https://github.com/jtrusty/data-ingest-aws/commit/dd3be5a086430a9cbd3f055e1cd510aebbf15950))


### Code Refactoring

* **config:** drop the deprecated config spellings ([ad7b309](https://github.com/jtrusty/data-ingest-aws/commit/ad7b309e03baf8fb0ac3988ef054249116fb0aa4))
* **config:** move the checkpoint table under landing ([9423be9](https://github.com/jtrusty/data-ingest-aws/commit/9423be98dc34f4cfe835d018e7fa29c2e806b368))
* **config:** one s3:// location per layer, replacing bucket + prefix ([7f486fa](https://github.com/jtrusty/data-ingest-aws/commit/7f486fa4a969b0de4c733c8034f6db2f78e2fe94))
* name job scripts &lt;layer&gt;_load[_&lt;source&gt;] ([f638661](https://github.com/jtrusty/data-ingest-aws/commit/f638661d0b39fdb6d78d17f1e292a0052af7b7a8))

## [0.1.2](https://github.com/jtrusty/data-ingest-aws/compare/v0.1.1...v0.1.2) (2026-08-24)


### Bug Fixes

* **logging:** emit under Glue, where the host pre-configures root logging ([a92f163](https://github.com/jtrusty/data-ingest-aws/commit/a92f163c3cdd078b7a787b3cac111456476f1eb7))

## [0.1.1](https://github.com/jtrusty/data-ingest-aws/compare/v0.1.0...v0.1.1) (2026-08-24)


### Bug Fixes

* **build:** quote the extras spec so mise install works on Windows ([3dea1cd](https://github.com/jtrusty/data-ingest-aws/commit/3dea1cd50d76971c5afa545a46d2de0a639f60b4))
* **snowflake:** cap memory so a large full load survives 1 DPU ([a1ffcfa](https://github.com/jtrusty/data-ingest-aws/commit/a1ffcfa047d24901b3db4db52ddcc37194232363))

## Changelog

All notable changes to this project are documented here. This file is
maintained automatically by [Release Please](https://github.com/googleapis/release-please)
based on [Conventional Commits](https://www.conventionalcommits.org/).
