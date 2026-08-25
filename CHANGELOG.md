# Changelog

## [1.2.0](https://github.com/jtrusty/data-ingest-aws/compare/v1.1.0...v1.2.0) (2026-08-25)


### Features

* **bronze:** make the source_key table prefix optional ([783917f](https://github.com/jtrusty/data-ingest-aws/commit/783917f63584f1486c2f85f7b6802ccb0e33f707))
* **config:** let tables inherit database and schema from the source ([7acdf43](https://github.com/jtrusty/data-ingest-aws/commit/7acdf4384ca0620147691060a34f3ee583d069a6))
* define Bronze tables from the union of every pending run's schema ([ef43872](https://github.com/jtrusty/data-ingest-aws/commit/ef438724a277bdcf0d5949dc55dceb90bf6ca93f))


### Bug Fixes

* **bronze:** normalize column names to lowercase in generated SQL ([8a586d1](https://github.com/jtrusty/data-ingest-aws/commit/8a586d1f074fcb98775bf7da763c54099af97366))
* **bronze:** use an explicit INSERT column list; Trino has no INSERT * ([42bb8a7](https://github.com/jtrusty/data-ingest-aws/commit/42bb8a72b103f5826653fa83498e34965a1072bc))
* **bronze:** use Hive backtick quoting in DDL, not Trino double quotes ([5662c89](https://github.com/jtrusty/data-ingest-aws/commit/5662c893da861fff8d398a2e57a47582a53c9eed))
* cast a non-conforming batch to the pinned schema before declaring drift ([fe14651](https://github.com/jtrusty/data-ingest-aws/commit/fe146516ca007b70541a5abc3cd02286ce3e7272))
* detect an Iceberg catalog entry whose metadata file was deleted ([65e6b71](https://github.com/jtrusty/data-ingest-aws/commit/65e6b717f23dd9ff4a036b092319efacac356774))
* refuse drifted landing runs, and stop the manifest claiming a schema it lacks ([1c8bc2f](https://github.com/jtrusty/data-ingest-aws/commit/1c8bc2f69b7006a868a13d53645b503f38280a42))


### Documentation

* correct the Redshift grant -- USAGE only, not GRANT SELECT ([b0f806a](https://github.com/jtrusty/data-ingest-aws/commit/b0f806a6753950a334f2fd50810cc970726d19dc))
* full IAM role reference, derived from the calls the code actually makes ([b99796d](https://github.com/jtrusty/data-ingest-aws/commit/b99796d3a203d60f6666e86d2ab0cbe3e110b4c6))
* state what the code does, not what it used to do ([da7b9fc](https://github.com/jtrusty/data-ingest-aws/commit/da7b9fc8fff9a3e0e8126210a6f868b331a94903))

## [1.1.0](https://github.com/jtrusty/data-ingest-aws/compare/v1.0.0...v1.1.0) (2026-08-25)


### Features

* **bronze:** apply added source columns instead of dropping them silently ([de0d6ac](https://github.com/jtrusty/data-ingest-aws/commit/de0d6ac164543ccd6cee30186cf731b4b2159185))


### Bug Fixes

* **config:** reject unknown keys instead of silently ignoring them ([540aa54](https://github.com/jtrusty/data-ingest-aws/commit/540aa545af38fdb973dd3f8f589a085ef54bd749))
* pin Parquet types from source metadata, create Athena tables, skip Bronze cleanly ([35b0533](https://github.com/jtrusty/data-ingest-aws/commit/35b0533b6a80fd559ccc42dc4cdd2033042c301a))


### Documentation

* bring the README up to date with Bronze, and regroup by task ([a29f8e1](https://github.com/jtrusty/data-ingest-aws/commit/a29f8e135d4d232273ee09d535d398ab34e5eeaa))
* **config:** comment out partition_by and flag its one-shot nature ([7f44571](https://github.com/jtrusty/data-ingest-aws/commit/7f445712fd1b9adc3ab3be20dd9d6189aba4374b))
* state the versioning posture explicitly ([c123ed5](https://github.com/jtrusty/data-ingest-aws/commit/c123ed58b440b0c4df4bd0fad5eccd23d6b8de7a))

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
