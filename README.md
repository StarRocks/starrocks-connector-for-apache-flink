# StarRocks Connector for Apache Flink®

The connector supports to read from and write to StarRocks through Apache Flink®.

## Modules and supported Flink versions

The repository is a Maven multi-module build. Both version modules publish the same
artifact, `com.starrocks:flink-connector-starrocks:<version>_flink-<minor>`, and the
`_flink-<minor>` suffix selects the build for a Flink version:

| Module | Flink | Java |
|--------|-------|------|
| `flink-connector-starrocks-1.x` | 1.16 – 1.20 | 8+ |
| `flink-connector-starrocks-2.x` | 2.0 – 2.3 | 11+ |
| `flink-connector-starrocks-common` | not published | 8+ |
| `flink-connector-starrocks-tests` | not published | 8+ |

`flink-connector-starrocks-common` holds the version-agnostic connector code. It is
compiled against the exact Flink version selected for each build and shaded into the
connector jar, so it is never published or resolved on its own. The few public
interfaces whose signatures differ between the majors live in
`src/main/flink1` and `src/main/flink2`, selected by `-Dflink.major`. The version modules
contain only the thin layer that touches Flink APIs which differ between the majors
(sink/source functions, table factories, the unified-sink writer glue).
`flink-connector-starrocks-tests` holds the version-portable test suite; each build
runs it against the connector module selected for that build, so shared behavior is
tested once per Flink version instead of being maintained twice.

A build selects one major: `mvn verify` builds Flink 1.x, and `-Dflink.major=2` builds
Flink 2.x (`build.sh` sets it).

Build a connector for a specific Flink version with:

```bash
# one-time on a fresh machine
mvn -f starrocks-stream-load-sdk clean install -DskipTests

sh build.sh 1.20    # -> flink-connector-starrocks-1.x/target/...jar (JDK 8+)
sh build.sh 2.0     # -> flink-connector-starrocks-2.x/target/...jar (JDK 11+)
```

### Adopting a new Flink version

* **New minor without API breaks** (the normal case): add the version and its
  `flink-connector-kafka` (test-only) mapping to `common.sh`, and add a matrix leg in
  `.github/workflows/ci-pipeline.yml`. No Java changes are needed — the CI matrix
  compiles every module against the exact target version, which proves compatibility.
* **Minor with API breaks**: changes are contained to the affected version module
  (~40 classes); shared logic in the common module is unaffected.
* **New major** (e.g. Flink 3.x): add a new version module — a small pom that inherits
  everything from the parent, plus the API-facing classes — and a mapping line in
  `common.sh`.

## Documentation

For the user manual of the released version of the Flink connector, please visit the StarRocks official documentation.

* [Read data from StarRocks using Apache Flink](https://docs.starrocks.io/docs/unloading/Flink_connector/)
* [Continuously load data from Apache Flink](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/)

For the new features in the snapshot version of the Flink connector, please see the docs in this repo.

* [Read from StarRocks](docs/content/connector-source.md)
* [Write to StarRocks](docs/content/connector-sink.md)

## Release

Maintainers cut releases to Maven Central with the `flink-connector-release` skill in
[`.claude/skills/flink-connector-release`](.claude/skills/flink-connector-release/SKILL.md). It wraps
the repo's `build.sh`/`deploy.sh` with strict per-step verification before the irreversible publish.
See [`references/release-guide.md`](.claude/skills/flink-connector-release/references/release-guide.md)
for the full procedure and background.

## LICENSE

The connector is under the [Apache License 2.0](LICENSE).
