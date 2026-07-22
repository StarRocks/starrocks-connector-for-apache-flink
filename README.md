# StarRocks Connector for Apache Flink®

The connector supports to read from and write to StarRocks through Apache Flink®.

## Modules and supported Flink versions

The repository is a Maven multi-module build with one published connector artifact
per Flink major version:

| Module | Artifact | Flink | Java |
|--------|----------|-------|------|
| `flink-connector-starrocks-1.x` | `com.starrocks:flink-connector-starrocks-1.x:<version>_flink-<minor>` | 1.16 – 1.20 | 8+ |
| `flink-connector-starrocks-2.x` | `com.starrocks:flink-connector-starrocks-2.x:<version>_flink-<minor>` | 2.0 – 2.3 | 11+ (17+ for Flink 2.2+) |
| `flink-connector-starrocks-common` | not published | – | 8+ |
| `flink-connector-starrocks-tests` | not published | – | 8+ |

`flink-connector-starrocks-common` holds the version-agnostic connector code. It is
compiled against the exact Flink version selected for each build and shaded into the
connector jar, so it is never published or resolved on its own. The version modules
contain only the thin layer that touches Flink APIs which differ between the majors
(sink/source functions, table factories, the unified-sink writer glue).
`flink-connector-starrocks-tests` holds the version-portable test suite; each build
runs it against the connector module selected for that build, so shared behavior is
tested once per Flink version instead of being maintained twice.

Build a connector for a specific Flink version with:

```bash
# one-time on a fresh machine
mvn -f starrocks-stream-load-sdk clean install -DskipTests

sh build.sh 1.20    # -> flink-connector-starrocks-1.x/target/...jar (JDK 8+)
sh build.sh 2.0     # -> flink-connector-starrocks-2.x/target/...jar (JDK 11+; Flink 2.2+ needs JDK 17+)
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
