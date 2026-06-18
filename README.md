# StarRocks Connector for Apache Flink®

The connector supports to read from and write to StarRocks through Apache Flink®.

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

The connector is under the [Apache License 2.0](LICENSE.txt).
