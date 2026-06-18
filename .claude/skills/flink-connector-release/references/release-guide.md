# StarRocks Flink Connector — Release Guide

This guide describes **what** a release of `flink-connector-starrocks` involves, so you can
understand the process end to end. It is intentionally high-level — for the exact commands and
scripts that carry out each step, see `SKILL.md` next to this guide.

> Examples use version **1.2.15** (tag **v1.2.15**). Substitute your version.

## Background (read once — it explains why each step exists)

- **Version scheme.** Each Flink version is published under its own coordinates,
  `com.starrocks:flink-connector-starrocks:<version>_flink-<minor>` (e.g. `1.2.15_flink-1.20`), one
  per supported Flink minor version. The project version normally carries a `-SNAPSHOT` suffix;
  releasing means producing builds without it.
- **The stream-load SDK is bundled in.** The connector shades `starrocks-stream-load-sdk` (a
  `1.1-SNAPSHOT`) into its jar; the SDK is never published on its own. The catch: a build can pick up
  a *stale* copy of that SDK from a remote snapshot repo — so the process installs the SDK from the
  exact release commit first, then verifies the bundled SDK matches.
- **Commit fingerprints.** Every jar embeds the git commit it was built from (in
  `starrocks-connector-git.properties` and `stream-load-sdk-git.properties`). Building from the
  release tag makes both equal the tag commit — which is what the verification step checks.

## Prerequisites (one-time per release machine)

- JDK 8 and Maven.
- **Publish rights to the `com.starrocks` namespace — the access-controlled part.** The namespace is
  owned by the StarRocks org, so you publish with a Central Portal user token from an account the
  maintainers/admins have granted access to (you cannot self-provision this — get it from the
  maintainers). Put the token in `~/.m2/settings.xml` under a server whose id is `central` (the id
  the publishing plugin looks for):

  ```xml
  <servers>
    <server>
      <id>central</id>
      <username>TOKEN_USERNAME</username>
      <password>TOKEN_PASSWORD</password>
    </server>
  </servers>
  ```
- **A GPG signing key** (Maven Central verifies signatures). Use the project's designated release
  signing key if the maintainers have one — ask them rather than assuming. Central does not require a
  specific org key; it only checks that each artifact has a valid signature whose public key is on a
  keyserver, so a maintainer may also generate one:

  ```bash
  gpg --full-generate-key                                    # create a key pair
  gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>  # publish the public key
  ```

  Either way, configure the passphrase (and key name) in `~/.m2/settings.xml` under a `release`
  profile so `mvn -Prelease` signs without prompting — or rely on gpg-agent:

  ```xml
  <profiles>
    <profile>
      <id>release</id>
      <properties>
        <gpg.keyname>KEY_ID</gpg.keyname>
        <gpg.passphrase>PASSPHRASE</gpg.passphrase>
      </properties>
    </profile>
  </profiles>
  ```
- You choose the version when you tag: the release sets `srfc.version` to it on the release tag,
  so it need not already match on `main`, and RC versions (e.g. `1.2.15-RC0`) are supported.

## The release process

1. **Tag.** Create a release tag, based on the latest `main`, pointing at a commit that has the
   `-SNAPSHOT` removed from the project version. Before tagging, make sure `main`'s user docs
   (`docs/content/connector-sink.md` and `connector-source.md`) already list this release in their
   **Version requirements** table; if not, add the row (or consciously decide to skip it) first.
2. **Install the SDK from the tag.** Check out the tag, then install the stream-load SDK locally from
   it, so the connector bundles this release's SDK rather than a stale remote one:

   ```bash
   cd starrocks-stream-load-sdk && mvn clean install -DskipTests
   ```
3. **Deploy.** Build and publish the connector to Maven Central with `deploy.sh`, once per supported
   Flink version (publishing is irreversible — a published jar can never be changed, so each version
   is strictly validated first):

   ```bash
   bash deploy.sh 1.20    # run once for each supported Flink minor version (1.16 … 1.20)
   ```
4. **Verify on Maven Central.** Download each published jar and confirm both embedded commit
   fingerprints (connector and bundled SDK) equal the tag commit, and the version is not a snapshot.
5. **Release notes.** Write release notes for this version and create the GitHub release on the tag,
   summarizing the features, enhancements, and bug fixes (and the contributors) since the previous
   release.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Bundled SDK commit ≠ tag | The SDK wasn't installed from the tag, or a stale remote snapshot was used. |
| Connector version is a snapshot | Not built from the release tag, or `-SNAPSHOT` wasn't removed before tagging. |
| Signing fails | No GPG key available, or the public key isn't on a keyserver. |
| Publish rejected (auth error) | Wrong/missing Central credentials, or no publish rights on `com.starrocks`. |
| One Flink version failed mid-publish | Already-published versions can't be overwritten — fix the cause and redo only the failed version. |
| A jar 404s right after publishing | The Central mirror can lag a few minutes — wait and retry. |
