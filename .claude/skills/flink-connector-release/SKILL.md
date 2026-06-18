---
name: flink-connector-release
description: >-
  Release (publish) the StarRocks Flink connector (flink-connector-starrocks) to
  Maven Central. Use this whenever the user wants to cut a connector release, "发版"/"发包"
  the connector, tag a new connector version (e.g. v1.2.15), deploy connector jars to
  Maven Central / Sonatype Central, build & sign release artifacts for the supported Flink
  versions, or verify a published connector release. Trigger even if they only say "release
  the connector", "deploy the flink connector", "push 1.2.x to maven", or name deploy.sh /
  the connector pom version in a release context. This skill enforces strict per-step
  validation BEFORE the irreversible deploy, since a published jar can never be changed.
---

# Releasing flink-connector-starrocks to Maven Central

## What this skill does

Publishes `com.starrocks:flink-connector-starrocks` for every supported Flink minor
version. The driving constraint: **a jar, once deployed to Maven Central, can never be
modified or deleted.** So the whole design is *verify locally, then publish* — the deploy
step refuses to run until a local build has proven, for the exact tag commit, that every
artifact is correct. The build/deploy themselves go through the repo's own `build.sh` /
`deploy.sh`; this skill wraps them with strict verification gates and a confirmation.

The scripts live in `scripts/` next to this file. They read the version from `pom.xml` and
ask `common.sh` to print the supported Flink list (`bash common.sh supported-minor-versions`)
— nothing is hardcoded — so they keep working as those change. For the background, the manual
procedure, and troubleshooting — the *why* behind each step (version scheme, SDK shading, the
stale-snapshot trap, Central Portal) — see `references/release-guide.md`, a standalone
user-facing release guide.

## Before you start

- Run from inside the connector repo (or set `CONNECTOR_REPO=/path/to/repo`).
- The release machine needs: JDK 8, Maven, a GPG signing key, and a `<server><id>central</id>`
  entry in `~/.m2/settings.xml` (Central Portal user token). Stage 00 checks all of this.
- You don't need to pre-set `srfc.version` on `main`: stage 01 sets it to the version you pass
  (on the release branch only), so you can cut an RC or any version that differs from `main`
  without a prior bump+merge. `main` is never modified by the release.
- Decide the version. Example throughout: **1.2.15** → tag **v1.2.15**. Replace as needed.

## The flow (run in order, do not skip a gate)

Each stage is a script under this skill's `scripts/`. From the repo root,
`cd .claude/skills/flink-connector-release/` first — the commands below are relative to it (the scripts
locate the repo automatically, so the working directory doesn't otherwise matter). Stop
immediately if any stage fails — later stages depend on earlier ones having passed.

```
cd .claude/skills/flink-connector-release          # from the repo root
scripts/00_preflight.sh               # environment readiness check (read-only, changes nothing)
scripts/01_tag.sh         1.2.15      # checks repo/tag state, sets srfc.version=1.2.15, commits the de-SNAPSHOT + tags v1.2.15 (no push)
scripts/02_install_sdk.sh 1.2.15      # checkout the tag; install the stream-load SDK FROM the tag
scripts/03_build_verify.sh            # build each version via build.sh + STRICTLY verify; NO deploy
git push origin v1.2.15               # push the tag ONLY after 03 passes
scripts/04_deploy.sh                  # IRREVERSIBLE publish via deploy.sh (gated on 03 + confirmation)
scripts/05_verify_central.sh 1.2.15   # download from Maven Central, re-verify every jar
scripts/06_upload_release.sh RELEASE_NOTES.md   # upload your notes + jars as a DRAFT; do NOT publish
```

Why this order matters:

- **01 before 02/03**: the tag must point at the de-SNAPSHOT commit so the jars' version
  numbers and embedded git fingerprints reflect the release commit.
- **02 before 03**: the connector shades the SDK into its jar. Installing the SDK from the tag
  (stage 02) plus re-checking the bundled SDK's commit in every built jar (stage 03) guarantees
  the *tag's* SDK is what ships, not a stale remote `1.1-SNAPSHOT` with a different commit.
- **03 before push and before 04**: 03 builds every version (via `build.sh`) and asserts on the
  real bytes — release version (no `SNAPSHOT`), connector **and** bundled-SDK git commit both
  equal the tag, SDK classes present. It verifies *all* versions before writing a marker, so you
  never publish 1.16 and then find 1.20 is broken. Pushing the tag only after 03 passes means a
  broken build never leaves a dangling tag on origin. 04 then re-builds via `deploy.sh` rather than
  reusing 03's jars — safe, because the same tag + locally-installed SDK + HEAD reproduce the bytes
  03 verified, and 05 re-checks the published jar. (GPG signing happens in 04 via `deploy.sh`;
  00_preflight confirms a key exists, and a signing failure aborts a version before it publishes.)
- **04 is the only irreversible action.** It refuses to run unless 03's marker matches the
  current commit, and it requires you to type the version (or set `CONFIRM_DEPLOY=<version>`).
- **05** confirms the published bytes — the ones the world consumes — are correct.

## Release notes (stage 06)

**Writing the notes is not scripted** — you (the agent running this) compose them from the template;
stage 06 only uploads them. Copy [`assets/release-note-template.md`](assets/release-note-template.md)
to a file (e.g. `RELEASE_NOTES.md`) and fill it in: `## What's Changed` grouped into **Features** /
**Enhancements** / **BugFix** — one line per PR, `- <desc> [#NNN](pr-url)`; the repo's
`[Feature]` / `[Enhancement]` / `[BugFix]` commit-subject prefixes map straight to these groups —
then **## Contributors** (PR authors as `[handle](profile-url)`). Use the previous release as a
reference, e.g. [v1.2.14](https://github.com/StarRocks/starrocks-connector-for-apache-flink/releases/tag/v1.2.14).
List the merged PRs with `git log --first-parent --pretty='%s' <prev-tag>..v<version>`.

Then **upload** with `scripts/06_upload_release.sh`, which only does the upload: it downloads the
published per-version jars (versions read from the repo) and creates a **draft** release with your
notes + jars attached. It never publishes — a maintainer reviews the draft and publishes it.

```bash
scripts/06_upload_release.sh RELEASE_NOTES.md      # upload your notes + jars as a DRAFT
```

## The validation gate (`scripts/verify_jar.sh`)

This is the heart of the skill and is reused by both 03 (local jar) and 05 (downloaded jar).
Given a jar and the expected tag commit it asserts:

1. `git.build.version` is **not** a `SNAPSHOT` (you are releasing a release build).
2. `starrocks-connector-git.properties` → `git.commit.id` **==** the tag commit.
3. `stream-load-sdk-git.properties` is present (proves the SDK was shaded in) and its
   `git.commit.id` **==** the tag commit (catches a wrong/stale bundled SDK).
4. SDK classes (`com/starrocks/data/load/stream/`) are actually in the jar.
5. (optional) the version equals the expected `1.2.15_flink-<minor>`.

Any failure makes it exit non-zero with a clear `DO NOT DEPLOY` message.

## Re-running and partial failures

- All of 03 / 04 / 05 accept an optional list of Flink minor versions, e.g.
  `scripts/04_deploy.sh 1.20` to act on one version only. With no args they cover every
  supported version (from `common.sh supported-minor-versions`).
- If a deploy fails midway, already-published versions cannot be overwritten. Check the
  Deployments page at <https://central.sonatype.com>, fix the cause, and re-run **only** the
  failed version.
- The verification marker lives at `<repo>/.release/verified-<version>.commit` and is tied to
  a specific commit; it is local scratch state and can be deleted.

## Notes for the agent running this

- Treat every script's non-zero exit as a hard stop; surface its output to the user.
- Never edit a published artifact or suggest "re-deploying to fix" — explain that Central is
  immutable and the fix is a new version.
- `scripts/04_deploy.sh` is outward-facing and irreversible. Do not bypass its confirmation;
  if running non-interactively, the user must explicitly set `CONFIRM_DEPLOY=<version>`.
- `scripts/06_upload_release.sh` only creates a **draft** GitHub release — never publish it; a
  maintainer reviews and publishes. (Re-running requires deleting the previous draft first.)
- These scripts wrap the repo's own `build.sh`/`deploy.sh` with strict verification gates and a
  confirmation step; prefer them for releases. Running `build.sh`/`deploy.sh` directly is fine for
  ad-hoc local builds.
