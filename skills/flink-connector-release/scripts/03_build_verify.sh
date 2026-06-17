#!/usr/bin/env bash
# 03_build_verify.sh [minor ...]  — build every Flink version with the repo's
#                                   build.sh and STRICTLY verify each jar. No deploy.
#
# This is the safety gate before the irreversible deploy. For each Flink version we
# run the repo's `build.sh` (which does `mvn clean package`) and then assert on the
# real bytes via verify_jar.sh: release version (no SNAPSHOT), connector AND
# bundled-SDK git commit both equal the tag, SDK actually bundled. We verify ALL
# requested versions; only if every one passes do we write the marker that
# 04_deploy.sh requires — so we never publish 1.16 and then find 1.20 is broken.
#
# Note: build.sh stops at the `package` phase, so these jars are unsigned. GPG
# signing is exercised by deploy.sh in stage 04 (it uses -Prelease), and
# 00_preflight already checks that a signing key exists.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"
REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"

# Must be on the release tag (detached, pom de-SNAPSHOTed). 02 put us here.
pom_is_snapshot "$REPO_ROOT" && die "pom is still a -SNAPSHOT — checkout the release tag first (run 02_install_sdk.sh)"
SRFC="$(pom_srfc_version "$REPO_ROOT")"
EXPECTED_COMMIT="$(git rev-parse HEAD)"
# The build must come from the clean tagged tree: git-commit-id stamps HEAD, so a dirty worktree
# would ship uncommitted bytes while the fingerprints still read the tag commit (undetectable later).
[ -z "$(git status --porcelain)" ] \
  || die "working tree not clean (uncommitted or untracked files) — 03 must verify the clean tagged build; commit/stash/clean first"
info "Building & verifying ${SRFC} at commit $EXPECTED_COMMIT"

mapfile -t VERSIONS < <(resolve_versions "$REPO_ROOT" "$@")
[ "${#VERSIONS[@]}" -gt 0 ] || die "no Flink versions resolved — is common.sh updated to support 'supported-minor-versions'?"
declare -a RESULT
overall=0

# Build against the locally-installed SDK; don't let Maven refresh 1.1-SNAPSHOT from a remote repo,
# so what 03 verifies is what 04 will publish.
pin_local_snapshots

for m in "${VERSIONS[@]}"; do
  info "──────── build flink $m (via build.sh) ────────"
  if ! bash build.sh "$m"; then
    fail "build.sh failed for flink $m"; RESULT+=("$m FAIL(build)"); overall=1; continue
  fi

  jar="target/flink-connector-starrocks-${SRFC}_flink-${m}.jar"
  if [ ! -f "$jar" ]; then fail "expected jar not found: $jar"; RESULT+=("$m FAIL(no-jar)"); overall=1; continue; fi

  if ! "$SCRIPT_DIR/verify_jar.sh" "$jar" "$EXPECTED_COMMIT" "${SRFC}_flink-${m}"; then
    RESULT+=("$m FAIL(verify)"); overall=1; continue
  fi

  RESULT+=("$m OK")
done

echo
info "── build/verify summary (commit $EXPECTED_COMMIT) ──"
for r in "${RESULT[@]}"; do printf '   %s\n' "$r"; done

MARKER_DIR="$REPO_ROOT/.release"
if [ "$overall" -eq 0 ]; then
  mkdir -p "$MARKER_DIR"
  { echo "$EXPECTED_COMMIT"; printf 'versions: %s\n' "${VERSIONS[*]}"; } > "$MARKER_DIR/verified-$SRFC.commit"
  info "${C_GRN}ALL VERSIONS VERIFIED${C_RST} — wrote $MARKER_DIR/verified-$SRFC.commit"
  echo  "Next:  git push origin v$SRFC   then   scripts/04_deploy.sh"
else
  rm -f "$MARKER_DIR/verified-$SRFC.commit" 2>/dev/null || true
  die "one or more versions FAILED — nothing is verified, do NOT deploy"
fi
