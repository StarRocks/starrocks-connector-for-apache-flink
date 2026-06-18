#!/usr/bin/env bash
# verify_jar.sh — strict validation of ONE connector jar.
#
# This is the gate that protects the irreversible deploy. A deployed jar can
# never be changed, so before publishing (stage 03, on the locally-built jar)
# and after publishing (stage 05, on the jar downloaded from Maven Central) we
# prove the jar is exactly what the tag should produce.
#
# Usage: verify_jar.sh <jar> <expected_commit> [expected_version]
#   <jar>              path to the shaded connector jar (NOT original-/sources/javadoc)
#   <expected_commit>  full 40-char git SHA the jar must have been built from (the tag commit)
#   [expected_version] e.g. 1.2.15_flink-1.18 ; if given, git.build.version must equal it
#
# Exit 0 only if EVERY check passes.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

JAR="${1:-}"; EXPECTED_COMMIT="${2:-}"; EXPECTED_VERSION="${3:-}"
[ -n "$JAR" ] && [ -n "$EXPECTED_COMMIT" ] || die "usage: verify_jar.sh <jar> <expected_commit> [expected_version]"

CONN_PROP=starrocks-connector-git.properties   # written by the connector module
SDK_PROP=stream-load-sdk-git.properties        # written by the SDK module, only present if the SDK jar was shaded in
SDK_CLASS_PATH='com/starrocks/data/load/stream/' # SDK's own classes — sanity that the SDK really got bundled

ok=0; bad=0
yes(){ pass "$1"; ok=$((ok+1)); }
no(){  fail "$1"; bad=$((bad+1)); }

case "$(basename "$JAR")" in
  original-*|*-sources.jar|*-javadoc.jar)
    die "$(basename "$JAR") is not the primary shaded jar (looks like original-/sources/javadoc)" ;;
esac
[ -f "$JAR" ] || die "jar not found: $JAR"

info "Verifying $(basename "$JAR")  (expect commit ${EXPECTED_COMMIT:0:12}…)"
list="$(unzip -l "$JAR")" || die "cannot read jar: $JAR"

# --- connector fingerprint --------------------------------------------------
conn="$(unzip -p "$JAR" "$CONN_PROP" 2>/dev/null || true)"
if [ -z "$conn" ]; then
  no "$CONN_PROP missing from jar"
else
  yes "$CONN_PROP present"
  conn_ver="$(printf '%s\n' "$conn" | prop_build_version)"
  conn_id="$(printf '%s\n' "$conn" | prop_commit_id)"
  case "$conn_ver" in
    "")          no "connector git.build.version missing" ;;
    *SNAPSHOT*)  no "connector git.build.version is a SNAPSHOT: $conn_ver  (you are about to release a snapshot build!)" ;;
    *)           yes "connector git.build.version is a release: $conn_ver" ;;
  esac
  if [ "$conn_id" = "$EXPECTED_COMMIT" ]; then yes "connector git.commit.id matches the tag"
  else no "connector git.commit.id=$conn_id != expected $EXPECTED_COMMIT"; fi
fi

# --- bundled SDK fingerprint ------------------------------------------------
# This is the subtle one: the SDK is shaded into the connector jar. If a stale
# 1.1-SNAPSHOT was pulled from a remote repo instead of the tag's SDK, the jar
# looks fine but carries a different SDK commit. Catch that here.
sdk="$(unzip -p "$JAR" "$SDK_PROP" 2>/dev/null || true)"
if [ -z "$sdk" ]; then
  no "$SDK_PROP missing — the stream-load SDK was not bundled"
else
  yes "$SDK_PROP present (SDK bundled)"
  sdk_id="$(printf '%s\n' "$sdk" | prop_commit_id)"
  if [ "$sdk_id" = "$EXPECTED_COMMIT" ]; then yes "bundled SDK git.commit.id matches the tag"
  else no "bundled SDK git.commit.id=$sdk_id != expected $EXPECTED_COMMIT  (wrong/stale SDK shaded in — re-run 02_install_sdk.sh on the tag)"; fi
fi

# Pure-bash substring test (no pipe): grep -q would SIGPIPE the producer and,
# under `set -o pipefail`, make a found match look like a failure.
case "$list" in
  *"$SDK_CLASS_PATH"*) yes "SDK classes present ($SDK_CLASS_PATH)" ;;
  *)                   no  "SDK classes ($SDK_CLASS_PATH) not found in jar" ;;
esac

# --- optional exact version --------------------------------------------------
if [ -n "$EXPECTED_VERSION" ]; then
  if [ "${conn_ver:-}" = "$EXPECTED_VERSION" ]; then yes "version == $EXPECTED_VERSION"
  else no "git.build.version=${conn_ver:-<none>} != expected $EXPECTED_VERSION"; fi
fi

echo
if [ "$bad" -eq 0 ]; then
  info "${C_GRN}ALL $ok CHECKS PASSED${C_RST} — $(basename "$JAR")"
  exit 0
else
  die "$bad check(s) FAILED, $ok passed — DO NOT DEPLOY $(basename "$JAR")"
fi
