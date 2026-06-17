#!/usr/bin/env bash
# 05_verify_central.sh <version> [minor ...]  — final confirmation from Maven Central.
#
# Download each published jar and re-run the same strict checks as the pre-deploy
# gate, this time against the bytes the world will actually consume. Both git
# fingerprints (connector + bundled SDK) must equal the tag commit.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

VERSION="${1:-}"
[ -n "$VERSION" ] || die "usage: 05_verify_central.sh <version> [minor ...]   e.g. 05_verify_central.sh 1.2.15"
shift || true
REPO_ROOT="$(resolve_repo)"
TAG="v$VERSION"
EXPECTED_COMMIT="$(git -C "$REPO_ROOT" rev-parse "${TAG}^{commit}" 2>/dev/null || true)"
[ -n "$EXPECTED_COMMIT" ] || die "cannot resolve commit for tag $TAG — is the tag present locally?"

BASE="https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
mapfile -t VERSIONS < <(resolve_versions "$REPO_ROOT" "$@")
[ "${#VERSIONS[@]}" -gt 0 ] || die "no Flink versions resolved — is common.sh updated to support 'supported-minor-versions'?"

info "Verifying ${VERSION} on Maven Central (expect commit $EXPECTED_COMMIT)"
declare -a RESULT; overall=0

for m in "${VERSIONS[@]}"; do
  art="flink-connector-starrocks-${VERSION}_flink-${m}.jar"
  url="$BASE/${VERSION}_flink-${m}/$art"
  info "──────── flink $m ────────"
  # Central's repo1 mirror can lag a few minutes after publish — retry a little.
  got=0
  for attempt in 1 2 3 4 5; do
    if curl -fsSL -o "$WORK/$art" "$url"; then got=1; break; fi
    warn "download attempt $attempt failed (mirror may be syncing): $url"; sleep 15
  done
  if [ "$got" -ne 1 ]; then fail "could not download $url"; RESULT+=("$m FAIL(download)"); overall=1; continue; fi

  if "$SCRIPT_DIR/verify_jar.sh" "$WORK/$art" "$EXPECTED_COMMIT" "${VERSION}_flink-${m}"; then
    RESULT+=("$m OK")
  else
    RESULT+=("$m FAIL(verify)"); overall=1
  fi
done

echo
info "── Maven Central verification summary ──"; for r in "${RESULT[@]}"; do printf '   %s\n' "$r"; done
if [ "$overall" -eq 0 ]; then info "${C_GRN}RELEASE VERIFIED ON MAVEN CENTRAL${C_RST} — ${VERSION}"; exit 0
else die "some artifacts failed verification on Maven Central — investigate immediately"; fi
