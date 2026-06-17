#!/usr/bin/env bash
# 02_install_sdk.sh <version>  — checkout the tag, install the stream-load SDK from it.
#
# The connector shades starrocks-stream-load-sdk:1.1-SNAPSHOT into its jar. If
# that SDK isn't installed locally from THIS tag, Maven may pull a stale
# 1.1-SNAPSHOT from the configured snapshot repo, and the connector jar would
# ship an SDK built from a different commit. Installing it here from the tag,
# then verifying the installed artifact's commit, pins the SDK to this release.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

VERSION="${1:-}"
[ -n "$VERSION" ] || die "usage: 02_install_sdk.sh <version>   e.g. 02_install_sdk.sh 1.2.15"
REPO_ROOT="$(resolve_repo)"
TAG="v$VERSION"
MVN="${CUSTOM_MVN:-mvn}"
MAVEN_REPO="${MAVEN_REPO:-$HOME/.m2/repository}"
cd "$REPO_ROOT"

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null || die "tag $TAG not found — run 01_tag.sh first"
[ -z "$(git status --porcelain)" ] || die "working tree dirty — commit/stash before checking out the tag"

info "Checking out $TAG (detached HEAD) so all builds carry the tag's commit"
git checkout --quiet "$TAG"
EXPECTED_COMMIT="$(git rev-parse HEAD)"

info "Installing the stream-load SDK from the tag into $MAVEN_REPO"
( cd starrocks-stream-load-sdk && "$MVN" clean install -DskipTests )

# Verify the artifact the connector build will actually shade in.
verify_installed_sdk "$REPO_ROOT" "$EXPECTED_COMMIT"
pass "installed SDK commit matches the tag ($EXPECTED_COMMIT)"

echo
info "${C_GRN}SDK installed and pinned to $TAG${C_RST}. Next: scripts/03_build_verify.sh"
