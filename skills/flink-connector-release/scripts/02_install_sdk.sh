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
[ -z "$(git status --porcelain --untracked-files=no)" ] || die "working tree dirty — commit/stash before checking out the tag"

info "Checking out $TAG (detached HEAD) so all builds carry the tag's commit"
git checkout --quiet "$TAG"
EXPECTED_COMMIT="$(git rev-parse HEAD)"

SDK_VER="$(grep -m1 -oE '<version>[^<]+' starrocks-stream-load-sdk/pom.xml | sed 's/.*>//')"
info "Installing starrocks-stream-load-sdk:$SDK_VER from the tag into $MAVEN_REPO"
( cd starrocks-stream-load-sdk && "$MVN" clean install -DskipTests )

# Verify the artifact that the connector build will actually shade in.
SDK_JAR="$MAVEN_REPO/com/starrocks/starrocks-stream-load-sdk/$SDK_VER/starrocks-stream-load-sdk-$SDK_VER-jar-with-dependencies.jar"
[ -f "$SDK_JAR" ] || die "installed SDK jar-with-dependencies not found at $SDK_JAR"
sdk_id="$(unzip -p "$SDK_JAR" stream-load-sdk-git.properties 2>/dev/null | prop_commit_id)"
if [ "$sdk_id" = "$EXPECTED_COMMIT" ]; then
  pass "installed SDK commit matches the tag ($EXPECTED_COMMIT)"
else
  die "installed SDK commit=$sdk_id != tag $EXPECTED_COMMIT — installation did not pick up the tag's code"
fi

echo
info "${C_GRN}SDK installed and pinned to $TAG${C_RST}. Next: scripts/03_build_verify.sh"
