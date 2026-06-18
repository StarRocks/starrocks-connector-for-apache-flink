#!/usr/bin/env bash
# 01_tag.sh <version>  — create the release commit + tag LOCALLY (no push).
#
# The tag must point at a commit whose pom has the -SNAPSHOT removed, so that
# building from the tag produces release version numbers and the git-commit-id
# fingerprints baked into the jars equal this commit. We do NOT push here: the
# tag is pushed only after stage 03 has proven the build is correct, so a broken
# build never leaves a dangling tag on origin.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

VERSION="${1:-}"
[ -n "$VERSION" ] || die "usage: 01_tag.sh <version>   e.g. 01_tag.sh 1.2.15"
REPO_ROOT="$(resolve_repo)"
TAG="v$VERSION"
cd "$REPO_ROOT"

# --- repo / version / tag-state checks (these live here, not in preflight) ---
[ -z "$(git status --porcelain)" ] || die "working tree has uncommitted changes — commit or stash them first"
pass "no uncommitted changes to tracked files"
git rev-parse -q --verify "refs/tags/$TAG" >/dev/null && die "tag $TAG already exists — delete it (git tag -d $TAG) or pick another version"
pass "tag $TAG does not exist yet"
info "supported Flink minor versions: $(supported_minor_versions "$REPO_ROOT")"

info "Fetching origin…"
git fetch origin

info "Creating branch release-$VERSION from origin/main"
git checkout -B "release-$VERSION" origin/main

# Read srfc.version from origin/main for reference. We do NOT require it to equal $VERSION:
# stage 01 sets it to $VERSION on the release branch below, so cutting an RC (or any version
# that differs from main) needs no prior bump+merge on main. main is never modified here.
srfc="$(pom_srfc_version "$REPO_ROOT")"
[ -n "$srfc" ] || die "could not read <srfc.version> from pom.xml on origin/main"
info "pom srfc.version on origin/main is '$srfc'; releasing as '$VERSION'"

# The pom must currently be a -SNAPSHOT; if not, something is off.
pom_is_snapshot "$REPO_ROOT" \
  || die "the project <version> in pom.xml is not a -SNAPSHOT on origin/main — unexpected; inspect the <version>\${srfc.version}_flink-... line"

# Set srfc.version to the release version (the property value only; the project <version>
# derives from it). This lives only on the release branch — main keeps its own version. A no-op
# when it already equals $VERSION. Replaces the old "must already equal $VERSION or die" gate,
# so RC / off-main versions can be cut without first bumping main.
if [ "$srfc" != "$VERSION" ]; then
  info "Setting srfc.version: '$srfc' -> '$VERSION' (release branch only; main untouched)"
  sed -i "s#<srfc.version>${srfc}</srfc.version>#<srfc.version>${VERSION}</srfc.version>#" pom.xml
  [ "$(pom_srfc_version "$REPO_ROOT")" = "$VERSION" ] \
    || die "failed to set srfc.version to '$VERSION' — inspect the <srfc.version> line in pom.xml"
  pass "pom srfc.version set to $VERSION"
else
  pass "pom srfc.version already == $VERSION"
fi

info "Removing -SNAPSHOT from the project <version>"
# Operate only on the single project-version line; remove its -SNAPSHOT suffix.
sed -i 's#\(<version>${srfc.version}_flink-${flink.minor.version}\)-SNAPSHOT</version>#\1</version>#' pom.xml

# Verify the edit landed exactly as intended (determinism over trust).
grep -qE '<version>\$\{srfc.version\}_flink-\$\{flink.minor.version\}</version>' pom.xml \
  || die "de-SNAPSHOT edit did not produce the expected version line — inspect pom.xml"
pom_is_snapshot "$REPO_ROOT" && die "a -SNAPSHOT project version is still present after the edit — aborting"
pass "pom project version is now: \${srfc.version}_flink-\${flink.minor.version}  (=> ${VERSION}_flink-<minor>)"

git add pom.xml
git commit -m "[Release] flink-connector-starrocks $VERSION"
git tag "$TAG"
COMMIT="$(git rev-parse "$TAG")"

echo
info "${C_GRN}Tag $TAG created locally${C_RST} at $COMMIT"
cat <<EOF

Next:
  scripts/02_install_sdk.sh $VERSION     # checkout the tag, install the SDK from it
  scripts/03_build_verify.sh             # build + verify every Flink version (no deploy)
  git push origin $TAG                   # push ONLY after 03 passes
  scripts/04_deploy.sh                   # the irreversible publish

To undo this (before pushing):
  git tag -d $TAG && git checkout main && git branch -D release-$VERSION
EOF
