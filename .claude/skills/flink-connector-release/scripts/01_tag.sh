#!/usr/bin/env bash
# 01_tag.sh <version>  — create the release commit + tag LOCALLY (no push).
#
# The tag must point at a commit whose poms have the -SNAPSHOT removed, so that
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

# The user docs on main should already list this release in their "Version requirements" table.
# Check it now (the tree here == origin/main), while a missing row is still cheap to fix. Not a hard
# error: confirm to proceed, or set CONFIRM_DOCS_VERSION=$VERSION when running non-interactively.
check_docs_version "$REPO_ROOT" "$VERSION"

# The poms must currently be -SNAPSHOTs; if not, something is off.
pom_is_snapshot "$REPO_ROOT" \
  || die "the project versions are not -SNAPSHOTs on origin/main — unexpected; inspect the <version> lines of the poms"

# Set srfc.version and the parent version to the release version, and drop -SNAPSHOT from the
# connector version (it derives from srfc.version) in every module pom. This lives only on the
# release branch — main keeps its own version — so RC / off-main versions can be cut without first
# bumping main. set_release_version verifies every edit landed (determinism over trust).
info "Setting the project versions to '$VERSION': srfc.version was '$srfc', parent was '$(pom_parent_version "$REPO_ROOT")' (release branch only; main untouched)"
set_release_version "$REPO_ROOT" "$VERSION"
pass "project versions are now: parent $VERSION, connector \${srfc.version}_flink-\${flink.minor.version}  (=> ${VERSION}_flink-<minor>)"

git add pom.xml "${MODULE_POMS[@]}"
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
