#!/usr/bin/env bash
# 06_upload_release.sh <notes-file> [--yes]
#   Upload the GitHub release as a DRAFT. You write the release notes yourself (following
#   assets/release-note-template.md) and pass the file here; this script only does the upload:
#   it downloads the published per-version connector jars and creates a DRAFT release with your
#   notes + jars attached. It never publishes — a maintainer reviews the draft and publishes it.
#
#   Versions come from the repo (connector version from pom `srfc.version`, Flink minor list from
#   common.sh), so the attached jars match what was actually released.
#
#   <notes-file>  the release body (markdown) you wrote
#   --yes         skip the confirmation (or set CONFIRM=yes)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"
REPO_ROOT="$(resolve_repo)"
cd "$REPO_ROOT"

REPO="${RELEASE_REPO:-StarRocks/starrocks-connector-for-apache-flink}"
BASE="https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks"

NOTES=""; YES="${CONFIRM:-}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --yes) YES=yes; shift;;
    -*)    die "unknown arg: $1";;
    *)     [ -z "$NOTES" ] || die "unexpected extra arg: $1"; NOTES="$1"; shift;;
  esac
done
[ -n "$NOTES" ] || die "usage: 06_upload_release.sh <notes-file> [--yes]   (write the notes first, following assets/release-note-template.md)"
[ -f "$NOTES" ] || die "notes file not found: $NOTES"

command -v gh >/dev/null 2>&1 || die "gh (GitHub CLI) not found"
gh auth status >/dev/null 2>&1 || die "gh is not authenticated — run 'gh auth login'"

VERSION="$(pom_srfc_version "$REPO_ROOT")"          # actual connector version, e.g. 1.2.15
TAG="v$VERSION"
git rev-parse -q --verify "refs/tags/$TAG" >/dev/null || die "tag $TAG not found locally — run the earlier steps first"
gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1 \
  && die "a release for $TAG already exists (draft or published) — delete the draft (gh release delete $TAG) or edit it on GitHub"

mapfile -t VERSIONS < <(resolve_versions "$REPO_ROOT")   # actual supported Flink minors
[ "${#VERSIONS[@]}" -gt 0 ] || die "no Flink versions resolved — is common.sh updated to support 'supported-minor-versions'?"

# download the published jars to attach as assets (the same artifacts on Central)
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
for m in "${VERSIONS[@]}"; do
  art="flink-connector-starrocks-${VERSION}_flink-${m}.jar"
  info "downloading $art"
  curl -fsSL --retry 5 --retry-delay 10 -o "$WORK/$art" "$BASE/${VERSION}_flink-${m}/$art" \
    || die "download failed for flink $m — is ${VERSION}_flink-${m} published on Central yet?"
done

# confirm (creating a draft is reversible, but it is an outward action)
echo
info "Will create a DRAFT release $TAG on $REPO — notes: $NOTES, jars: ${VERSIONS[*]}"
if [ -t 0 ]; then
  printf 'Create the draft now? [y/N] '; read -r ans; [ "$ans" = y ] || [ "$ans" = Y ] || die "aborted"
else
  [ "$YES" = yes ] || die "non-interactive: pass --yes or set CONFIRM=yes"
fi

gh release create "$TAG" \
  --repo "$REPO" \
  --title "Release $VERSION" \
  --notes-file "$NOTES" \
  --draft --verify-tag \
  "$WORK"/*.jar

echo
info "${C_GRN}DRAFT release $TAG created${C_RST} (not published)"
gh release view "$TAG" --repo "$REPO" | grep -iE '^(url|draft):' || true
echo "Review/edit it on GitHub, then a maintainer publishes."
