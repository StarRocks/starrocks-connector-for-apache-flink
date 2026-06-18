#!/usr/bin/env bash
# Shared helpers for the flink-connector release scripts.
# Source from each stage script:  source "$(dirname "$0")/lib.sh"
#
# Why a shared lib: every stage needs the same notion of "where is the repo",
# "what is the connector version", and "which Flink versions are supported".
# Reading those from one place keeps every stage consistent with the others
# and with the repo's own build.sh / deploy.sh / common.sh.

set -euo pipefail

# ---- pretty output ---------------------------------------------------------
if [ -t 1 ]; then
  C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_BLU=$'\033[34m'; C_RST=$'\033[0m'
else
  C_RED=; C_GRN=; C_YEL=; C_BLU=; C_RST=
fi
info()  { printf '%s==>%s %s\n' "$C_BLU" "$C_RST" "$*"; }
warn()  { printf '%sWARN%s  %s\n' "$C_YEL" "$C_RST" "$*" >&2; }
pass()  { printf '  %sPASS%s %s\n' "$C_GRN" "$C_RST" "$*"; }
fail()  { printf '  %sFAIL%s %s\n' "$C_RED" "$C_RST" "$*" >&2; }
die()   { printf '%sABORT%s %s\n' "$C_RED" "$C_RST" "$*" >&2; exit 1; }

# ---- repo location ---------------------------------------------------------
# The connector repo. Override with CONNECTOR_REPO=/path; otherwise use the git
# toplevel of the current directory. We validate it really is the connector so a
# stray CWD can never make us tag/build/deploy the wrong project.
resolve_repo() {
  local root
  if [ -n "${CONNECTOR_REPO:-}" ]; then
    root="$CONNECTOR_REPO"
  else
    root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  fi
  [ -n "$root" ] && [ -f "$root/pom.xml" ] \
    || die "Cannot find the connector repo. Run from inside it, or set CONNECTOR_REPO=/path/to/starrocks-connector-for-apache-flink"
  grep -q "<artifactId>flink-connector-starrocks</artifactId>" "$root/pom.xml" \
    || die "$root/pom.xml is not flink-connector-starrocks — set CONNECTOR_REPO to the right repo"
  printf '%s' "$root"
}

# ---- version helpers (parse the repo, do not hardcode) ---------------------
# srfc.version declared in the root pom — the connector version, e.g. 1.2.15
pom_srfc_version() {
  grep -m1 -oE '<srfc.version>[^<]+' "$1/pom.xml" | sed 's/.*>//'
}

# Is the root pom's project <version> still a -SNAPSHOT? 0 = yes (snapshot), 1 = no (release)
pom_is_snapshot() {
  grep -qE '<version>\$\{srfc.version\}_flink-\$\{flink.minor.version\}-SNAPSHOT</version>' "$1/pom.xml"
}

# The user docs that carry a per-release "Version requirements" table row.
DOCS_VERSION_FILES=(docs/content/connector-sink.md docs/content/connector-source.md)

# Assert the user docs' "Version requirements" table lists <version>. Each connector release adds a
# row to these tables; forgetting it ships a release the docs never mention. 01_tag.sh runs this
# against the release branch's tree (== origin/main at that point), while everything is still
# reversible. A missing row is NOT a hard error — a maintainer may intentionally skip the doc bump —
# so we require explicit confirmation (interactive y/N, or CONFIRM_DOCS_VERSION=<version> when
# non-interactive) and otherwise continue. The match field-splits each table row on "|" and compares
# the trimmed first data cell to <version> exactly (so a version mentioned in prose, or a longer
# version sharing the prefix, does not count), and only inside the "## Version requirements" section.
check_docs_version() {
  local root="$1" version="$2" f path ans
  local -a missing=()
  for f in "${DOCS_VERSION_FILES[@]}"; do
    path="$root/$f"
    if [ ! -f "$path" ]; then
      warn "doc not found: $f — treating as a missing version row"; missing+=("$f"); continue
    fi
    if awk -v v="$version" -F'|' '
        /^##[[:space:]]+Version requirements/ { inblk=1; next }
        inblk && /^##[[:space:]]/            { inblk=0 }
        inblk && $1=="" && NF>=2 {
          cell=$2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", cell)
          if (cell==v) found=1
        }
        END { exit found ? 0 : 1 }
      ' "$path"; then
      pass "docs: $f Version requirements lists $version"
    else
      missing+=("$f")
    fi
  done

  [ "${#missing[@]}" -eq 0 ] && return 0

  warn "Version requirements table does NOT list $version in: ${missing[*]}"
  warn "Usually you add a '$version' row to these tables on main first (e.g. a '[Doc] Add doc for $version' PR) before releasing."
  if [ -t 0 ]; then
    printf 'Release %s anyway, without the docs version row? [y/N] ' "$version"; read -r ans
    case "$ans" in
      y|Y) warn "proceeding without the docs version row for $version (confirmed)";;
      *)   die "aborted — add the $version row to the Version requirements table on main, then re-run";;
    esac
  else
    [ "${CONFIRM_DOCS_VERSION:-}" = "$version" ] \
      || die "non-interactive: docs missing the $version row — add it on main, or set CONFIRM_DOCS_VERSION=$version to release anyway"
    warn "proceeding without the docs version row for $version (CONFIRM_DOCS_VERSION set)"
  fi
}

# Assert HEAD is exactly the release tag v<version> — not merely some clean, de-SNAPSHOTed checkout.
# git-commit-id stamps HEAD into every jar, and 03's marker is written from HEAD too; so without this
# a branch that advanced past v<version> (or any other de-SNAPSHOT commit) could pass 03/04 and
# publish artifacts whose embedded commit != the tag — which 05 only catches after the bytes are
# already immutable on Central. Dies unless HEAD == the tag's commit. (^{commit} peels annotated tags;
# `|| true` keeps a missing tag from tripping set -e before we can emit a clear message.)
verify_head_is_tag() {
  local root="$1" version="$2" tag head tagc
  tag="v$version"
  head="$(git -C "$root" rev-parse HEAD)"
  tagc="$(git -C "$root" rev-parse -q --verify "refs/tags/$tag^{commit}" 2>/dev/null || true)"
  [ -n "$tagc" ] || die "release tag $tag not found — run 01_tag.sh first"
  [ "$head" = "$tagc" ] \
    || die "HEAD ($head) is not the release tag $tag ($tagc) — checkout the tag (run 02_install_sdk.sh) before continuing"
}

# Assert origin already has the release tag v<version> AND it resolves to <expected> commit. Maven
# Central artifacts are immutable, so publishing before the public tag is pushed (push skipped or
# failed), or when origin's tag points at a different commit, would leave Central artifacts with no
# matching — or a mismatched — GitHub tag. ls-remote is read-only; for an annotated tag the ^{} line
# is the peeled commit, so prefer it and fall back to the direct line for a lightweight tag.
verify_tag_on_origin() {
  local root="$1" version="$2" expected="$3" tag remote rc
  tag="v$version"
  remote="$(git -C "$root" ls-remote origin "refs/tags/$tag" "refs/tags/$tag^{}" 2>/dev/null || true)"
  rc="$(awk -v t="refs/tags/$tag^{}" '$2==t{print $1}' <<<"$remote")"
  [ -n "$rc" ] || rc="$(awk -v t="refs/tags/$tag" '$2==t{print $1}' <<<"$remote")"
  [ -n "$rc" ] || die "tag $tag is not on origin — push it before deploying: git push origin $tag"
  [ "$rc" = "$expected" ] \
    || die "origin's $tag ($rc) != the commit being deployed ($expected) — origin has a different tag; reconcile (re-push or fix the tag) before publishing"
}

# Supported Flink minor versions — ask common.sh to print them (the repo's source
# of truth). `bash common.sh supported-minor-versions` returns a space-separated
# list and needs no maven. Empty output => caller's count guard will abort.
supported_minor_versions() {
  bash "$1/common.sh" supported-minor-versions 2>/dev/null
}

# Resolve the version list to operate on: explicit args if given, else all supported.
# Usage: resolve_versions "$REPO_ROOT" "$@"
resolve_versions() {
  local root="$1"; shift
  if [ "$#" -gt 0 ]; then printf '%s\n' "$@"; else supported_minor_versions "$root" | tr ' ' '\n' | grep -v '^$'; fi
}

# Read git.commit.id from a properties stream on stdin. ".abbrev" is a different
# key (text after "id" is "." not "="), so this matches exactly one line.
prop_commit_id()    { sed -n 's/^git\.commit\.id=\(.*\)$/\1/p'; }
prop_build_version(){ sed -n 's/^git\.build\.version=\(.*\)$/\1/p'; }

# Path to the locally-installed stream-load SDK jar-with-dependencies that the connector shades in.
installed_sdk_jar() {
  local root="$1" repo ver
  repo="${MAVEN_REPO:-$HOME/.m2/repository}"
  ver="$(grep -m1 -oE '<version>[^<]+' "$root/starrocks-stream-load-sdk/pom.xml" | sed 's/.*>//')"
  printf '%s' "$repo/com/starrocks/starrocks-stream-load-sdk/$ver/starrocks-stream-load-sdk-$ver-jar-with-dependencies.jar"
}

# Assert the locally-installed SDK (the exact jar the connector build will shade) was built from
# <expected> commit. Dies otherwise. Used by 02 (after install) and 04 (right before deploy).
verify_installed_sdk() {
  local root="$1" expected="$2" jar id
  jar="$(installed_sdk_jar "$root")"
  [ -f "$jar" ] || die "installed SDK not found at $jar — run 02_install_sdk.sh"
  id="$(unzip -p "$jar" stream-load-sdk-git.properties 2>/dev/null | prop_commit_id)"
  [ "$id" = "$expected" ] || die "installed SDK commit=${id:-none} != tag $expected — the local 1.1-SNAPSHOT changed; re-run 02_install_sdk.sh on the tag"
}

# Make `bash build.sh` / `bash deploy.sh` (which honor CUSTOM_MVN) suppress SNAPSHOT updates, so the
# build resolves the locally-installed SDK instead of refreshing 1.1-SNAPSHOT from a remote repo —
# otherwise a verified-then-published jar could shade a different SDK. Points CUSTOM_MVN at a tiny
# wrapper that appends -nsu. Call before invoking build.sh / deploy.sh.
pin_local_snapshots() {
  local wrap
  local -a base
  # CUSTOM_MVN may carry args (the repo's CI uses `mvn -B -ntp`); split into command+args so the
  # wrapper re-emits each token instead of treating the whole string as one executable name. Each
  # token is %q-quoted; "$@" and -nsu stay literal so they apply at call time.
  base=(${CUSTOM_MVN:-mvn})
  wrap="$(mktemp -d)/mvn"
  { printf '#!/usr/bin/env bash\n'
    printf 'exec'
    printf ' %q' "${base[@]}"
    printf ' "$@" -nsu\n'
  } > "$wrap"
  chmod +x "$wrap"
  export CUSTOM_MVN="$wrap"
  info "Pinned SNAPSHOT resolution to the local repo (-nsu) for build/deploy"
}
